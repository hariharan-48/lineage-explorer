"""
Script Parser for Exasol Lineage Extraction
============================================
Uses proper AST-based parsers to extract table references from scripts:
- sqlglot for SQL parsing (handles complex SQL reliably)
- luaparser for Lua AST analysis
- ast module for Python scripts

Install dependencies:
    pip install sqlglot luaparser
"""

import re
from typing import Set, List, Optional, Tuple
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)

# Try to import parsers
try:
    import sqlglot
    from sqlglot import exp
    HAS_SQLGLOT = True
except ImportError:
    HAS_SQLGLOT = False
    logger.warning("sqlglot not installed. SQL parsing will be limited. Run: pip install sqlglot")

try:
    from luaparser import ast as lua_ast
    from luaparser import astnodes
    HAS_LUAPARSER = True
except ImportError:
    HAS_LUAPARSER = False
    logger.warning("luaparser not installed. Lua parsing will be limited. Run: pip install luaparser")


@dataclass
class TableReference:
    """Represents a reference to a database object."""
    schema: Optional[str]
    name: str
    reference_type: str  # SELECT, INSERT, UPDATE, DELETE, MERGE, JOIN, CTE_SOURCE
    alias: Optional[str] = None

    def full_id(self) -> str:
        """Return fully qualified name."""
        if self.schema:
            return f"{self.schema}.{self.name}"
        return self.name


class SQLParser:
    """
    Parse SQL statements using sqlglot AST.

    sqlglot supports:
    - All major SQL dialects (including Exasol-compatible syntax)
    - CTEs, subqueries, window functions
    - Complex JOINs
    - MERGE statements
    - DDL statements
    """

    def __init__(self, dialect: str = "postgres"):
        """
        Initialize SQL parser.

        Args:
            dialect: SQL dialect for parsing. Use 'postgres' for Exasol compatibility.
        """
        self.dialect = dialect

    def parse(self, sql: str) -> List[TableReference]:
        """
        Parse SQL and extract all table references.

        Args:
            sql: SQL statement to parse

        Returns:
            List of TableReference objects
        """
        if not HAS_SQLGLOT:
            return self._fallback_parse(sql)

        references: List[TableReference] = []
        cte_names: Set[str] = set()

        try:
            # Parse the SQL
            parsed = sqlglot.parse(sql, dialect=self.dialect)

            for statement in parsed:
                if statement is None:
                    continue

                # First, collect CTE names (these are not real table references)
                for cte in statement.find_all(exp.CTE):
                    if cte.alias:
                        cte_names.add(cte.alias.upper())

                # Find all table references
                for table in statement.find_all(exp.Table):
                    table_name = table.name
                    schema_name = table.db if hasattr(table, 'db') and table.db else None

                    # Skip CTE references
                    if table_name.upper() in cte_names:
                        continue

                    # Determine reference type based on parent
                    ref_type = self._get_reference_type(table)

                    references.append(TableReference(
                        schema=schema_name.upper() if schema_name else None,
                        name=table_name.upper(),
                        reference_type=ref_type,
                        alias=table.alias if hasattr(table, 'alias') else None
                    ))

        except Exception as e:
            logger.warning(f"sqlglot parsing failed, using fallback: {e}")
            return self._fallback_parse(sql)

        return self._deduplicate(references)

    def _get_reference_type(self, table_node) -> str:
        """Determine the type of table reference from AST context."""
        parent = table_node.parent
        is_inside_create = False

        while parent:
            parent_type = type(parent).__name__

            if parent_type == 'Insert':
                return 'INSERT'
            elif parent_type == 'Update':
                return 'UPDATE'
            elif parent_type == 'Delete':
                return 'DELETE'
            elif parent_type == 'Merge':
                return 'MERGE'
            elif parent_type in ('Join', 'From'):
                # Check if it's specifically a JOIN
                if parent_type == 'Join':
                    return 'JOIN'
                # Don't return SELECT yet - continue to check if inside CREATE
                pass
            elif parent_type == 'Create':
                is_inside_create = True
            elif parent_type == 'Drop':
                return 'DDL'

            parent = parent.parent if hasattr(parent, 'parent') else None

        # If inside a CREATE statement, check if this is the target table or a source table
        # The first table in CREATE TABLE x AS SELECT is DDL (target)
        # Tables in the SELECT part are reads (SELECT)
        if is_inside_create:
            # Check if this table is directly under Create (the target) or in a subquery (source)
            parent = table_node.parent
            if parent and type(parent).__name__ == 'Create':
                return 'DDL'

        return 'SELECT'  # Default - reading from table

    def _fallback_parse(self, sql: str) -> List[TableReference]:
        """Fallback regex-based parsing when sqlglot is not available."""
        references: List[TableReference] = []

        # Simple patterns for common SQL
        patterns = [
            (r'\bFROM\s+([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)', 'SELECT'),
            (r'\bJOIN\s+([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)', 'JOIN'),
            (r'\bINTO\s+([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)', 'INSERT'),
            (r'\bUPDATE\s+([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)', 'UPDATE'),
            (r'\bMERGE\s+INTO\s+([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)', 'MERGE'),
        ]

        for pattern, ref_type in patterns:
            for match in re.finditer(pattern, sql, re.IGNORECASE):
                table_ref = match.group(1)
                parts = table_ref.upper().split('.')
                if len(parts) == 2:
                    references.append(TableReference(schema=parts[0], name=parts[1], reference_type=ref_type))
                else:
                    references.append(TableReference(schema=None, name=parts[0], reference_type=ref_type))

        return self._deduplicate(references)

    def _deduplicate(self, refs: List[TableReference]) -> List[TableReference]:
        """Remove duplicate references."""
        seen: Set[Tuple[Optional[str], str]] = set()
        unique: List[TableReference] = []
        for ref in refs:
            key = (ref.schema, ref.name)
            if key not in seen:
                seen.add(key)
                unique.append(ref)
        return unique


class LuaScriptParser:
    """
    Parse Lua scripts using luaparser AST.

    Extracts SQL strings from:
    - query() function calls
    - String literals
    - Concatenated strings
    """

    def __init__(self):
        self.sql_parser = SQLParser()

    def parse(self, lua_code: str) -> List[TableReference]:
        """
        Parse Lua script and extract table references from SQL.

        Args:
            lua_code: Lua script source code

        Returns:
            List of TableReference objects
        """
        if not HAS_LUAPARSER:
            return self._fallback_parse(lua_code)

        references: List[TableReference] = []
        sql_strings: List[str] = []

        try:
            # Parse Lua AST
            tree = lua_ast.parse(lua_code)

            # Walk the AST to find SQL strings
            sql_strings = self._extract_sql_from_ast(tree)

        except Exception as e:
            logger.warning(f"Lua parsing failed, using fallback: {e}")
            sql_strings = self._extract_sql_fallback(lua_code)

        # Parse each SQL string
        for sql in sql_strings:
            # Convert escaped newlines/tabs to actual whitespace
            # Scripts may contain literal \n or \t that need to be converted
            sql = sql.replace('\\n', '\n').replace('\\t', '\t').replace('\\r', '\r')
            refs = self.sql_parser.parse(sql)
            references.extend(refs)

        return self._deduplicate(references)

    def _extract_sql_from_ast(self, tree) -> List[str]:
        """Extract SQL strings from Lua AST."""
        sql_strings: List[str] = []

        def get_string_value(node) -> Optional[str]:
            """Extract string value from AST node."""
            if isinstance(node, astnodes.String):
                # Handle both bytes and str
                s = node.s
                if isinstance(s, bytes):
                    s = s.decode('utf-8')
                return s
            elif isinstance(node, astnodes.Concat):
                # Handle string concatenation
                parts = []
                if hasattr(node, 'left'):
                    left = get_string_value(node.left)
                    if left:
                        parts.append(left)
                if hasattr(node, 'right'):
                    right = get_string_value(node.right)
                    if right:
                        parts.append(right)
                return ' '.join(parts) if parts else None
            return None

        def looks_like_sql(text: str) -> bool:
            """Check if text looks like SQL."""
            if not text:
                return False
            text_upper = text.upper()
            return any(kw in text_upper for kw in ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'MERGE', 'TRUNCATE', 'CREATE'])

        def walk(node):
            """Recursively walk the AST tree."""
            if node is None:
                return

            # Look for function calls that execute SQL:
            # - query() / pquery() - standard Lua scripting functions
            # - exa.query_no_preprocessing() / exa.pquery_no_preprocessing() - adapter scripts
            if isinstance(node, astnodes.Call):
                func = node.func
                func_name = None

                # Direct function call: query(), pquery()
                if isinstance(func, astnodes.Name):
                    func_name = func.id
                # Method call: exa.query_no_preprocessing(), exa.pquery_no_preprocessing()
                elif isinstance(func, astnodes.Index):
                    if hasattr(func, 'idx') and isinstance(func.idx, astnodes.Name):
                        func_name = func.idx.id

                if func_name in ('query', 'pquery', 'query_no_preprocessing', 'pquery_no_preprocessing'):
                    # Extract string argument (first arg is the SQL string)
                    if hasattr(node, 'args') and node.args:
                        arg = node.args[0] if node.args else None
                        if arg:
                            sql = get_string_value(arg)
                            if sql and looks_like_sql(sql):
                                sql_strings.append(sql)

            # Look for string literals that contain SQL keywords
            if isinstance(node, astnodes.String):
                content = get_string_value(node)
                if content and looks_like_sql(content):
                    sql_strings.append(content)

            # Recurse into all child attributes
            for attr in ['body', 'chunk', 'block', 'args', 'values', 'targets',
                         'left', 'right', 'func', 'table', 'key', 'value',
                         'source', 'target', 'iter', 'step', 'start', 'stop']:
                child = getattr(node, attr, None)
                if child is not None:
                    if isinstance(child, list):
                        for item in child:
                            walk(item)
                    elif hasattr(child, '__dict__'):
                        walk(child)

        walk(tree)
        return sql_strings

    def _extract_sql_fallback(self, lua_code: str) -> List[str]:
        """Fallback SQL extraction without AST parsing."""
        sql_strings: List[str] = []

        # Multi-line strings [[...]]
        for match in re.finditer(r'\[\[(.*?)\]\]', lua_code, re.DOTALL):
            content = match.group(1)
            if any(kw in content.upper() for kw in ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'MERGE', 'TRUNCATE']):
                sql_strings.append(content)

        # query(), pquery(), query_no_preprocessing(), pquery_no_preprocessing() calls
        # Match: query(...), pquery(...), exa.query_no_preprocessing(...), etc.
        query_funcs = r'(?:exa\.)?p?query(?:_no_preprocessing)?'

        # With single/double quoted strings
        for match in re.finditer(query_funcs + r'\s*\(\s*["\'](.+?)["\']', lua_code, re.IGNORECASE | re.DOTALL):
            sql_strings.append(match.group(1))

        # With [[ ]] multiline strings
        for match in re.finditer(query_funcs + r'\s*\(\s*\[\[(.+?)\]\]', lua_code, re.DOTALL):
            sql_strings.append(match.group(1))

        # Regular strings with SQL
        for match in re.finditer(r'["\']([^"\']*(?:SELECT|INSERT|UPDATE|DELETE)[^"\']*)["\']',
                                  lua_code, re.IGNORECASE):
            sql_strings.append(match.group(1))

        return sql_strings

    def _fallback_parse(self, lua_code: str) -> List[TableReference]:
        """Fallback when luaparser is not available."""
        sql_strings = self._extract_sql_fallback(lua_code)
        references: List[TableReference] = []

        for sql in sql_strings:
            refs = self.sql_parser.parse(sql)
            references.extend(refs)

        return self._deduplicate(references)

    def _deduplicate(self, refs: List[TableReference]) -> List[TableReference]:
        """Remove duplicate references."""
        seen: Set[Tuple[Optional[str], str]] = set()
        unique: List[TableReference] = []
        for ref in refs:
            key = (ref.schema, ref.name)
            if key not in seen:
                seen.add(key)
                unique.append(ref)
        return unique


class PythonScriptParser:
    """
    Parse Python scripts using Python's ast module.

    Extracts SQL strings from:
    - String literals
    - f-strings
    - Triple-quoted strings
    """

    def __init__(self):
        self.sql_parser = SQLParser()

    def parse(self, python_code: str) -> List[TableReference]:
        """
        Parse Python script and extract table references from SQL.

        Args:
            python_code: Python script source code

        Returns:
            List of TableReference objects
        """
        import ast

        references: List[TableReference] = []
        sql_strings: List[str] = []

        try:
            tree = ast.parse(python_code)

            class StringVisitor(ast.NodeVisitor):
                def __init__(self):
                    self.strings: List[str] = []

                def visit_Constant(self, node):
                    if isinstance(node.value, str) and self._looks_like_sql(node.value):
                        self.strings.append(node.value)
                    self.generic_visit(node)

                def visit_JoinedStr(self, node):
                    # Handle f-strings
                    parts = []
                    for value in node.values:
                        if isinstance(value, ast.Constant):
                            parts.append(str(value.value))
                        else:
                            parts.append("?")  # Placeholder for variables
                    combined = ''.join(parts)
                    if self._looks_like_sql(combined):
                        self.strings.append(combined)
                    self.generic_visit(node)

                def _looks_like_sql(self, text: str) -> bool:
                    if not text:
                        return False
                    text_upper = text.upper()
                    return any(kw in text_upper for kw in ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'MERGE'])

            visitor = StringVisitor()
            visitor.visit(tree)
            sql_strings = visitor.strings

        except SyntaxError as e:
            logger.warning(f"Python parsing failed: {e}")
            # Fallback to regex extraction
            sql_strings = self._extract_sql_fallback(python_code)

        for sql in sql_strings:
            refs = self.sql_parser.parse(sql)
            references.extend(refs)

        return self._deduplicate(references)

    def _extract_sql_fallback(self, python_code: str) -> List[str]:
        """Fallback SQL extraction."""
        sql_strings: List[str] = []

        # Triple-quoted strings
        for match in re.finditer(r'"""(.*?)"""', python_code, re.DOTALL):
            if any(kw in match.group(1).upper() for kw in ['SELECT', 'INSERT']):
                sql_strings.append(match.group(1))

        for match in re.finditer(r"'''(.*?)'''", python_code, re.DOTALL):
            if any(kw in match.group(1).upper() for kw in ['SELECT', 'INSERT']):
                sql_strings.append(match.group(1))

        return sql_strings

    def _deduplicate(self, refs: List[TableReference]) -> List[TableReference]:
        """Remove duplicate references."""
        seen: Set[Tuple[Optional[str], str]] = set()
        unique: List[TableReference] = []
        for ref in refs:
            key = (ref.schema, ref.name)
            if key not in seen:
                seen.add(key)
                unique.append(ref)
        return unique


def parse_script(script_text: str, language: str,
                 known_objects: Optional[Set[str]] = None) -> List[TableReference]:
    """
    Parse a script and return table references.

    Args:
        script_text: The script source code
        language: 'LUA', 'PYTHON', or 'SQL'
        known_objects: Optional set of known object IDs for validation

    Returns:
        List of TableReference objects found in the script
    """
    language = language.upper()

    if language == 'SQL':
        parser = SQLParser()
        refs = parser.parse(script_text)
    elif language == 'PYTHON':
        parser = PythonScriptParser()
        refs = parser.parse(script_text)
    else:  # LUA or default
        parser = LuaScriptParser()
        refs = parser.parse(script_text)

    # Validate against known objects if provided
    if known_objects:
        validated: List[TableReference] = []
        for ref in refs:
            # Try exact match (uppercase for Exasol)
            if ref.schema:
                full_id = f"{ref.schema.upper()}.{ref.name.upper()}"
                if full_id in known_objects:
                    validated.append(ref)
                    continue
                # For DDL (CREATE TABLE), include even if not in known_objects
                # The table being created might not exist yet
                if ref.reference_type == 'DDL':
                    validated.append(TableReference(
                        schema=ref.schema.upper(),
                        name=ref.name.upper(),
                        reference_type=ref.reference_type,
                        alias=ref.alias
                    ))
                    continue

            # Try to match name in any schema (case-insensitive)
            ref_name_upper = ref.name.upper()
            found = False
            for obj_id in known_objects:
                if obj_id.endswith(f".{ref_name_upper}"):
                    matched_schema = obj_id.split('.')[0]
                    validated.append(TableReference(
                        schema=matched_schema,
                        name=ref_name_upper,
                        reference_type=ref.reference_type,
                        alias=ref.alias
                    ))
                    found = True
                    break

            # For DDL without schema, still include it
            if not found and ref.reference_type == 'DDL':
                validated.append(TableReference(
                    schema=ref.schema.upper() if ref.schema else None,
                    name=ref_name_upper,
                    reference_type=ref.reference_type,
                    alias=ref.alias
                ))
        return validated

    return refs


# CLI testing
if __name__ == '__main__':
    print("=" * 60)
    print("Script Parser Test")
    print("=" * 60)

    # Check available parsers
    print(f"\nsqlglot available: {HAS_SQLGLOT}")
    print(f"luaparser available: {HAS_LUAPARSER}")

    # Test SQL parsing
    print("\n--- SQL Parsing Test ---")
    test_sql = """
    WITH monthly_data AS (
        SELECT customer_id, SUM(amount) as total
        FROM DWH.FACT_SALES
        GROUP BY customer_id
    )
    SELECT m.*, c.name
    FROM monthly_data m
    LEFT JOIN DWH.DIM_CUSTOMER c ON m.customer_id = c.id
    WHERE c.status = 'ACTIVE'
    """

    sql_parser = SQLParser()
    refs = sql_parser.parse(test_sql)
    print(f"Found {len(refs)} table references:")
    for ref in refs:
        print(f"  {ref.full_id()} ({ref.reference_type})")

    # Test Lua parsing
    print("\n--- Lua Script Parsing Test ---")
    test_lua = '''
    function run(ctx)
        local result = query([[
            SELECT * FROM DWH.FACT_ORDERS fo
            INNER JOIN DWH.DIM_PRODUCT dp ON fo.product_id = dp.id
            WHERE fo.order_date > '2024-01-01'
        ]])

        for row in result do
            query("INSERT INTO STAGING.PROCESSED_ORDERS VALUES(" .. row.id .. ")")
        end
    end
    '''

    lua_parser = LuaScriptParser()
    refs = lua_parser.parse(test_lua)
    print(f"Found {len(refs)} table references:")
    for ref in refs:
        print(f"  {ref.full_id()} ({ref.reference_type})")

    # Test Python parsing
    print("\n--- Python Script Parsing Test ---")
    test_python = '''
def process_data(ctx):
    sql = """
    SELECT *
    FROM MART.VW_SALES_SUMMARY s
    JOIN MART.DIM_TIME t ON s.date_key = t.date_key
    WHERE t.year = 2024
    """
    result = ctx.execute(sql)

    for row in result:
        ctx.emit(row)
    '''

    python_parser = PythonScriptParser()
    refs = python_parser.parse(test_python)
    print(f"Found {len(refs)} table references:")
    for ref in refs:
        print(f"  {ref.full_id()} ({ref.reference_type})")
