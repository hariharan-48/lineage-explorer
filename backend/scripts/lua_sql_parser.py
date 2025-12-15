"""
Lua/SQL Parser for Exasol Scripts
=================================
Parses Lua UDF scripts and Python scripts to extract table/view references
from embedded SQL statements.

This handles complex cases like:
- SQL strings in Lua query() calls
- Multi-line SQL statements
- Dynamic SQL with string concatenation
- CTEs (WITH clauses)
- Subqueries
- MERGE statements
- Various JOIN types
"""

import re
from typing import Set, List, Tuple, Optional
from dataclasses import dataclass


@dataclass
class SQLReference:
    """Represents a reference to a database object in SQL."""
    schema: Optional[str]
    name: str
    reference_type: str  # SELECT, INSERT, UPDATE, DELETE, MERGE, JOIN, CTE


class LuaSQLParser:
    """
    Parses Lua scripts to extract SQL table references.

    The parser handles:
    1. Exasol's query() function calls
    2. Multi-line SQL strings
    3. String concatenation in Lua
    4. Various SQL statement types
    """

    # SQL keywords that precede table names
    TABLE_KEYWORDS = [
        'FROM', 'JOIN', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL JOIN',
        'LEFT OUTER JOIN', 'RIGHT OUTER JOIN', 'FULL OUTER JOIN', 'CROSS JOIN',
        'INTO', 'UPDATE', 'TABLE', 'MERGE INTO', 'USING'
    ]

    # Keywords that indicate the end of a table reference
    END_KEYWORDS = [
        'WHERE', 'ON', 'SET', 'VALUES', 'GROUP', 'ORDER', 'HAVING', 'LIMIT',
        'UNION', 'INTERSECT', 'EXCEPT', 'WHEN', 'THEN', 'ELSE', 'END', 'AS',
        'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'WITH', 'AND', 'OR', ')', ','
    ]

    def __init__(self, known_objects: Optional[Set[str]] = None):
        """
        Initialize parser.

        Args:
            known_objects: Set of known object IDs (SCHEMA.NAME) for validation
        """
        self.known_objects = known_objects or set()

        # Build regex patterns
        self._build_patterns()

    def _build_patterns(self) -> None:
        """Build regex patterns for SQL parsing."""
        # Pattern to find query() calls with SQL strings
        self.query_pattern = re.compile(
            r'query\s*\(\s*["\'](.+?)["\']',
            re.IGNORECASE | re.DOTALL
        )

        # Pattern for multi-line strings with [[...]]
        self.multiline_string_pattern = re.compile(
            r'\[\[(.*?)\]\]',
            re.DOTALL
        )

        # Pattern for string concatenation cleanup
        self.concat_pattern = re.compile(
            r'["\'\]]\s*\.\.\s*["\'\[]'
        )

        # Pattern for table references (SCHEMA.TABLE or just TABLE)
        self.table_ref_pattern = re.compile(
            r'([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)'
        )

        # Pattern for CTE names (WITH name AS)
        self.cte_pattern = re.compile(
            r'\bWITH\s+(\w+)\s+AS\s*\(',
            re.IGNORECASE
        )

        # Keywords pattern
        keywords = '|'.join(re.escape(kw) for kw in self.TABLE_KEYWORDS)
        self.table_keyword_pattern = re.compile(
            rf'\b({keywords})\s+([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)',
            re.IGNORECASE
        )

    def parse_lua_script(self, script_text: str) -> List[SQLReference]:
        """
        Parse a Lua script and extract all SQL table references.

        Args:
            script_text: The Lua script text

        Returns:
            List of SQLReference objects
        """
        if not script_text:
            return []

        references: List[SQLReference] = []
        cte_names: Set[str] = set()  # Track CTE names to exclude them

        # Extract SQL from query() calls
        sql_strings = self._extract_sql_strings(script_text)

        for sql in sql_strings:
            # Find CTEs first (they're not real table references)
            cte_names.update(self._find_cte_names(sql))

            # Find table references
            refs = self._find_table_references(sql, cte_names)
            references.extend(refs)

        # Deduplicate while preserving order
        seen = set()
        unique_refs = []
        for ref in references:
            key = (ref.schema, ref.name)
            if key not in seen:
                seen.add(key)
                unique_refs.append(ref)

        return unique_refs

    def _extract_sql_strings(self, script_text: str) -> List[str]:
        """Extract SQL strings from Lua code."""
        sql_strings: List[str] = []

        # Find query() calls
        for match in self.query_pattern.finditer(script_text):
            sql = match.group(1)
            # Clean up concatenation
            sql = self.concat_pattern.sub(' ', sql)
            sql_strings.append(sql)

        # Find multi-line strings that look like SQL
        for match in self.multiline_string_pattern.finditer(script_text):
            content = match.group(1)
            # Check if it looks like SQL
            if any(kw in content.upper() for kw in ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'MERGE']):
                sql_strings.append(content)

        # Also look for any string containing SQL keywords
        # This catches SQL in regular string variables
        all_strings = re.findall(r'["\']([^"\']*(?:SELECT|INSERT|UPDATE|DELETE|FROM)[^"\']*)["\']',
                                  script_text, re.IGNORECASE)
        sql_strings.extend(all_strings)

        return sql_strings

    def _find_cte_names(self, sql: str) -> Set[str]:
        """Find CTE names to exclude from table references."""
        cte_names = set()
        for match in self.cte_pattern.finditer(sql):
            cte_names.add(match.group(1).upper())
        return cte_names

    def _find_table_references(self, sql: str, exclude: Set[str]) -> List[SQLReference]:
        """Find table references in SQL string."""
        references: List[SQLReference] = []
        sql_upper = sql.upper()

        # Find all keyword + table name patterns
        for match in self.table_keyword_pattern.finditer(sql):
            keyword = match.group(1).upper()
            table_ref = match.group(2)

            # Determine reference type
            if 'JOIN' in keyword:
                ref_type = 'JOIN'
            elif keyword in ('FROM', 'USING'):
                ref_type = 'SELECT'
            elif keyword == 'INTO':
                # Could be INSERT INTO or SELECT INTO
                if 'INSERT' in sql_upper[:match.start()]:
                    ref_type = 'INSERT'
                else:
                    ref_type = 'SELECT'
            elif keyword == 'UPDATE':
                ref_type = 'UPDATE'
            elif keyword == 'MERGE INTO':
                ref_type = 'MERGE'
            elif keyword == 'TABLE':
                # Could be DROP TABLE, TRUNCATE TABLE, etc.
                ref_type = 'DDL'
            else:
                ref_type = 'SELECT'

            # Parse schema.name
            schema, name = self._parse_table_ref(table_ref)

            # Skip if it's a CTE name
            if name.upper() in exclude:
                continue

            # Skip common non-table keywords
            if name.upper() in ('DUAL', 'DUMMY', 'SYSDATE'):
                continue

            references.append(SQLReference(
                schema=schema,
                name=name,
                reference_type=ref_type
            ))

        return references

    def _parse_table_ref(self, ref: str) -> Tuple[Optional[str], str]:
        """Parse a table reference into schema and name."""
        parts = ref.split('.')
        if len(parts) == 2:
            return parts[0].upper(), parts[1].upper()
        else:
            return None, parts[0].upper()

    def validate_references(self, references: List[SQLReference]) -> List[SQLReference]:
        """
        Validate references against known objects.

        Only returns references that match known objects in the database.
        """
        if not self.known_objects:
            return references

        validated = []
        for ref in references:
            # Try exact match first
            if ref.schema:
                full_id = f"{ref.schema}.{ref.name}"
                if full_id in self.known_objects:
                    validated.append(ref)
                    continue

            # Try to find match in any schema
            for obj_id in self.known_objects:
                if obj_id.endswith(f".{ref.name}"):
                    # Update schema to matched schema
                    matched_schema = obj_id.split('.')[0]
                    validated.append(SQLReference(
                        schema=matched_schema,
                        name=ref.name,
                        reference_type=ref.reference_type
                    ))
                    break

        return validated


class PythonSQLParser(LuaSQLParser):
    """
    Parser for Python UDF scripts.

    Similar to Lua but handles Python string syntax.
    """

    def _extract_sql_strings(self, script_text: str) -> List[str]:
        """Extract SQL strings from Python code."""
        sql_strings: List[str] = []

        # Triple-quoted strings
        triple_double = re.findall(r'"""(.*?)"""', script_text, re.DOTALL)
        triple_single = re.findall(r"'''(.*?)'''", script_text, re.DOTALL)
        sql_strings.extend(triple_double)
        sql_strings.extend(triple_single)

        # Regular strings with SQL
        single_line = re.findall(
            r'["\']([^"\']*(?:SELECT|INSERT|UPDATE|DELETE|FROM)[^"\']*)["\']',
            script_text, re.IGNORECASE
        )
        sql_strings.extend(single_line)

        # f-strings (extract the template)
        f_strings = re.findall(r'f["\']([^"\']*)["\']', script_text)
        for fs in f_strings:
            if any(kw in fs.upper() for kw in ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'FROM']):
                # Remove f-string placeholders
                cleaned = re.sub(r'\{[^}]+\}', '?', fs)
                sql_strings.append(cleaned)

        return sql_strings


def parse_script(script_text: str, language: str = 'LUA',
                 known_objects: Optional[Set[str]] = None) -> List[SQLReference]:
    """
    Parse a script and return table references.

    Args:
        script_text: The script source code
        language: 'LUA' or 'PYTHON'
        known_objects: Set of known object IDs for validation

    Returns:
        List of SQLReference objects found in the script
    """
    if language.upper() == 'PYTHON':
        parser = PythonSQLParser(known_objects)
    else:
        parser = LuaSQLParser(known_objects)

    refs = parser.parse_lua_script(script_text)

    if known_objects:
        refs = parser.validate_references(refs)

    return refs


# Example usage and testing
if __name__ == '__main__':
    # Test Lua script
    test_lua = '''
    function run(ctx)
        -- Simple query
        local result = query([[
            SELECT * FROM DWH.FACT_SALES fs
            JOIN DWH.DIM_CUSTOMER dc ON fs.customer_id = dc.id
            WHERE fs.date > '2024-01-01'
        ]])

        -- Dynamic query
        local table_name = "STAGING.STG_ORDERS"
        query("INSERT INTO " .. table_name .. " SELECT * FROM RAW.ORDERS")

        -- CTE example
        query([[
            WITH monthly_sales AS (
                SELECT month, SUM(amount) as total
                FROM DWH.FACT_SALES
                GROUP BY month
            )
            SELECT * FROM monthly_sales
            JOIN DWH.DIM_TIME ON monthly_sales.month = DIM_TIME.month
        ]])
    end
    '''

    print("Testing Lua parser:")
    print("-" * 40)
    refs = parse_script(test_lua, 'LUA')
    for ref in refs:
        print(f"  {ref.schema}.{ref.name} ({ref.reference_type})")

    # Test Python script
    test_python = '''
def run(ctx):
    # Triple-quoted SQL
    sql = """
        SELECT *
        FROM MART.VW_SALES_SUMMARY
        LEFT JOIN MART.DIM_DATE ON sale_date = date_key
    """
    ctx.execute(sql)

    # f-string with SQL
    schema = "DWH"
    query = f"UPDATE {schema}.FACT_ORDERS SET status = 'PROCESSED'"
    '''

    print("\nTesting Python parser:")
    print("-" * 40)
    refs = parse_script(test_python, 'PYTHON')
    for ref in refs:
        print(f"  {ref.schema}.{ref.name} ({ref.reference_type})")
