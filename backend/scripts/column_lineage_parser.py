#!/usr/bin/env python3
"""
Column-Level Lineage Parser
============================
Extracts column-level lineage from SQL definitions using sqlglot's lineage module.

This module parses SQL (view definitions, stored procedures, etc.) to trace how
individual columns flow through transformations.

Usage:
    from column_lineage_parser import ColumnLineageExtractor

    extractor = ColumnLineageExtractor(dialect="exasol")
    deps = extractor.extract_column_lineage(sql, target_object_id, schema_context)
"""

import logging
from typing import Dict, List, Optional, Set, Tuple, Any
from dataclasses import dataclass, field

try:
    import sqlglot
    from sqlglot import exp
    from sqlglot.lineage import lineage as sqlglot_lineage
    from sqlglot.optimizer.scope import build_scope
    HAS_SQLGLOT_LINEAGE = True
except ImportError:
    HAS_SQLGLOT_LINEAGE = False
    print("Warning: sqlglot lineage module not available. Column lineage will be limited.")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Aggregate functions that indicate AGGREGATE transformation
AGGREGATE_FUNCTIONS = {
    'SUM', 'COUNT', 'AVG', 'MIN', 'MAX', 'STDDEV', 'VARIANCE',
    'FIRST', 'LAST', 'GROUP_CONCAT', 'LISTAGG', 'ARRAY_AGG',
    'MEDIAN', 'PERCENTILE', 'PERCENTILE_CONT', 'PERCENTILE_DISC',
    'ANY_VALUE', 'APPROX_COUNT_DISTINCT', 'COUNTIF', 'COUNT_IF',
}

# Functions that indicate FUNCTION transformation
KNOWN_FUNCTIONS = {
    'COALESCE', 'NVL', 'NVL2', 'IFNULL', 'NULLIF', 'IIF',
    'CONCAT', 'SUBSTRING', 'SUBSTR', 'LEFT', 'RIGHT', 'TRIM', 'LTRIM', 'RTRIM',
    'UPPER', 'LOWER', 'INITCAP', 'REPLACE', 'TRANSLATE',
    'TO_CHAR', 'TO_DATE', 'TO_TIMESTAMP', 'TO_NUMBER',
    'DATE_TRUNC', 'DATE_ADD', 'DATE_SUB', 'DATEADD', 'DATEDIFF',
    'EXTRACT', 'YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND',
    'ROUND', 'FLOOR', 'CEIL', 'CEILING', 'ABS', 'SIGN', 'MOD',
    'GREATEST', 'LEAST', 'DECODE', 'LENGTH', 'LEN', 'CHARINDEX',
}


@dataclass
class ColumnLineageDep:
    """Represents a single column-level dependency."""
    source_object_id: str
    source_column: str
    target_object_id: str
    target_column: str
    transformation: Optional[str] = None
    transformation_type: str = "DIRECT"


@dataclass
class SchemaContext:
    """Schema context for column resolution."""
    # Maps object_id -> list of column names
    object_columns: Dict[str, List[str]] = field(default_factory=dict)
    # Maps alias -> object_id (for resolving table aliases in SQL)
    alias_map: Dict[str, str] = field(default_factory=dict)


class ColumnLineageExtractor:
    """
    Extracts column-level lineage from SQL using sqlglot.

    Supports:
    - SELECT queries with aliases
    - JOINs
    - CTEs (WITH clauses)
    - Subqueries
    - Aggregations
    - CASE expressions
    - Type casts
    - Various SQL dialects (exasol, bigquery, etc.)
    """

    def __init__(self, dialect: str = "exasol"):
        """
        Initialize the extractor.

        Args:
            dialect: SQL dialect (exasol, bigquery, postgres, etc.)
        """
        self.dialect = dialect.lower()
        # Map to sqlglot dialect names
        self.sqlglot_dialect = self._map_dialect(dialect)

    def _map_dialect(self, dialect: str) -> str:
        """Map our dialect names to sqlglot dialect names."""
        dialect_map = {
            "exasol": "postgres",  # Exasol is close to PostgreSQL
            "bigquery": "bigquery",
            "postgres": "postgres",
            "mysql": "mysql",
            "snowflake": "snowflake",
            "redshift": "redshift",
        }
        return dialect_map.get(dialect.lower(), "postgres")

    def extract_column_lineage(
        self,
        sql: str,
        target_object_id: str,
        schema_context: Optional[SchemaContext] = None,
    ) -> List[ColumnLineageDep]:
        """
        Extract column-level dependencies from SQL.

        Args:
            sql: SQL definition (e.g., view definition, SELECT statement)
            target_object_id: ID of the target object (e.g., "DWH.MY_VIEW")
            schema_context: Optional schema context with column metadata

        Returns:
            List of ColumnLineageDep objects
        """
        if not sql:
            return []

        if not HAS_SQLGLOT_LINEAGE:
            return self._fallback_extract(sql, target_object_id, schema_context)

        try:
            return self._extract_with_sqlglot(sql, target_object_id, schema_context)
        except Exception as e:
            logger.warning(f"sqlglot lineage extraction failed: {e}, falling back to basic parser")
            return self._fallback_extract(sql, target_object_id, schema_context)

    def _extract_with_sqlglot(
        self,
        sql: str,
        target_object_id: str,
        schema_context: Optional[SchemaContext],
    ) -> List[ColumnLineageDep]:
        """Extract lineage using sqlglot's lineage module."""
        dependencies: List[ColumnLineageDep] = []

        # Clean up SQL - remove CREATE VIEW prefix if present
        sql_clean = self._clean_sql(sql)

        try:
            # Parse the SQL
            parsed = sqlglot.parse_one(sql_clean, dialect=self.sqlglot_dialect)
        except Exception as e:
            logger.warning(f"Failed to parse SQL: {e}")
            return self._fallback_extract(sql, target_object_id, schema_context)

        if parsed is None:
            return []

        # Build schema dict for sqlglot lineage if we have context
        schema_dict = self._build_schema_dict(schema_context) if schema_context else {}

        # Find the SELECT statement (handle CREATE VIEW AS SELECT, etc.)
        select_stmt = self._find_select(parsed)
        if select_stmt is None:
            return []

        # Get target columns from SELECT clause
        target_columns = self._extract_select_columns(select_stmt)

        # For each target column, trace its lineage
        for target_col, expression in target_columns:
            col_deps = self._trace_column_lineage(
                target_col,
                expression,
                select_stmt,
                target_object_id,
                schema_context,
                schema_dict,
            )
            dependencies.extend(col_deps)

        return dependencies

    def _clean_sql(self, sql: str) -> str:
        """Clean SQL by removing CREATE VIEW prefix."""
        import re
        sql = sql.strip()

        # Common patterns to remove
        patterns = [
            r"CREATE\s+OR\s+REPLACE\s+FORCE\s+VIEW",
            r"CREATE\s+OR\s+REPLACE\s+VIEW",
            r"CREATE\s+VIEW",
        ]

        sql_upper = sql.upper()
        for pattern in patterns:
            match = re.match(pattern, sql_upper)
            if match:
                # Find AS keyword after the view name (handles whitespace/newlines)
                remaining = sql[match.end():]
                # Match: view_name AS (with any whitespace)
                as_match = re.search(r'^\s*\S+\s+AS\s+', remaining, re.IGNORECASE)
                if as_match:
                    sql = remaining[as_match.end():].strip()
                    break

        return sql

    def _find_select(self, parsed: exp.Expression) -> Optional[exp.Select]:
        """Find the SELECT statement in parsed SQL."""
        if isinstance(parsed, exp.Select):
            return parsed

        # Handle CREATE VIEW AS SELECT
        if isinstance(parsed, exp.Create):
            # Look for the expression/query
            expr = parsed.find(exp.Select)
            if expr:
                return expr

        # Handle WITH (CTE) statements
        if isinstance(parsed, exp.With):
            # Get the main query
            main_expr = parsed.find(exp.Select)
            if main_expr:
                return main_expr

        # Generic search
        select = parsed.find(exp.Select)
        return select

    def _normalize_column_name(self, col_name: str) -> str:
        """Normalize column name by removing extra whitespace and newlines."""
        import re
        # Replace newlines and multiple spaces with single space
        normalized = re.sub(r'\s+', ' ', col_name)
        return normalized.strip()

    def _extract_select_columns(
        self,
        select: exp.Select
    ) -> List[Tuple[str, exp.Expression]]:
        """
        Extract column names and their expressions from SELECT clause.

        Returns list of (column_name, expression) tuples.
        """
        columns = []

        for i, expr in enumerate(select.expressions):
            # Get the alias if present, otherwise generate a name
            if isinstance(expr, exp.Alias):
                col_name = expr.alias
                col_expr = expr.this
            elif isinstance(expr, exp.Column):
                col_name = expr.name
                col_expr = expr
            elif hasattr(expr, 'alias') and expr.alias:
                col_name = expr.alias
                col_expr = expr
            else:
                # For expressions without alias, use the SQL representation
                col_name = expr.sql(dialect=self.sqlglot_dialect) if hasattr(expr, 'sql') else f"col_{i}"
                col_expr = expr

            # Normalize column name to handle newlines/extra spaces in SQL
            col_name = self._normalize_column_name(col_name)
            columns.append((col_name, col_expr))

        return columns

    def _trace_column_lineage(
        self,
        target_col: str,
        expression: exp.Expression,
        select_stmt: exp.Select,
        target_object_id: str,
        schema_context: Optional[SchemaContext],
        schema_dict: Dict,
    ) -> List[ColumnLineageDep]:
        """Trace lineage for a single target column."""
        dependencies = []

        # Determine transformation type from the expression
        transformation_type = self._classify_transformation(expression)
        transformation_sql = self._get_transformation_sql(expression, transformation_type)

        # Find all source columns referenced in the expression
        source_cols = self._find_source_columns(expression, select_stmt, schema_context)

        for source_object_id, source_column in source_cols:
            dep = ColumnLineageDep(
                source_object_id=source_object_id,
                source_column=source_column,
                target_object_id=target_object_id,
                target_column=target_col,
                transformation=transformation_sql,
                transformation_type=transformation_type,
            )
            dependencies.append(dep)

        return dependencies

    def _classify_transformation(self, expression: exp.Expression) -> str:
        """Classify the type of transformation applied to a column."""
        if isinstance(expression, exp.Column):
            return "DIRECT"

        if isinstance(expression, exp.Alias):
            return self._classify_transformation(expression.this)

        if isinstance(expression, exp.Cast):
            return "CAST"

        if isinstance(expression, exp.Case):
            return "CASE"

        if isinstance(expression, exp.Anonymous):
            func_name = expression.name.upper() if hasattr(expression, 'name') else ""
            if func_name in AGGREGATE_FUNCTIONS:
                return "AGGREGATE"
            if func_name in KNOWN_FUNCTIONS:
                return "FUNCTION"
            return "FUNCTION"

        # Check for function calls
        if isinstance(expression, (exp.Func, exp.AggFunc)):
            func_name = expression.key.upper() if hasattr(expression, 'key') else ""
            # Also check the class name
            if not func_name:
                func_name = type(expression).__name__.upper()

            if func_name in AGGREGATE_FUNCTIONS or isinstance(expression, exp.AggFunc):
                return "AGGREGATE"
            return "FUNCTION"

        # Check for arithmetic/string operations
        if isinstance(expression, (exp.Binary, exp.Add, exp.Sub, exp.Mul, exp.Div,
                                   exp.Concat, exp.DPipe)):
            return "EXPRESSION"

        # Unknown transformation
        return "UNKNOWN"

    def _get_transformation_sql(self, expression: exp.Expression, transformation_type: str) -> Optional[str]:
        """Get SQL representation of the transformation if not DIRECT."""
        if transformation_type == "DIRECT":
            return None

        try:
            sql = expression.sql(dialect=self.sqlglot_dialect)
            # Truncate if too long
            if len(sql) > 200:
                sql = sql[:197] + "..."
            return sql
        except Exception:
            return None

    def _find_source_columns(
        self,
        expression: exp.Expression,
        select_stmt: exp.Select,
        schema_context: Optional[SchemaContext],
    ) -> List[Tuple[str, str]]:
        """
        Find all source columns referenced in an expression.

        Returns list of (object_id, column_name) tuples.
        """
        source_cols: List[Tuple[str, str]] = []

        # Build table alias map from the FROM clause
        alias_map = self._build_alias_map(select_stmt)

        # Find all Column references in the expression
        for col in expression.find_all(exp.Column):
            # Get table reference - could be string or Identifier
            table_ref = None
            if hasattr(col, 'table') and col.table:
                # Convert to string if it's an Identifier
                table_ref = str(col.table) if col.table else None

            # Get column name
            col_name = str(col.name) if hasattr(col, 'name') and col.name else str(col)

            # Resolve table reference to object_id
            object_id = self._resolve_table_ref(table_ref, alias_map, schema_context)

            if object_id:
                source_cols.append((object_id, col_name))
            elif table_ref:
                # Use the table reference as-is if we can't resolve it
                source_cols.append((table_ref.upper(), col_name))

        # Also check if expression itself is a Column (for simple cases)
        if isinstance(expression, exp.Column) and not source_cols:
            table_ref = str(expression.table) if hasattr(expression, 'table') and expression.table else None
            col_name = str(expression.name) if hasattr(expression, 'name') and expression.name else None
            if col_name:
                object_id = self._resolve_table_ref(table_ref, alias_map, schema_context)
                if object_id:
                    source_cols.append((object_id, col_name))
                elif table_ref:
                    source_cols.append((table_ref.upper(), col_name))

        return source_cols

    def _build_alias_map(self, select_stmt: exp.Select) -> Dict[str, str]:
        """Build a map of table aliases to table names from the FROM clause."""
        alias_map: Dict[str, str] = {}

        # Get FROM clause
        from_clause = select_stmt.find(exp.From)
        if from_clause:
            self._extract_table_aliases(from_clause, alias_map)

        # Get JOIN clauses
        for join in select_stmt.find_all(exp.Join):
            self._extract_table_aliases(join, alias_map)

        return alias_map

    def _extract_table_aliases(self, clause: exp.Expression, alias_map: Dict[str, str]) -> None:
        """Extract table aliases from a FROM or JOIN clause."""
        for table in clause.find_all(exp.Table):
            table_name = self._get_full_table_name(table)

            # Check for alias
            alias = table.alias if hasattr(table, 'alias') and table.alias else None

            if alias:
                alias_map[alias.upper()] = table_name.upper()
            else:
                # Use table name as its own key
                short_name = table.name.upper() if hasattr(table, 'name') else table_name.upper()
                alias_map[short_name] = table_name.upper()

    def _get_full_table_name(self, table: exp.Table) -> str:
        """Get the full table name including schema if present."""
        parts = []

        # Catalog (for BigQuery: project)
        if hasattr(table, 'catalog') and table.catalog:
            parts.append(table.catalog)

        # Schema (database/dataset)
        if hasattr(table, 'db') and table.db:
            parts.append(table.db)

        # Table name
        if hasattr(table, 'name') and table.name:
            parts.append(table.name)

        return ".".join(parts) if parts else str(table)

    def _resolve_table_ref(
        self,
        table_ref: Optional[str],
        alias_map: Dict[str, str],
        schema_context: Optional[SchemaContext],
    ) -> Optional[str]:
        """Resolve a table reference to an object_id."""
        if not table_ref:
            # If no table reference, we can't resolve
            return None

        table_ref_upper = table_ref.upper()

        # Check alias map first
        if table_ref_upper in alias_map:
            return alias_map[table_ref_upper]

        # Check schema context
        if schema_context and table_ref_upper in schema_context.alias_map:
            return schema_context.alias_map[table_ref_upper]

        # Return as-is if we can't resolve
        return table_ref_upper

    def _build_schema_dict(self, schema_context: SchemaContext) -> Dict[str, Dict[str, str]]:
        """Build a schema dict for sqlglot lineage."""
        schema_dict = {}

        for obj_id, columns in schema_context.object_columns.items():
            # Parse object_id to get schema and table
            parts = obj_id.split(".")
            if len(parts) >= 2:
                schema_name = parts[0]
                table_name = parts[1]

                if schema_name not in schema_dict:
                    schema_dict[schema_name] = {}

                # Add columns with dummy types (sqlglot just needs the names)
                schema_dict[schema_name][table_name] = {col: "VARCHAR" for col in columns}

        return schema_dict

    def _fallback_extract(
        self,
        sql: str,
        target_object_id: str,
        schema_context: Optional[SchemaContext],
    ) -> List[ColumnLineageDep]:
        """
        Fallback extraction when sqlglot lineage fails.

        Tries to parse SELECT clause for target columns and match source columns.
        """
        import re

        dependencies = []
        sql_upper = sql.upper()

        # Try to parse with sqlglot to at least get target columns
        target_columns = []
        try:
            sql_clean = self._clean_sql(sql)
            parsed = sqlglot.parse_one(sql_clean, dialect=self.sqlglot_dialect)
            if parsed:
                select_stmt = self._find_select(parsed)
                if select_stmt:
                    target_columns = self._extract_select_columns(select_stmt)
        except Exception:
            pass

        # Build alias map from SQL using regex
        alias_map = {}
        # Pattern: FROM/JOIN schema.table alias or schema.table AS alias
        table_pattern = r'(?:FROM|JOIN)\s+([A-Z_][A-Z0-9_]*(?:\.[A-Z_][A-Z0-9_]*)?)\s+(?:AS\s+)?([A-Z_][A-Z0-9_]*)?'
        for match in re.finditer(table_pattern, sql_upper):
            table_name = match.group(1)
            alias = match.group(2)
            if alias and alias not in ('ON', 'WHERE', 'AND', 'OR', 'LEFT', 'RIGHT', 'INNER', 'OUTER', 'FULL', 'CROSS', 'JOIN'):
                alias_map[alias] = table_name
            # Also map short table name to full name
            short_name = table_name.split('.')[-1]
            alias_map[short_name] = table_name

        # Find source column references: table.column or alias.column
        col_pattern = r'([A-Z_][A-Z0-9_]*)\.([A-Z_][A-Z0-9_]*)'
        source_refs = []
        for match in re.finditer(col_pattern, sql_upper):
            table_ref = match.group(1)
            col_name = match.group(2)

            # Skip keywords
            if table_ref in ('SYS', 'DUAL', 'AS', 'ON', 'AND', 'OR'):
                continue

            # Resolve table reference
            object_id = alias_map.get(table_ref, table_ref)
            if schema_context and table_ref in schema_context.alias_map:
                object_id = schema_context.alias_map[table_ref]

            source_refs.append((object_id, col_name))

        # If we have target columns from parsing, try to match source to target
        if target_columns:
            for target_col, expression in target_columns:
                # Find source columns in this expression
                expr_sql = expression.sql(dialect=self.sqlglot_dialect).upper() if hasattr(expression, 'sql') else ""

                # Determine transformation type
                trans_type = "DIRECT"
                if HAS_SQLGLOT_LINEAGE and isinstance(expression, exp.Column):
                    trans_type = "DIRECT"
                elif any(agg in expr_sql for agg in ['SUM(', 'COUNT(', 'AVG(', 'MIN(', 'MAX(']):
                    trans_type = "AGGREGATE"
                elif 'CASE' in expr_sql:
                    trans_type = "CASE"
                elif 'CAST(' in expr_sql or '::' in expr_sql:
                    trans_type = "CAST"
                elif any(fn in expr_sql for fn in ['COALESCE(', 'NVL(', 'CONCAT(']):
                    trans_type = "FUNCTION"

                # Find source columns referenced in this expression
                for source_obj, source_col in source_refs:
                    if source_col in expr_sql or f"{source_obj.split('.')[-1]}.{source_col}" in expr_sql:
                        dep = ColumnLineageDep(
                            source_object_id=source_obj,
                            source_column=source_col,
                            target_object_id=target_object_id,
                            target_column=target_col,
                            transformation=expr_sql[:200] if trans_type != "DIRECT" else None,
                            transformation_type=trans_type,
                        )
                        dependencies.append(dep)
        else:
            # Can't determine target columns - skip creating UNKNOWN entries
            logger.warning(f"Could not parse target columns for {target_object_id}")

        return dependencies


def extract_column_lineage_for_view(
    view_definition: str,
    view_id: str,
    source_tables: Dict[str, List[str]],
    dialect: str = "exasol",
) -> List[ColumnLineageDep]:
    """
    Convenience function to extract column lineage for a view.

    Args:
        view_definition: SQL definition of the view
        view_id: Object ID of the view (e.g., "DWH.MY_VIEW")
        source_tables: Dict mapping table IDs to their column lists
        dialect: SQL dialect

    Returns:
        List of column-level dependencies
    """
    extractor = ColumnLineageExtractor(dialect=dialect)

    # Build schema context
    schema_context = SchemaContext(object_columns=source_tables)

    return extractor.extract_column_lineage(view_definition, view_id, schema_context)


# Example usage and testing
if __name__ == "__main__":
    # Test SQL
    test_sql = """
    SELECT
        o.ORDER_ID,
        c.CUSTOMER_NAME,
        SUM(o.AMOUNT) as TOTAL_AMOUNT,
        CAST(o.ORDER_DATE AS DATE) as ORDER_DATE,
        CASE WHEN o.STATUS = 'COMPLETED' THEN 'Done' ELSE 'Pending' END as STATUS_LABEL
    FROM SALES.ORDERS o
    JOIN CUSTOMERS.CUSTOMER c ON o.CUSTOMER_ID = c.ID
    WHERE o.ORDER_DATE >= '2024-01-01'
    """

    # Schema context
    schema = SchemaContext(
        object_columns={
            "SALES.ORDERS": ["ORDER_ID", "CUSTOMER_ID", "AMOUNT", "ORDER_DATE", "STATUS"],
            "CUSTOMERS.CUSTOMER": ["ID", "CUSTOMER_NAME", "EMAIL"],
        }
    )

    extractor = ColumnLineageExtractor(dialect="exasol")
    deps = extractor.extract_column_lineage(test_sql, "DWH.SALES_SUMMARY", schema)

    print("Column-level dependencies:")
    for dep in deps:
        print(f"  {dep.source_object_id}.{dep.source_column} -> {dep.target_object_id}.{dep.target_column}")
        print(f"    Type: {dep.transformation_type}, Transform: {dep.transformation}")
