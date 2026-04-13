#!/usr/bin/env python3
"""
Adapt DuckDB SQL to Postgres dialect.
Handles function/operator incompatibilities between the two engines.
"""

import re


def _fix_round_calls(sql: str) -> str:
    """Fix ROUND(expr, n) -> ROUND((expr)::numeric, n) using paren balancing."""
    result = []
    i = 0
    upper = sql.upper()
    while i < len(sql):
        # Find ROUND(
        idx = upper.find('ROUND(', i)
        if idx == -1:
            result.append(sql[i:])
            break
        # Check it's a word boundary
        if idx > 0 and (upper[idx-1].isalnum() or upper[idx-1] == '_'):
            result.append(sql[i:idx+6])
            i = idx + 6
            continue
        result.append(sql[i:idx])
        # Find matching close paren
        start = idx + 5  # position of '('
        depth = 1
        j = start + 1
        while j < len(sql) and depth > 0:
            if sql[j] == '(':
                depth += 1
            elif sql[j] == ')':
                depth -= 1
            j += 1
        if depth != 0:
            # Unbalanced — leave as-is
            result.append(sql[idx:j])
            i = j
            continue
        # inner is everything between ROUND( and the matching )
        inner = sql[start+1:j-1]
        # Find the last comma at depth 0 — that separates expr from precision
        last_comma = -1
        d = 0
        for k, ch in enumerate(inner):
            if ch == '(':
                d += 1
            elif ch == ')':
                d -= 1
            elif ch == ',' and d == 0:
                last_comma = k
        if last_comma == -1:
            # No comma — ROUND(expr) with no precision, leave as-is
            result.append(f"ROUND({inner})")
        else:
            expr = inner[:last_comma].strip()
            prec = inner[last_comma+1:].strip()
            if '::numeric' in expr:
                result.append(f"ROUND({expr}, {prec})")
            else:
                result.append(f"ROUND(({expr})::numeric, {prec})")
        i = j
    return ''.join(result)


def adapt_sql_for_postgres(raw_sql: str) -> str:
    """Convert DuckDB SQL to Postgres-compatible SQL."""
    sql = raw_sql.strip()

    # MEDIAN(expr) -> PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY expr)
    def _replace_median(m):
        expr = m.group(1)
        return f"PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {expr})"
    sql = re.sub(r'\bMEDIAN\s*\(([^)]+)\)', _replace_median, sql, flags=re.IGNORECASE)

    # EPOCH(interval) -> EXTRACT(EPOCH FROM interval)
    def _replace_epoch(m):
        expr = m.group(1)
        return f"EXTRACT(EPOCH FROM {expr})"
    sql = re.sub(r'\bEPOCH\s*\(([^)]+)\)', _replace_epoch, sql, flags=re.IGNORECASE)

    # YEAR(date) -> EXTRACT(YEAR FROM date)::INTEGER
    def _replace_year(m):
        expr = m.group(1)
        return f"EXTRACT(YEAR FROM {expr})::INTEGER"
    sql = re.sub(r'\bYEAR\s*\(([^)]+)\)', _replace_year, sql, flags=re.IGNORECASE)

    # MONTH(date) -> EXTRACT(MONTH FROM date)::INTEGER
    def _replace_month(m):
        expr = m.group(1)
        return f"EXTRACT(MONTH FROM {expr})::INTEGER"
    sql = re.sub(r'\bMONTH\s*\(([^)]+)\)', _replace_month, sql, flags=re.IGNORECASE)

    # QUARTER(date) -> EXTRACT(QUARTER FROM date)::INTEGER
    def _replace_quarter(m):
        expr = m.group(1)
        return f"EXTRACT(QUARTER FROM {expr})::INTEGER"
    sql = re.sub(r'\bQUARTER\s*\(([^)]+)\)', _replace_quarter, sql, flags=re.IGNORECASE)

    # QUANTILE_CONT -> PERCENTILE_CONT
    sql = re.sub(r'\bQUANTILE_CONT\b', 'PERCENTILE_CONT', sql, flags=re.IGNORECASE)

    # LIST_AGG / GROUP_CONCAT -> STRING_AGG
    sql = re.sub(r'\bLIST_AGG\b', 'STRING_AGG', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bGROUP_CONCAT\b', 'STRING_AGG', sql, flags=re.IGNORECASE)
    # LIST( -> ARRAY_AGG(
    sql = re.sub(r'\bLIST\b\s*\(', 'ARRAY_AGG(', sql, flags=re.IGNORECASE)

    # STRFTIME(fmt, expr) -> TO_CHAR(expr, fmt)
    def _replace_strftime(m):
        fmt = m.group(1)
        expr = m.group(2)
        return f"TO_CHAR({expr}, {fmt})"
    sql = re.sub(r"\bSTRFTIME\s*\(\s*('[^']*')\s*,\s*([^)]+)\)",
                 _replace_strftime, sql, flags=re.IGNORECASE)

    # ROUND(double, int) -> ROUND((expr)::numeric, int) for Postgres
    # Use paren-balancing instead of regex to avoid catastrophic backtracking
    sql = _fix_round_calls(sql)

    # DATEDIFF('unit', a, b) -> (b - a) for days
    def _replace_datediff(m):
        unit = m.group(1).strip("'\"").lower()
        a = m.group(2).strip()
        b = m.group(3).strip()
        if unit == 'day':
            return f"({b} - {a})"
        elif unit == 'month':
            return (f"(EXTRACT(YEAR FROM {b}) * 12 + EXTRACT(MONTH FROM {b}) "
                    f"- EXTRACT(YEAR FROM {a}) * 12 - EXTRACT(MONTH FROM {a}))")
        return f"({b} - {a})"
    sql = re.sub(r"\bDATEDIFF\s*\(\s*('[^']*')\s*,\s*([^,]+)\s*,\s*([^)]+)\)",
                 _replace_datediff, sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bDATE_DIFF\s*\(\s*('[^']*')\s*,\s*([^,]+)\s*,\s*([^)]+)\)",
                 _replace_datediff, sql, flags=re.IGNORECASE)

    # ::DOUBLE -> ::DOUBLE PRECISION
    sql = re.sub(r'::DOUBLE\b(?!\s+PRECISION)', '::DOUBLE PRECISION', sql, flags=re.IGNORECASE)

    # DuckDB REGEXP_MATCHES -> Postgres ~ operator (can't auto-convert cleanly)
    # DuckDB ILIKE works in Postgres too

    # DuckDB LEN() -> LENGTH()
    sql = re.sub(r'\bLEN\s*\(', 'LENGTH(', sql, flags=re.IGNORECASE)

    # DuckDB DAYOFWEEK() -> EXTRACT(DOW FROM ...)
    def _replace_dow(m):
        expr = m.group(1)
        return f"EXTRACT(DOW FROM {expr})::INTEGER"
    sql = re.sub(r'\bDAYOFWEEK\s*\(([^)]+)\)', _replace_dow, sql, flags=re.IGNORECASE)

    # DuckDB DAYNAME() -> TO_CHAR(expr, 'Day')
    def _replace_dayname(m):
        expr = m.group(1)
        return f"TRIM(TO_CHAR({expr}, 'Day'))"
    sql = re.sub(r'\bDAYNAME\s*\(([^)]+)\)', _replace_dayname, sql, flags=re.IGNORECASE)

    # DuckDB MONTHNAME() -> TO_CHAR(expr, 'Month')
    def _replace_monthname(m):
        expr = m.group(1)
        return f"TRIM(TO_CHAR({expr}, 'Month'))"
    sql = re.sub(r'\bMONTHNAME\s*\(([^)]+)\)', _replace_monthname, sql, flags=re.IGNORECASE)

    return sql
