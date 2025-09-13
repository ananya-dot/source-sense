/*
 * File: extract_table.sql
 * Purpose: Extracts table metadata from Supabase
 *
 * Description:
 *   - Retrieves table information including type, size, and row count
 *   - Filters out system tables and applies include/exclude regex patterns
 *   - Supabase-specific: includes RLS (Row Level Security) information
 *
 * Parameters:
 *   {normalized_exclude_regex} - Regex pattern for tables to exclude
 *   {normalized_include_regex} - Regex pattern for tables to include
 *
 */
SELECT
    t.table_catalog,
    t.table_schema,
    t.table_name,
    t.table_type,
    pg_size_pretty(pg_total_relation_size(c.oid)) AS table_size,
    pg_stat_get_live_tuples(c.oid) AS row_count,
    pg_stat_get_dead_tuples(c.oid) AS dead_row_count,
    pg_stat_get_last_analyze(c.oid) AS last_analyze,
    pg_stat_get_last_autoanalyze(c.oid) AS last_autoanalyze,
    pg_stat_get_last_vacuum(c.oid) AS last_vacuum,
    pg_stat_get_last_autovacuum(c.oid) AS last_autovacuum,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM pg_policy 
            WHERE schemaname = t.table_schema 
            AND tablename = t.table_name
        ) THEN true 
        ELSE false 
    END AS has_row_level_security,
    CASE 
        WHEN t.table_schema = 'auth' THEN 'auth_table'
        WHEN t.table_schema = 'storage' THEN 'storage_table'
        WHEN t.table_schema = 'public' THEN 'user_table'
        ELSE 'system_table'
    END AS table_category
FROM
    information_schema.tables t
LEFT JOIN pg_class c ON c.relname = t.table_name
LEFT JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = t.table_schema
WHERE t.table_schema NOT IN ('information_schema', 'pg_catalog')
    AND CONCAT(t.table_catalog, '.', t.table_schema, '.', t.table_name) NOT REGEXP '{normalized_exclude_regex}'
    AND CONCAT(t.table_catalog, '.', t.table_schema, '.', t.table_name) REGEXP '{normalized_include_regex}';
