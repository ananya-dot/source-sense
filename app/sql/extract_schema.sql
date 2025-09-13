/*
 * File: extract_schema.sql
 * Purpose: Extracts detailed schema information from Supabase
 *
 * Description:
 *   - Retrieves schema metadata including ownership and description
 *   - Counts tables and views in each schema
 *   - Filters out system schemas and applies include/exclude regex patterns
 *   - Supabase-specific: includes auth, storage, and public schemas
 *
 * Parameters:
 *   {normalized_exclude_regex} - Regex pattern for schemas to exclude
 *   {normalized_include_regex} - Regex pattern for schemas to include
 *
 */
SELECT
    n.nspname AS schema_name,
    pg_catalog.pg_get_userbyid(n.nspowner) AS schema_owner,
    pg_catalog.obj_description(n.oid, 'pg_namespace') AS schema_description,
    CAST(table_counts.table_count AS CHAR) AS table_count,
    CAST(table_counts.view_count AS CHAR) AS view_count,
    CASE 
        WHEN n.nspname = 'public' THEN 'user_schema'
        WHEN n.nspname = 'auth' THEN 'auth_schema'
        WHEN n.nspname = 'storage' THEN 'storage_schema'
        WHEN n.nspname LIKE 'supabase_%' THEN 'supabase_system'
        ELSE 'other'
    END AS schema_type
FROM
    pg_catalog.pg_namespace n
LEFT JOIN (
    SELECT
        schemaname,
        SUM(CASE WHEN tablename NOT LIKE 'pg_%' AND schemaname NOT IN ('information_schema', 'pg_catalog') THEN 1 ELSE 0 END) as table_count,
        SUM(CASE WHEN tablename LIKE 'pg_%' OR schemaname IN ('information_schema', 'pg_catalog') THEN 1 ELSE 0 END) as view_count
    FROM
        pg_catalog.pg_tables
    GROUP BY
        schemaname
) as table_counts
ON n.nspname = table_counts.schemaname
WHERE n.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
    AND CONCAT(current_database(), '.', n.nspname) NOT REGEXP '{normalized_exclude_regex}'
    AND CONCAT(current_database(), '.', n.nspname) REGEXP '{normalized_include_regex}';
