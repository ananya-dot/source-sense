/*
 * File: tables_check.sql
 * Purpose: Checks if tables exist in the specified schema
 *
 * Description:
 *   - Verifies table existence before extraction
 *   - Returns count of tables in each schema
 *   - Supabase-specific: categorizes tables by schema type
 *
 */
SELECT 
    schemaname,
    COUNT(*) AS table_count,
    CASE 
        WHEN schemaname = 'public' THEN 'user_schema'
        WHEN schemaname = 'auth' THEN 'auth_schema'
        WHEN schemaname = 'storage' THEN 'storage_schema'
        WHEN schemaname LIKE 'supabase_%' THEN 'supabase_system'
        ELSE 'other'
    END AS schema_type
FROM pg_catalog.pg_tables
WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
GROUP BY schemaname
ORDER BY schemaname;
