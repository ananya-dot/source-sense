/*
 * File: extract_column.sql
 * Purpose: Extracts column metadata from Supabase
 *
 * Description:
 *   - Retrieves detailed column information including data type, constraints, and defaults
 *   - Filters out system tables and applies include/exclude regex patterns
 *   - Supabase-specific: includes JSONB columns and generated columns
 *
 * Parameters:
 *   {normalized_exclude_regex} - Regex pattern for columns to exclude
 *   {normalized_include_regex} - Regex pattern for columns to include
 *
 */
SELECT
    c.table_catalog,
    c.table_schema,
    c.table_name,
    c.column_name,
    c.ordinal_position,
    c.column_default,
    c.is_nullable,
    c.data_type,
    c.character_maximum_length,
    c.character_octet_length,
    c.numeric_precision,
    c.numeric_precision_radix,
    c.numeric_scale,
    c.datetime_precision,
    c.interval_type,
    c.interval_precision,
    c.character_set_catalog,
    c.character_set_schema,
    c.character_set_name,
    c.collation_catalog,
    c.collation_schema,
    c.collation_name,
    c.domain_catalog,
    c.domain_schema,
    c.domain_name,
    c.udt_catalog,
    c.udt_schema,
    c.udt_name,
    c.scope_catalog,
    c.scope_schema,
    c.scope_name,
    c.maximum_cardinality,
    c.dtd_identifier,
    c.is_self_referencing,
    c.is_identity,
    c.identity_generation,
    c.identity_start,
    c.identity_increment,
    c.identity_maximum,
    c.identity_minimum,
    c.identity_cycle,
    c.is_generated,
    c.generation_expression,
    c.is_updatable,
    pg_catalog.col_description(pgc.oid, c.ordinal_position) AS column_description,
    CASE 
        WHEN c.data_type = 'jsonb' THEN 'supabase_jsonb'
        WHEN c.data_type = 'uuid' THEN 'supabase_uuid'
        WHEN c.column_name LIKE '%_id' AND c.data_type = 'uuid' THEN 'foreign_key_uuid'
        WHEN c.column_name = 'created_at' OR c.column_name = 'updated_at' THEN 'timestamp_column'
        ELSE 'standard_column'
    END AS column_category
FROM
    information_schema.columns c
LEFT JOIN pg_class pgc ON pgc.relname = c.table_name
LEFT JOIN pg_namespace pgn ON pgn.oid = pgc.relnamespace AND pgn.nspname = c.table_schema
WHERE c.table_schema NOT IN ('information_schema', 'pg_catalog')
    AND CONCAT(c.table_catalog, '.', c.table_schema, '.', c.table_name, '.', c.column_name) NOT REGEXP '{normalized_exclude_regex}'
    AND CONCAT(c.table_catalog, '.', c.table_schema, '.', c.table_name, '.', c.column_name) REGEXP '{normalized_include_regex}';
