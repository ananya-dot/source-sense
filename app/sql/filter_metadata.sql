/*
 * File: filter_metadata.sql
 * Purpose: Filters metadata based on include/exclude patterns
 *
 * Description:
 *   - Generic filtering query template for metadata extraction
 *   - Can be used for databases, schemas, tables, or columns
 *
 * Parameters:
 *   {table_name} - Name of the table to filter
 *   {normalized_exclude_regex} - Regex pattern for items to exclude
 *   {normalized_include_regex} - Regex pattern for items to include
 *
 */
SELECT *
FROM {table_name}
WHERE {table_name} NOT REGEXP '{normalized_exclude_regex}'
    AND {table_name} REGEXP '{normalized_include_regex}';
