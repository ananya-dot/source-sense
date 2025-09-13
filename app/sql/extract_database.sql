/*
 * File: extract_database.sql
 * Purpose: Extracts list of databases from Supabase
 *
 * Description:
 *   - Retrieves the current database (Supabase projects typically have one main database)
 *   - Excludes system databases
 *
 */
SELECT current_database() AS database_name;
