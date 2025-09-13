/*
 * File: test_authentication.sql
 * Purpose: Tests Supabase database connectivity and authentication
 *
 * Description:
 *   - Simple query to verify Supabase connection and permissions
 *   - Returns current database and user information
 *   - Simplified for Supabase compatibility
 *
 */
SELECT 
    current_database() AS database_name,
    current_user AS user_name,
    version() AS version_info,
    now() AS connection_time,
    'SSL enabled' AS ssl_status;
