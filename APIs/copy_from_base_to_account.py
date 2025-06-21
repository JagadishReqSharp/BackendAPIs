from datetime import datetime
from typing import Dict, List, Any, Optional
import logging
from dataclasses import dataclass

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class CopyResult:
    """Result of copying operation for a single table"""
    table_name: str
    records_copied: int
    success: bool
    error_message: Optional[str] = None


class ProjectRecordsCopier:
    """API class for copying records between projects"""

    def __init__(self, db_connection):
        """
        Initialize with database connection

        Args:
            db_connection: Your database connection object (e.g., SQLAlchemy session, psycopg2 connection, etc.)
        """
        self.db_connection = db_connection
        self.default_tables = [
            'ACCOUNT_STATUSES',
            'USER_PROJECTS',
            'PRODUCTS_BY_PROJECT',
            'PRODUCTS_BY_PROJECT_USER_ACCESS',
            'INTEGRATION_SYSTEMS',
            'KEY_ATTRIBUTES_HEADER',
            'KEY_ATTRIBUTES_LIST',
            'FUNCTIONAL_LEVELS',
            'BUSINESS_TEAMS'
        ]

    def copy_project_records(self, corporate_account: str, copy_from_project_id: str,
                             copy_to_project_id: str, table_names: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Copy records from one project to another across specified tables

        Args:
            corporate_account: The corporate account identifier
            copy_from_project_id: Source project ID to copy from
            copy_to_project_id: Target project ID to copy to
            table_names: Optional list of table names to copy. If None, uses default tables.

        Returns:
            Dict containing operation results and summary
        """
        # Use provided table names or default ones
        tables_to_process = table_names if table_names is not None else self.default_tables

        # Validate table names
        if not tables_to_process or len(tables_to_process) == 0:
            return {
                'success': False,
                'error': 'No tables specified for copying',
                'total_records_copied': 0,
                'failed_tables': [],
                'results_by_table': {},
                'summary': {
                    'corporate_account': corporate_account,
                    'copy_from_project_id': copy_from_project_id,
                    'copy_to_project_id': copy_to_project_id,
                    'tables_processed': 0,
                    'tables_succeeded': 0,
                    'tables_failed': 0
                }
            }
        results = []
        total_records_copied = 0
        failed_tables = []

        logger.info(f"Starting copy operation from project {copy_from_project_id} to {copy_to_project_id}")

        try:
            # Begin transaction
            self.db_connection.begin()

            for table_name in tables_to_process:
                try:
                    result = self._copy_table_records(
                        table_name,
                        corporate_account,
                        copy_from_project_id,
                        copy_to_project_id
                    )
                    results.append(result)

                    if result.success:
                        total_records_copied += result.records_copied
                        logger.info(f"Successfully copied {result.records_copied} records from {table_name}")
                    else:
                        failed_tables.append(table_name)
                        logger.error(f"Failed to copy records from {table_name}: {result.error_message}")

                except Exception as e:
                    error_msg = f"Error copying table {table_name}: {str(e)}"
                    logger.error(error_msg)
                    results.append(CopyResult(table_name, 0, False, error_msg))
                    failed_tables.append(table_name)

            # Commit transaction if no failures, otherwise rollback
            if not failed_tables:
                self.db_connection.commit()
                logger.info("All tables copied successfully. Transaction committed.")
            else:
                self.db_connection.rollback()
                logger.warning(f"Some tables failed to copy: {failed_tables}. Transaction rolled back.")

        except Exception as e:
            self.db_connection.rollback()
            logger.error(f"Transaction failed: {str(e)}")
            raise

        return {
            'success': len(failed_tables) == 0,
            'total_records_copied': total_records_copied,
            'failed_tables': failed_tables,
            'results_by_table': {r.table_name: r for r in results},
            'summary': {
                'corporate_account': corporate_account,
                'copy_from_project_id': copy_from_project_id,
                'copy_to_project_id': copy_to_project_id,
                'tables_processed': len(tables_to_process),
                'tables_succeeded': len(tables_to_process) - len(failed_tables),
                'tables_failed': len(failed_tables)
            }
        }

    def _copy_table_records(self, table_name: str, corporate_account: str,
                            copy_from_project_id: str, copy_to_project_id: str) -> CopyResult:
        """
        Copy records from a specific table

        Args:
            table_name: Name of the table to copy from
            corporate_account: Corporate account identifier
            copy_from_project_id: Source project ID
            copy_to_project_id: Target project ID

        Returns:
            CopyResult object with operation details
        """
        try:
            # Get table columns to build dynamic query
            columns_info = self._get_table_columns(table_name)

            if not columns_info:
                return CopyResult(table_name, 0, False, "Could not retrieve table schema")

            # Build SELECT query for source records
            select_columns = []
            for column in columns_info:
                if column.lower() == 'project_id':
                    select_columns.append(f"'{copy_to_project_id}' as project_id")
                elif column.lower() in ['created_date', 'updated_date']:
                    select_columns.append(f"CURRENT_TIMESTAMP as {column}")
                else:
                    select_columns.append(column)

            select_query = f"""
                SELECT {', '.join(select_columns)}
                FROM {table_name}
                WHERE corporate_account = %s 
                AND project_id = %s
            """

            # Execute SELECT to get records to copy
            cursor = self.db_connection.cursor()
            cursor.execute(select_query, (corporate_account, copy_from_project_id))
            records_to_copy = cursor.fetchall()

            if not records_to_copy:
                return CopyResult(table_name, 0, True, "No records found to copy")

            # Build INSERT query
            column_placeholders = ', '.join(['%s'] * len(columns_info))
            insert_query = f"""
                INSERT INTO {table_name} ({', '.join(columns_info)})
                VALUES ({column_placeholders})
            """

            # Execute INSERT for all records
            cursor.executemany(insert_query, records_to_copy)
            records_copied = cursor.rowcount

            return CopyResult(table_name, records_copied, True)

        except Exception as e:
            return CopyResult(table_name, 0, False, str(e))

    def _get_table_columns(self, table_name: str) -> List[str]:
        """
        Get column names for a table

        Args:
            table_name: Name of the table

        Returns:
            List of column names
        """
        try:
            # This query works for PostgreSQL, MySQL, and most SQL databases
            # Adjust based on your specific database system
            query = """
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s 
                ORDER BY ordinal_position
            """

            cursor = self.db_connection.cursor()
            cursor.execute(query, (table_name.lower(),))
            columns = [row[0] for row in cursor.fetchall()]

            return columns

        except Exception as e:
            logger.error(f"Error getting columns for table {table_name}: {str(e)}")
            return []

    def validate_parameters(self, corporate_account: str, copy_from_project_id: str,
                            copy_to_project_id: str) -> Dict[str, Any]:
        """
        Validate input parameters before copying

        Args:
            corporate_account: Corporate account identifier
            copy_from_project_id: Source project ID
            copy_to_project_id: Target project ID

        Returns:
            Dict with validation results
        """
        errors = []

        if not corporate_account or not corporate_account.strip():
            errors.append("corporate_account is required")

        if not copy_from_project_id or not copy_from_project_id.strip():
            errors.append("copy_from_project_id is required")

        if not copy_to_project_id or not copy_to_project_id.strip():
            errors.append("copy_to_project_id is required")

        if copy_from_project_id == copy_to_project_id:
            errors.append("copy_from_project_id and copy_to_project_id cannot be the same")

        # Check if source project exists and has records
        if not errors:
            try:
                cursor = self.db_connection.cursor()

                # Check at least one table for source project existence
                cursor.execute("""
                    SELECT COUNT(*) FROM CORPORATE_ACCOUNT_PROJECTS 
                    WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID  = %s
                """, (corporate_account, copy_from_project_id))

                source_count = cursor.fetchone()[0]
                if source_count == 0:
                    errors.append(f"Source project {copy_from_project_id} not found")

                # Check if target project already has records (optional warning)
                cursor.execute("""
                    SELECT COUNT(*) FROM CORPORATE_ACCOUNT_PROJECTS 
                    WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID  = %s
                """, (corporate_account, copy_to_project_id))

                target_count = cursor.fetchone()[0]
                warning = None
                if target_count == 0:
                    errors.append(f"Target project {copy_to_project_id} not found")

            except Exception as e:
                errors.append(f"Error validating projects: {str(e)}")
                warning = None

        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'warning': warning if 'warning' in locals() else None
        }


# Usage example and API endpoint functions
def copy_project_records_api(db_connection, corporate_account: str,
                             copy_from_project_id: str, copy_to_project_id: str,
                             table_names: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Main API function to copy project records

    Args:
        db_connection: Database connection object
        corporate_account: Corporate account identifier
        copy_from_project_id: Source project ID to copy from
        copy_to_project_id: Target project ID to copy to
        table_names: Optional list of table names to copy. If None, uses default tables:
                    ['ACCOUNT_STATUSES', 'USER_PROJECTS', 'PRODUCTS_BY_PROJECT',
                     'PRODUCTS_BY_PROJECT_USER_ACCESS', 'INTEGRATION_SYSTEMS',
                     'KEY_ATTRIBUTES_HEADER', 'KEY_ATTRIBUTES_LIST',
                     'FUNCTIONAL_LEVELS', 'BUSINESS_TEAMS']

    Returns:
        Dict containing operation results
    """
    copier = ProjectRecordsCopier(db_connection)

    # Validate parameters
    validation_result = copier.validate_parameters(
        corporate_account, copy_from_project_id, copy_to_project_id
    )

    if not validation_result['valid']:
        return {
            'success': False,
            'error': 'Validation failed',
            'validation_errors': validation_result['errors'],
            'warning': validation_result.get('warning')
        }

    # Perform the copy operation
    try:
        result = copier.copy_project_records(
            corporate_account, copy_from_project_id, copy_to_project_id, table_names
        )

        if validation_result.get('warning'):
            result['warning'] = validation_result['warning']

        return result

    except Exception as e:
        logger.error(f"Copy operation failed: {str(e)}")
        return {
            'success': False,
            'error': f'Copy operation failed: {str(e)}',
            'total_records_copied': 0
        }


# Example usage with different database connections
def example_usage():
    """Example of how to use the API with different database types"""

    # Example 1: Using with psycopg2 (PostgreSQL) - Copy all default tables
    """
    import psycopg2

    conn = psycopg2.connect(
        host="localhost",
        database="your_db",
        user="your_user",
        password="your_password"
    )

    result = copy_project_records_api(
        db_connection=conn,
        corporate_account="CORP123",
        copy_from_project_id="PROJ001",
        copy_to_project_id="PROJ002"
    )

    print(f"Copy operation result: {result}")
    """

    # Example 2: Using with specific table names
    """
    result = copy_project_records_api(
        db_connection=conn,
        corporate_account="CORP123",
        copy_from_project_id="PROJ001",
        copy_to_project_id="PROJ002",
        table_names=["USER_PROJECTS", "PRODUCTS_BY_PROJECT", "BUSINESS_TEAMS"]
    )

    print(f"Copy operation result: {result}")
    """

    # Example 3: Using with SQLAlchemy - Copy only specific tables
    """
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    engine = create_engine('postgresql://user:password@localhost/dbname')
    Session = sessionmaker(bind=engine)
    session = Session()

    # Copy only specific tables
    selected_tables = [
        "ACCOUNT_STATUSES",
        "USER_PROJECTS", 
        "INTEGRATION_SYSTEMS"
    ]

    result = copy_project_records_api(
        db_connection=session,
        corporate_account="CORP123",
        copy_from_project_id="PROJ001",
        copy_to_project_id="PROJ002",
        table_names=selected_tables
    )

    print(f"Copy operation result: {result}")
    """

    # Example 4: Copy single table
    """
    result = copy_project_records_api(
        db_connection=conn,
        corporate_account="CORP123",
        copy_from_project_id="PROJ001",
        copy_to_project_id="PROJ002",
        table_names=["USER_PROJECTS"]  # Copy only USER_PROJECTS table
    )

    print(f"Copy operation result: {result}")
    """

    pass


if __name__ == "__main__":
    # Run example usage
    example_usage()