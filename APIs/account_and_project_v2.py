import re
from flask import Flask, request, jsonify, Blueprint
from flask_cors import CORS
import mysql.connector
from mysql.connector.constants import flag_is_set
from foundational_v2 import validate_status
import config
from datetime import datetime, timedelta
import logging
import jwt
from config import TOKEN_EXPIRY_DAYS
from argon2 import PasswordHasher
from functools import wraps
from access_validation_at_api_level import validate_access

from typing import Dict, List, Any, Optional
from dataclasses import dataclass


from config import SECRET_KEY
from foundational_v2 import generate_next_sequence , validate_corporate_account, validate_project_id, validate_functional_domain, validate_project_prefix, validate_user_id
from utils import token_required



# Create a blueprint for user-related routes
account_and_project_blueprint = Blueprint('account_and_project', __name__)



# Modified token_required decorator for blueprints
def blueprint_token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        # If your token_required is a function that returns a decorator
        auth_decorator = token_required(f)
        return auth_decorator(*args, **kwargs)
    return decorated


ph = PasswordHasher()

# Hash password
def hash_password(password: str) -> str:
    try:
        return ph.hash(password)
    except Exception as e:
        raise Exception("Error hashing password")

# Verify password
def verify_password(stored_hash: str, provided_password: str) -> bool:
    try:
        return ph.verify(stored_hash, provided_password)
    except Exception:
        return False



app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": ["http://localhost:3000"]}})
logging.basicConfig(filename='debugging.log', level=logging.DEBUG)


@dataclass
class CopyResult:
    """Result of copying operation for a single table"""
    table_name: str
    records_copied: int
    success: bool
    error_message: Optional[str] = None


class ProjectRecordsCopier:
    """Class for copying records between projects"""

    def __init__(self, connection):
        """
        Initialize with database connection

        Args:
            connection: MySQL database connection object
        """
        self.connection = connection
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

        logging.info(f"Starting copy operation from project {copy_from_project_id} to {copy_to_project_id}")

        try:
            # Begin transaction
            self.connection.start_transaction()

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
                        logging.info(f"Successfully copied {result.records_copied} records from {table_name}")
                    else:
                        failed_tables.append(table_name)
                        logging.error(f"Failed to copy records from {table_name}: {result.error_message}")

                except Exception as e:
                    error_msg = f"Error copying table {table_name}: {str(e)}"
                    logging.error(error_msg)
                    results.append(CopyResult(table_name, 0, False, error_msg))
                    failed_tables.append(table_name)

            # Commit transaction if no failures, otherwise rollback
            if not failed_tables:
                self.connection.commit()
                logging.info("All tables copied successfully. Transaction committed.")
            else:
                self.connection.rollback()
                logging.warning(f"Some tables failed to copy: {failed_tables}. Transaction rolled back.")

        except Exception as e:
            self.connection.rollback()
            logging.error(f"Transaction failed: {str(e)}")
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
                    select_columns.append(f"NOW() as {column}")
                else:
                    select_columns.append(column)

            select_query = f"""
                SELECT {', '.join(select_columns)}
                FROM {table_name}
                WHERE corporate_account = %s 
                AND project_id = %s
            """

            # Execute SELECT to get records to copy
            cursor = self.connection.cursor()
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
            # This query works for MySQL
            query = """
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s 
                AND table_schema = DATABASE()
                ORDER BY ordinal_position
            """

            cursor = self.connection.cursor()
            cursor.execute(query, (table_name,))
            columns = [row[0] for row in cursor.fetchall()]

            return columns

        except Exception as e:
            logging.error(f"Error getting columns for table {table_name}: {str(e)}")
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

        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'warning': warning if 'warning' in locals() else None
        }


@account_and_project_blueprint.route('/api/copy_project_records', methods=['POST'])
@token_required
@validate_access
def copy_project_records(current_user):
    """
    API endpoint to copy project records from one project to another

    Expected JSON payload:
    {
        "corporate_account": "CORP123",
        "copy_from_project_id": "PROJ001",
        "copy_to_project_id": "PROJ002",
        "table_names": ["USER_PROJECTS", "PRODUCTS_BY_PROJECT"]  // Optional
    }
    """
    data = request.json
    corporate_account = data.get('corporate_account')
    copy_from_project_id = data.get('copy_from_project_id')
    copy_to_project_id = data.get('copy_to_project_id')
    table_names = data.get('table_names')  # Optional parameter
    requesting_for_corporate_account = data.get('requesting_for_corporate_account')

    # When Super Admin is logged in, they can see all users across other accounts
    if requesting_for_corporate_account:
        corporate_account = requesting_for_corporate_account

    # Validate required parameters
    if not corporate_account:
        return jsonify({
            'status': 'Failed',
            'status_description': 'corporate_account is required'
        })

    if not copy_from_project_id:
        return jsonify({
            'status': 'Failed',
            'status_description': 'copy_from_project_id is required'
        })

    if not copy_to_project_id:
        return jsonify({
            'status': 'Failed',
            'status_description': 'copy_to_project_id is required'
        })

    # Validate corporate account using existing function
    if not validate_corporate_account(corporate_account):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Corporate account is not valid'
        })

    # Validate source project using existing function
    if not validate_project_id(corporate_account, copy_from_project_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Source project ID does not exist or is inactive'
        })

    # Validate target project using existing function
    if not validate_project_id(corporate_account, copy_to_project_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Target project ID does not exist or is inactive'
        })

    # Check if source and target projects are the same
    if copy_from_project_id == copy_to_project_id:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Source and target project IDs cannot be the same'
        })

    sts = "Success"
    sts_description = "Project records copied successfully"
    total_records_copied = 0
    failed_tables = []
    results_by_table = {}

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)

        # Create copier instance
        copier = ProjectRecordsCopier(connection)

        # Validate parameters
        validation_result = copier.validate_parameters(
            corporate_account, copy_from_project_id, copy_to_project_id
        )

        if not validation_result['valid']:
            return jsonify({
                'status': 'Failed',
                'status_description': 'Validation failed',
                'validation_errors': validation_result['errors'],
                'warning': validation_result.get('warning')
            })

        # Perform the copy operation
        result = copier.copy_project_records(
            corporate_account, copy_from_project_id, copy_to_project_id, table_names
        )

        if not result['success']:
            sts = "Failed"
            sts_description = "Some or all tables failed to copy"

        total_records_copied = result['total_records_copied']
        failed_tables = result['failed_tables']
        results_by_table = result['results_by_table']

        # Add warning if it exists
        warning = validation_result.get('warning')
        if warning:
            sts_description += f". Note: {warning}"

    except mysql.connector.Error as error:
        sts = "Failed"
        if error.errno == 1062:  # Duplicate entry
            sts_description = "Duplicate entry detected during copy operation"
        elif error.errno == 1048:  # Column cannot be null
            sts_description = "Required field is missing in source data"
        elif error.errno == 1406:  # Data too long for column
            sts_description = "Data too long for target column"
        else:
            sts_description = f"Database error occurred during copy operation: {error}"
        logging.error(f"Copy operation failed: {error}")

    except Exception as error:
        sts = "Failed"
        sts_description = f"Copy operation failed: {str(error)}"
        logging.error(f"Copy operation failed: {error}")

    finally:
        if connection.is_connected():
            connection.close()

    return jsonify({
        'status': sts,
        'status_description': sts_description,
        'total_records_copied': total_records_copied,
        'failed_tables': failed_tables,
        'results_by_table': {k: {
            'records_copied': v.records_copied,
            'success': v.success,
            'error_message': v.error_message
        } for k, v in results_by_table.items()},
        'summary': {
            'corporate_account': corporate_account,
            'copy_from_project_id': copy_from_project_id,
            'copy_to_project_id': copy_to_project_id,
            'tables_processed': len(results_by_table),
            'tables_succeeded': len([r for r in results_by_table.values() if r.success]),
            'tables_failed': len(failed_tables)
        }
    })


@account_and_project_blueprint.route('/api/create_account', methods=['POST'])
@token_required
@validate_access
def create_account(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    account_description = data.get('account_description')
    requesting_for_corporate_account = data.get('requesting_for_corporate_account')

    # When Super Admin is logged in, they can see all users across other accounts
    if requesting_for_corporate_account:
        corporate_account = requesting_for_corporate_account

    if not corporate_account or not corporate_account.strip():
        return jsonify({
            'status': 'Failed',
            'status_description': 'Corporate account is required'
        })

    corporate_account = corporate_account.strip()

    # Check if corporate account already exists
    if validate_corporate_account(corporate_account):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Corporate account already exists'
        })

    if not account_description or not account_description.strip():
        return jsonify({
            'status': 'Failed',
            'status_description': 'Account description is required'
        })

    account_description = account_description.strip()

    # Validate corporate account format (alphanumeric, no spaces, reasonable length)
    if len(corporate_account) > 50 or len(corporate_account) == 0:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Corporate account cannot be empty and must be 50 characters or less'
        })

    if ' ' in corporate_account or not corporate_account.isalnum():
        return jsonify({
            'status': 'Failed',
            'status_description': 'Corporate account cannot contain spaces and must be alphanumeric'
        })

    # Validate account description length
    if len(account_description) > 300:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Account description must be 300 characters or less'
        })

    sts = "Success"
    sts_description = "Account created successfully"

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_insert_query = """INSERT INTO CORPORATE_ACCOUNTS (CORPORATE_ACCOUNT, ACCOUNT_DESCRIPTION, STATUS, CREATED_DATE, UPDATED_DATE)
                                VALUES (%s, %s, %s, %s, %s) """

        record = (corporate_account, account_description, 'Active', datetime.now(), datetime.now())
        cursor.execute(mySql_insert_query, record)
        connection.commit()

    except mysql.connector.Error as error:
        sts = "Failed"
        if error.errno == 1062:  # Duplicate entry
            sts_description = f"Corporate account already exists"
        elif error.errno == 1048:  # Column cannot be null
            sts_description = "Required field is missing. Please check all required fields are provided."
        elif error.errno == 1406:  # Data too long for column
            sts_description = "One or more fields exceed the maximum allowed length."
        else:
            sts_description = f"A database error has occurred. Please try again or contact support. {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'corporate_account': corporate_account,
        'status': sts,
        'status_description': sts_description
    })







@account_and_project_blueprint.route('/api/create_project', methods=['POST'])
@token_required
@validate_access
def create_project(current_user):
    data = request.json
    corporate_account= data.get('corporate_account')
    project_id = data.get('project_id')
    project_description = data.get('project_description')
    functional_domain = data.get('functional_domain')
    project_prefix = data.get('project_prefix')
    requesting_for_corporate_account = data.get('requesting_for_corporate_account')

    # When Super Admin is logged in, they can see all users across other accounts
    if requesting_for_corporate_account :
        corporate_account = requesting_for_corporate_account


    project_id = project_id.strip()
    if not validate_corporate_account(corporate_account):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Corporate account is not valid'
        })

    if not project_id.strip():
        return jsonify({
            'status': 'Failed',
            'status_description': 'Project Id is required'
        })

    if  validate_project_id(corporate_account, project_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Project Id already exists'
        })
    if not project_description.strip():
        return jsonify({
            'req_id': None,
            'status': 'Failed',
            'status_description': 'Project description is required'
        })

    if not functional_domain.strip():
        return jsonify({
            'status': 'Failed',
            'status_description': 'Functional domain is required'
        })

    # if not validate_functional_domain(functional_domain):
    #     return jsonify({
    #         'status': 'Failed',
    #         'status_description': 'Functional domain is not valid'
    #     })

    project_prefix = project_prefix.strip()
    if len(project_prefix) >= 6 or len(project_prefix) == 0:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Project prefix cannot be empty and must be less than 6 characters long'
        })
    if ' ' in project_prefix or not project_prefix.isalnum():
        return jsonify({
            'status': 'Failed',
            'status_description': 'Project prefix cannot contain spaces and must be alphanumeric'
        })
    if  not validate_project_prefix(corporate_account, project_id, project_prefix):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Ths prefix is in use by another project'
        })
    sts = "Success"
    sts_description = "Project added successfully"

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_insert_query = """INSERT INTO CORPORATE_ACCOUNT_PROJECTS (CORPORATE_ACCOUNT, PROJECT_ID, PROJECT_DESCRIPTION, FUNCTIONAL_DOMAIN, PROJECT_PREFIX, 
        STATUS, CREATED_DATE, UPDATED_DATE)
                                VALUES (%s, %s, %s, %s, %s, %s,  %s, %s) """

        record = (corporate_account, project_id, project_description, functional_domain, project_prefix, 'Active', datetime.now(), datetime.now())
        cursor.execute(mySql_insert_query, record)
        connection.commit()

    except mysql.connector.Error as error:
        sts = "Failed"
        if error.errno == 1062:  # Duplicate entry
            sts_description = f"Attempt to create a duplicate entry"
        elif error.errno == 1048:  # Column cannot be null
            sts_description = "Required field is missing. Please check all required fields are provided."
        elif error.errno == 1406:  # Data too long for column
            sts_description = "One or more fields exceed the maximum allowed length."
        else:
            sts_description = f"A database error has occurred. Please try again or contact support.{error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'project_id': project_id,
        'status': sts,
        'status_description': sts_description
    })


@account_and_project_blueprint.route('/api/update_project', methods=['PUT', 'POST'])
@token_required
@validate_access
def update_project(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    project_description = data.get('project_description')
    functional_domain = data.get('functional_domain')
    project_prefix = data.get('project_prefix')
    requesting_for_corporate_account = data.get('requesting_for_corporate_account')

    # When Super Admin is logged in, they can see all users across other accounts
    if requesting_for_corporate_account :
        corporate_account = requesting_for_corporate_account


    project_id = project_id.strip()
    if not validate_corporate_account(corporate_account):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Corporate account is not valid'
        })

    if not project_id.strip():
        return jsonify({
            'status': 'Failed',
            'status_description': 'Project Id is required'
        })

    # Check if project exists (opposite of create logic)
    if not validate_project_id(corporate_account, project_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Project Id does not exist'
        })

    if not project_description.strip():
        return jsonify({
            'status': 'Failed',
            'status_description': 'Project description is required'
        })

    if not functional_domain.strip():
        return jsonify({
            'status': 'Failed',
            'status_description': 'Functional domain is required'
        })

    if not validate_functional_domain(corporate_account, functional_domain):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Functional domain is not valid'
        })

    project_prefix = project_prefix.strip()
    if len(project_prefix) >= 6 or len(project_prefix) == 0:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Project prefix cannot be empty and must be less than 6 characters long'
        })

    if ' ' in project_prefix or not project_prefix.isalnum():
        return jsonify({
            'status': 'Failed',
            'status_description': 'Project prefix cannot contain spaces and must be alphanumeric'
        })


    if  not validate_project_prefix(corporate_account, project_id, project_prefix):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Ths prefix is in use by another project'
        })


    sts = "Success"
    sts_description = "Project updated successfully"

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_update_query = """UPDATE CORPORATE_ACCOUNT_PROJECTS 
                              SET PROJECT_DESCRIPTION = %s, 
                                  FUNCTIONAL_DOMAIN = %s, 
                                  PROJECT_PREFIX = %s,
                                  UPDATED_DATE = %s
                              WHERE CORPORATE_ACCOUNT = %s 
                                AND PROJECT_ID = %s"""

        record = (project_description, functional_domain, project_prefix,
                  datetime.now(), corporate_account, project_id)
        cursor.execute(mySql_update_query, record)
        connection.commit()

    except mysql.connector.Error as error:
        sts = "Failed"
        if error.errno == 1062:  # Duplicate entry
            sts_description = f"Attempt to create a duplicate entry"
        elif error.errno == 1048:  # Column cannot be null
            sts_description = "Required field is missing. Please check all required fields are provided."
        elif error.errno == 1406:  # Data too long for column
            sts_description = "One or more fields exceed the maximum allowed length."
        else:
            sts_description = f"A database error has occurred. Please try again or contact support.{error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'project_id': project_id,
        'status': sts,
        'status_description': sts_description
    })



@account_and_project_blueprint.route('/api/get_project_list', methods=['GET','POST'])
@token_required
def get_project_list(current_user):
    data = request.json
    user_id = data.get('user_id')
    corporate_account = data.get('corporate_account')

    sts = "Success"
    sts_description = "Project list retrieved successfully"
    project_details = {}
    project_list = []

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        if corporate_account:
            mySql_select_query = """SELECT A.CORPORATE_ACCOUNT, A.ACCOUNT_DESCRIPTION, B.PROJECT_ID, B.PROJECT_DESCRIPTION, B.FUNCTIONAL_DOMAIN, B.PROJECT_PREFIX, 
            B.STATUS, B.CREATED_DATE, B.UPDATED_DATE FROM CORPORATE_ACCOUNTS A, CORPORATE_ACCOUNT_PROJECTS B 
            WHERE A.CORPORATE_ACCOUNT = B.CORPORATE_ACCOUNT AND A.STATUS = %s AND B.STATUS = %s AND A.CORPORATE_ACCOUNT = %s"""
            record = ('Active', 'Active', corporate_account)
        else:

            mySql_select_query = """SELECT A.CORPORATE_ACCOUNT, A.ACCOUNT_DESCRIPTION, B.PROJECT_ID, B.PROJECT_DESCRIPTION, B.FUNCTIONAL_DOMAIN, B.PROJECT_PREFIX, 
            B.STATUS, B.CREATED_DATE, B.UPDATED_DATE FROM CORPORATE_ACCOUNTS A, CORPORATE_ACCOUNT_PROJECTS B, USER_ACCOUNTS C 
            WHERE A.CORPORATE_ACCOUNT = B.CORPORATE_ACCOUNT AND A.CORPORATE_ACCOUNT = C.CORPORATE_ACCOUNT AND A.STATUS = %s AND B.STATUS = %s AND C.USER_ID = %s"""
            record = ('Active', 'Active', user_id)


        cursor.execute(mySql_select_query, record)
        for result in cursor.fetchall():


            project_details = {
                'corporate_account': result[0],
                'account_description': result[1],
                'project_id': result[2],
                'project_description': result[3],
                'functional_domain': result[4],
                'project_prefix': result[5],
                'status': result[6],
                'created_date': result[7],
                'updated_date': result[8]
            }
            project_list.append(project_details)


        if len(project_list) == 0:
            sts = "Failed"
            sts_description = "No matching accounts found"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the project list: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'project_list': project_list,
        'status': sts,
        'status_description': sts_description
    })


@account_and_project_blueprint.route('/api/get_account_list', methods=['GET','POST'])
@token_required
def get_account_list(current_user):
    data = request.json
    user_id = data.get('user_id')

    sts = "Success"
    sts_description = "Account list retrieved successfully"
    account_details = {}
    account_list = []

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_select_query = """SELECT CORPORATE_ACCOUNT, ACCOUNT_DESCRIPTION, STATUS, CREATED_DATE, UPDATED_DATE FROM CORPORATE_ACCOUNTS 
        WHERE STATUS = %s """
        record = ['Active']
        if user_id:
            mySql_select_query += ' AND CORPORATE_ACCOUNT IN (SELECT CORPORATE_ACCOUNT FROM USER_ACCOUNTS WHERE USER_ID = %s AND STATUS = %s)'
            record.append(user_id)
            record.append('Active')
        mySql_select_query += ' ORDER BY CORPORATE_ACCOUNT'
        record = tuple(record)  # Convert list to tuple for parameterized query
        logging.info(f"SQL : {mySql_select_query}")



        logging.info(f"SQL : {user_id}")

        cursor.execute(mySql_select_query, record)
        for result in cursor.fetchall():


            account_details = {
                'corporate_account': result[0],
                'account_description': result[1],
                'status': result[2],
                'created_date': result[3],
                'updated_date': result[4]
            }
            account_list.append(account_details)


        if len(account_list) == 0:
            sts = "Failed"
            sts_description = "No matching accounts found"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the account details: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'account_list': account_list,
        'status': sts,
        'status_description': sts_description
    })



@account_and_project_blueprint.route('/api/get_user_list', methods=['GET','POST'])
@token_required
@validate_access
def get_user_list(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    # user_status = data.get('user_status')
    requesting_for_corporate_account = data.get('requesting_for_corporate_account')



    logging.info(f"corporate_account: {corporate_account}")
    logging.info(f"requesting for corporate_account: {requesting_for_corporate_account}")


    # When Super Admin is logged in, they can see all users across other accounts
    if requesting_for_corporate_account :
        corporate_account = requesting_for_corporate_account

    sts = "Success"
    sts_description = "User list retrieved successfully"
    user_details = {}
    user_list = []

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        if corporate_account != 'ALL':
            mySql_select_query = """SELECT USER_ID, USER_NAME, A.STATUS, A.CORPORATE_ACCOUNT, ACCOUNT_DESCRIPTION, ACCESS_LEVEL, DEFAULT_PROJECT, A.CREATED_DATE, A.UPDATED_DATE, BT.BUSINESS_TEAM_DESCRIPTION, A.USER_ROLE, BT.BUSINESS_TEAM_ID
            FROM USER_ACCOUNTS A LEFT JOIN BUSINESS_TEAMS BT ON A.CORPORATE_ACCOUNT = BT.CORPORATE_ACCOUNT AND A.DEFAULT_PROJECT = BT.PROJECT_ID AND A.BUSINESS_TEAM_ID = BT.BUSINESS_TEAM_ID
            , CORPORATE_ACCOUNTS B
            WHERE A.CORPORATE_ACCOUNT = B.CORPORATE_ACCOUNT AND A.CORPORATE_ACCOUNT = %s ORDER BY USER_NAME """
            record = [corporate_account, ]
            cursor.execute(mySql_select_query, record)

        else:
                mySql_select_query = """SELECT USER_ID, USER_NAME, A.STATUS, A.CORPORATE_ACCOUNT, ACCOUNT_DESCRIPTION, ACCESS_LEVEL, DEFAULT_PROJECT, A.CREATED_DATE, A.UPDATED_DATE , BT.BUSINESS_TEAM_DESCRIPTION, A.USER_ROLE, BT.BUSINESS_TEAM_ID
                FROM USER_ACCOUNTS A LEFT JOIN BUSINESS_TEAMS BT ON A.CORPORATE_ACCOUNT = BT.CORPORATE_ACCOUNT AND A.DEFAULT_PROJECT = BT.PROJECT_ID AND A.BUSINESS_TEAM_ID = BT.BUSINESS_TEAM_ID
                , CORPORATE_ACCOUNTS B
                WHERE A.CORPORATE_ACCOUNT = B.CORPORATE_ACCOUNT ORDER BY USER_NAME """
                cursor.execute(mySql_select_query)


        for result in cursor.fetchall():
            user_details = {
                'user_id': result[0],
                'user_name': result[1],
                'status': result[2],
                'corporate_account': result[3],
                'account_description': result[4],
                'access_level': result[5],
                'default_project': result[6],
                'created_date': result[7],
                'updated_date': result[8],
                'business_team_description': result[9],
                'user_role': result[10],
                'business_team_id': result[11]
            }
            user_list.append(user_details)


        if len(user_list) == 0:
            sts = "Failed"
            sts_description = "No matching users found"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the user list: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'user_list': user_list,
        'status': sts,
        'status_description': sts_description
    })






@account_and_project_blueprint.route('/api/update_user_project_access', methods=['PUT', 'POST'])
@token_required
@validate_access
def update_user_project_access(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    user_ids = data.get('user_ids', [])
    project_id = data.get('project_id')
    access_level = data.get('access_level')
    business_team_id = data.get('business_team_id')
    user_role = data.get('user_role')
    requesting_for_corporate_account = data.get('requesting_for_corporate_account')
    # When Super Admin is logged in, they can see all users across other accounts
    if requesting_for_corporate_account:
        corporate_account = requesting_for_corporate_account


    if not validate_corporate_account(corporate_account):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Corporate account is not valid'
        })

    if not user_ids or len(user_ids) == 0:
        return jsonify({
            'status': 'Failed',
            'status_description': 'At least one user ID is required'
        })


    if not project_id or not project_id.strip():
        return jsonify({
            'status': 'Failed',
            'status_description': 'Project ID is required'
        })

    if not validate_project_id(corporate_account, project_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Invalid project ID'
        })

    for user_id in user_ids:
        if not validate_user_id(corporate_account, user_id):
            return jsonify({
                'status': 'Failed',
                'status_description': f'Invalid user ID: {user_id}'
            })

    if not isinstance(access_level, int) or not (1 <= access_level <= 9):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Access level must be a numeric value between 1 and 9'
        })

    sts = "Success"
    sts_description = "Project access updated successfully"

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        for user_id in user_ids:
            mySql_update_query = f"""UPDATE USER_PROJECTS 
                                  SET ACCESS_LEVEL = %s, 
                                      UPDATED_DATE = %s,
                                      BUSINESS_TEAM_ID = %s,
                                      USER_ROLE = %s
                                  WHERE CORPORATE_ACCOUNT = %s 
                                    AND PROJECT_ID = %s     
                                    AND USER_ID = %s"""
            record = (access_level, datetime.now(), business_team_id, user_role, corporate_account, project_id, user_id)
            cursor.execute(mySql_update_query, record)
            rows_impacted = cursor.rowcount
            if rows_impacted == 0:
                mySql_insert_query = """INSERT INTO USER_PROJECTS (CORPORATE_ACCOUNT, USER_ID, PROJECT_ID, ACCESS_LEVEL, CREATED_DATE, UPDATED_DATE, BUSINESS_TEAM_ID, USER_ROLE)
                                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
                record = (corporate_account, user_id, project_id, access_level, datetime.now(), datetime.now(), business_team_id, user_role)
                cursor.execute(mySql_insert_query, record)


        connection.commit()

    except mysql.connector.Error as error:
        sts = "Failed"
        if error.errno == 1062:  # Duplicate entry
            sts_description = f"Attempt to create a duplicate entry"
        elif error.errno == 1048:  # Column cannot be null
            sts_description = "Required field is missing. Please check all required fields are provided."
        elif error.errno == 1406:  # Data too long for column
            sts_description = "One or more fields exceed the maximum allowed length."
        else:
            sts_description = f"A database error has occurred. Please try again or contact support.{error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'status': sts,
        'status_description': sts_description
    })


@account_and_project_blueprint.route('/api/delete_user_project_access', methods=['DELETE', 'POST'])
@token_required
@validate_access
def delete_user_project_access(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    requesting_for_corporate_account = data.get('requesting_for_corporate_account')
    user_project_records = data.get('user_project_records', [])

    if requesting_for_corporate_account:
        corporate_account = requesting_for_corporate_account


    logging.info(f"Deleting project access - corporate_account: {corporate_account}")
    logging.info(f"Records to delete: {user_project_records}")

    if not validate_corporate_account(corporate_account):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Corporate account is not valid'
        })

    if not user_project_records or len(user_project_records) == 0:
        return jsonify({
            'status': 'Failed',
            'status_description': 'At least one user-project record is required'
        })

    sts = "Success"
    sts_description = "Project access removed successfully. User falls back to default access level if any."
    success_count = 0
    error_count = 0

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        for record in user_project_records:
            user_id = record.get('user_id', '').strip()
            project_id = record.get('project_id', '').strip()

            if not user_id or not project_id:
                error_count += 1
                continue

            try:
                mySql_delete_query = """DELETE FROM USER_PROJECTS 
                                      WHERE CORPORATE_ACCOUNT = %s 
                                        AND USER_ID = %s 
                                        AND PROJECT_ID = %s"""
                delete_record = (corporate_account, user_id, project_id)
                cursor.execute(mySql_delete_query, delete_record)

                if cursor.rowcount > 0:
                    success_count += 1
                else:
                    error_count += 1

            except mysql.connector.Error as user_error:
                error_count += 1
                logging.error(f"Error removing project access for user {user_id}, project {project_id}: {user_error}")

        connection.commit()

        # Prepare status message
        if success_count > 0:
            sts_description = f"Successfully removed project access for {success_count} record(s)"
            if error_count > 0:
                sts_description += f", {error_count} record(s) failed"
        else:
            sts = "Failed"
            sts_description = "Failed to remove project access for any records"

    except mysql.connector.Error as error:
        sts = "Failed"
        if error.errno == 1062:  # Duplicate entry
            sts_description = f"Attempt to create a duplicate entry"
        elif error.errno == 1048:  # Column cannot be null
            sts_description = "Required field is missing. Please check all required fields are provided."
        elif error.errno == 1406:  # Data too long for column
            sts_description = "One or more fields exceed the maximum allowed length."
        else:
            sts_description = f"A database error has occurred. Please try again or contact support.{error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'status': sts,
        'status_description': sts_description,
        'success_count': success_count,
        'error_count': error_count
    })


@account_and_project_blueprint.route('/api/get_user_project_list', methods=['GET', 'POST'])
@token_required
@validate_access
def get_user_project_list(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    user_ids = data.get('user_ids', [])
    project_id = data.get('project_id', None)
    # When Super Admin is logged in, they can see all users across other accounts
    requesting_for_corporate_account = data.get('requesting_for_corporate_account')
    if requesting_for_corporate_account:
        corporate_account = requesting_for_corporate_account


    logging.info(f"Getting user project list - corporate_account: {corporate_account}")
    logging.info(f"User IDs filter: {user_ids}, Project ID filter: {project_id}")

    if not validate_corporate_account(corporate_account):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Corporate account is not valid'
        })

    sts = "Success"
    sts_description = "User project list retrieved successfully"
    user_project_list = []

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        # Build dynamic query based on filters
        base_query = """SELECT UP.USER_ID, UA.USER_NAME, UP.PROJECT_ID, P.PROJECT_DESCRIPTION, 
                              UP.ACCESS_LEVEL, UP.CREATED_DATE, UP.UPDATED_DATE, BT.BUSINESS_TEAM_DESCRIPTION , UP.USER_ROLE, BT.BUSINESS_TEAM_ID
                       FROM USER_PROJECTS UP
                       LEFT JOIN USER_ACCOUNTS UA ON UP.CORPORATE_ACCOUNT = UA.CORPORATE_ACCOUNT 
                                                   AND UP.USER_ID = UA.USER_ID
                       LEFT JOIN CORPORATE_ACCOUNT_PROJECTS P ON UP.CORPORATE_ACCOUNT = P.CORPORATE_ACCOUNT 
                                              AND UP.PROJECT_ID = P.PROJECT_ID
                      LEFT JOIN BUSINESS_TEAMS BT ON BT.CORPORATE_ACCOUNT = UP.CORPORATE_ACCOUNT AND BT.PROJECT_ID = UP.PROJECT_ID AND BT.BUSINESS_TEAM_ID = UP.BUSINESS_TEAM_ID
                       WHERE UP.CORPORATE_ACCOUNT = %s"""

        params = [corporate_account]

        # Add user_ids filter if provided
        if user_ids and len(user_ids) > 0:
            placeholders = ','.join(['%s'] * len(user_ids))
            base_query += f" AND UP.USER_ID IN ({placeholders})"
            params.extend(user_ids)

        # Add project_id filter if provided
        if project_id and project_id.strip():
            base_query += " AND UP.PROJECT_ID = %s"
            params.append(project_id.strip())

        base_query += " ORDER BY UA.USER_NAME, P.PROJECT_ID"

        logging.info(f"SQL Query: {base_query}")
        logging.info(f"Parameters: {params}")

        cursor.execute(base_query, params)

        for result in cursor.fetchall():
            user_project_details = {
                'user_id': result[0],
                'user_name': result[1],
                'project_id': result[2],
                'project_name': result[3],
                'access_level': result[4],
                'created_date': result[5],
                'updated_date': result[6],
                'business_team_description' : result[7],
                'user_role': result[8],
                'business_team_id': result[9]
            }
            user_project_list.append(user_project_details)

        if len(user_project_list) == 0:
            sts = "Failed"
            sts_description = "No user projects found matching the criteria"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the user project list: {error}"
        logging.error(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'user_project_list': user_project_list,
        'status': sts,
        'status_description': sts_description
    })

















@account_and_project_blueprint.route('/api/create_functional_domain', methods=['POST'])
@token_required
@validate_access
def create_functional_domain(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    functional_domain = data.get('functional_domain')
    requesting_for_corporate_account = data.get('requesting_for_corporate_account')

    # When Super Admin is logged in, they can see all users across other accounts
    if requesting_for_corporate_account :
        corporate_account = requesting_for_corporate_account

    functional_domain = functional_domain.strip()

    if not validate_corporate_account(corporate_account):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Corporate account is not valid'
        })

    if not functional_domain:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Functional domain is required'
        })

    sts = "Success"
    sts_description = "Functional domain added successfully"


    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_insert_query = """INSERT INTO FUNCTIONAL_DOMAINS (CORPORATE_ACCOUNT, FUNCTIONAL_DOMAIN, CREATED_DATE, UPDATED_DATE)
                                VALUES (%s, %s, %s, %s) """
        record = (corporate_account, functional_domain, datetime.now(), datetime.now())
        cursor.execute(mySql_insert_query, record)
        connection.commit()

    except mysql.connector.Error as error:
        sts = "Failed"
        if error.errno == 1062:  # Duplicate entry
            sts_description = f"Attempt to create a duplicate entry"
        elif error.errno == 1048:  # Column cannot be null
            sts_description = "Required field is missing. Please check all required fields are provided."
        elif error.errno == 1406:  # Data too long for column
            sts_description = "One or more fields exceed the maximum allowed length."
        else:
            sts_description = f"A database error has occurred. Please try again or contact support.{error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'status': sts,
        'status_description': sts_description
    })



@account_and_project_blueprint.route('/api/update_functional_domain', methods=['POST'])
@token_required
@validate_access
def update_functional_domain(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    functional_domain_prev = data.get('functional_domain_prev')
    functional_domain = data.get('functional_domain')


    functional_domain = functional_domain.strip()

    if not validate_corporate_account(corporate_account):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Corporate account is not valid'
        })

    if not functional_domain_prev:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Previous functional domain is required'
        })

    if not functional_domain:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Functional domain is required'
        })

    sts = "Success"
    sts_description = "Functional domain updated successfully"


    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_update_query = """UPDATE FUNCTIONAL_DOMAINS 
                                SET FUNCTIONAL_DOMAIN = %s, UPDATED_DATE = %s
                                WHERE CORPORATE_ACCOUNT = %s AND FUNCTIONAL_DOMAIN = %s"""
        record = (functional_domain, datetime.now(), corporate_account, functional_domain_prev)
        cursor.execute(mySql_update_query, record)
        connection.commit()

    except mysql.connector.Error as error:
        sts = "Failed"
        if error.errno == 1062:  # Duplicate entry
            sts_description = f"Attempt to create a duplicate entry"
        elif error.errno == 1048:  # Column cannot be null
            sts_description = "Required field is missing. Please check all required fields are provided."
        elif error.errno == 1406:  # Data too long for column
            sts_description = "One or more fields exceed the maximum allowed length."
        else:
            sts_description = f"A database error has occurred. Please try again or contact support.{error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'status': sts,
        'status_description': sts_description
    })


@account_and_project_blueprint.route('/api/delete_functional_domain', methods=['POST'])
@token_required
@validate_access
def delete_functional_domain(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    functional_domain = data.get('functional_domain')


    functional_domain = functional_domain.strip()

    if not validate_corporate_account(corporate_account):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Corporate account is not valid'
        })


    if not functional_domain:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Functional domain is required'
        })

    sts = "Success"
    sts_description = "Functional domain deleted successfully"


    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_delete_query = """DELETE FROM FUNCTIONAL_DOMAINS 
                                WHERE CORPORATE_ACCOUNT = %s AND FUNCTIONAL_DOMAIN = %s"""
        record = (corporate_account, functional_domain)
        cursor.execute(mySql_delete_query, record)
        rows_impacted = cursor.rowcount
        if rows_impacted == 0:
            sts = "Failed"
            sts_description = "Functional domain does not exist or has already been deleted"

        connection.commit()

    except mysql.connector.Error as error:
        sts = "Failed"
        if error.errno == 1062:  # Duplicate entry
            sts_description = f"Attempt to create a duplicate entry"
        elif error.errno == 1048:  # Column cannot be null
            sts_description = "Required field is missing. Please check all required fields are provided."
        elif error.errno == 1406:  # Data too long for column
            sts_description = "One or more fields exceed the maximum allowed length."
        else:
            sts_description = f"A database error has occurred. Please try again or contact support.{error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'status': sts,
        'status_description': sts_description
    })



@account_and_project_blueprint.route('/api/get_functional_domains', methods=['GET','POST'])
@token_required
def get_functional_domains(current_user):

    data = request.json
    corporate_account = data.get('corporate_account')


    sts = "Success"
    sts_description = "Functional domains retrieved successfully"
    functional_domain_list = []

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_select_query = """SELECT FUNCTIONAL_DOMAIN FROM FUNCTIONAL_DOMAINS WHERE CORPORATE_ACCOUNT = %s ORDER BY FUNCTIONAL_DOMAIN"""
        record = (corporate_account, )
        cursor.execute(mySql_select_query, record)


        for result in cursor.fetchall():


            functional_domain_details = {
                'functional_domain': result[0]
            }
            functional_domain_list.append(functional_domain_details)


        if len(functional_domain_list) == 0:
            sts = "Failed"
            sts_description = "No matching functional domains found"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the functional domain details: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'functional_domain_list': functional_domain_list,
        'status': sts,
        'status_description': sts_description
    })




@account_and_project_blueprint.route('/api/validate_user_credentials', methods=['POST'])
def validate_user_credentials():
    data = request.json
    user_id = data.get('user_id')
    password = data.get('password')

    user_id = user_id.strip()

    logging.info(f"Validating user credentials for user_id: {user_id}")
    logging.info(f"Password provided: {'*' * len(password) if password else 'None'}")


    if not user_id:
        return jsonify({
            'status': 'Failed',
            'status_description': 'User Id is required'
        })
    if not password:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Password is required'
        })

    sts = "Success"
    sts_description = "Login success"
    user_name = ''
    corporate_account = ''
    user_since = ''
    login_token = ''
    default_project = ''
    last_used_project = ''



    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_insert_query = """SELECT USER_NAME, CORPORATE_ACCOUNT, PASSWORD_HASH, CREATED_DATE, DEFAULT_PROJECT, LAST_USED_PROJECT
         FROM USER_ACCOUNTS WHERE USER_ID = %s AND STATUS = %s"""

        record = (user_id,'Active')
        cursor.execute(mySql_insert_query, record)
        logging.info(f"User login - executed SQL is: {cursor._executed}")

        result = cursor.fetchone()
        if result is None:
            sts = "Failed"
            sts_description = "Login failed"
        else:
            if not verify_password(result[2], password):
                sts = "Failed"
                sts_description = "Login failed"
            else:
                logging.info(f"Login successful for user_id: {user_id}")
                user_name = result[0]
                corporate_account = result[1]
                user_since = result[3]
                default_project = result[4]
                last_used_project = result[5]
                expiration = datetime.utcnow() + timedelta(days=TOKEN_EXPIRY_DAYS)
                logging.info(f"secret key: {SECRET_KEY}")
                login_token = jwt.encode({'user_id': user_id, 'exp': expiration, 'timestamp': datetime.utcnow().isoformat()}, SECRET_KEY,
                                     algorithm="HS256")
                logging.info(f"Generated login token for user_id: {login_token}")
    except mysql.connector.Error as error:
        logging.info(f"sts: {sts}")


    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to login: {error}"
        logging.info(error)

    finally:
        logging.info(f"sts: {sts}")

    return jsonify({
        'user_id': user_id,
        'user_name': user_name,
        'corporate_account': corporate_account,
        'user_since': user_since,
        'login_token': login_token,
        'default_project': default_project,
        'last_used_project': last_used_project,
        'status': sts,
        'status_description': sts_description
    }) ,401 if sts == "Failed" else 200

@app.route('/api/protected', methods=['GET'])
@token_required
def protected_route(current_user):
    return jsonify({'message': f'Hello, {current_user}!'})


@account_and_project_blueprint.route('/api/create_user', methods=['POST'])
@token_required
@validate_access
def create_user(current_user):
    data = request.json
    corporate_account= data.get('corporate_account')
    user_id = data.get('user_id')
    user_name = data.get('user_name')
    password = data.get('password')
    access_level = data.get('access_level')
    default_project = data.get('default_project', None)
    requesting_for_corporate_account = data.get('requesting_for_corporate_account')
    business_team_id = data.get('business_team_id', None)
    user_role = data.get('user_role', None)



    logging.info(f"corporate_account: {corporate_account}")
    logging.info(f"requesting for corporate_account: {requesting_for_corporate_account}")


    # When Super Admin is logged in, they can see all users across other accounts
    if requesting_for_corporate_account:
        corporate_account = requesting_for_corporate_account








    user_id = user_id.strip()

    if not access_level:
        access_level = 1
    if not str(access_level).isdigit():
        access_level = 1



    if not validate_corporate_account(corporate_account):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Corporate account is not valid'
        })

    if not user_id.strip():
        return jsonify({
            'status': 'Failed',
            'status_description': 'User Id is required'
        })

    if not re.match(r"[^@]+@[^@]+\.[^@]+", user_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'User Id must be a valid email address'
        })

    if not user_name.strip():
        return jsonify({
            'status': 'Failed',
            'status_description': 'User name is required'
        })

    if not password.strip():
        return jsonify({
            'status': 'Failed',
            'status_description': 'Password is required'
        })


    logging.info(f"access_level: {access_level}")

    if not isinstance(access_level, int) or not (1 <= access_level <= 9):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Access level must be a numeric value between 1 and 9'
        })


    if  default_project and not validate_project_id(corporate_account, default_project):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Invalid default project'
        })

    sts = "Success"
    sts_description = "User added successfully"
    password_hash =  hash_password(password)


    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_insert_query = """INSERT INTO USER_ACCOUNTS (CORPORATE_ACCOUNT, USER_ID, USER_NAME, PASSWORD_HASH, 
        PASSWORD_RESET_TOKEN, PASSWORD_RESET_EXPIRES, LAST_PASSWORD_CHANGE, STATUS, ACCESS_LEVEL, CREATED_DATE, UPDATED_DATE, DEFAULT_PROJECT,
        BUSINESS_TEAM_ID, USER_ROLE)
                                VALUES (%s, %s, %s, %s, NULL, NULL, %s, %s, %s, %s, %s, %s, %s, %s) """
        record = (corporate_account, user_id, user_name,  password_hash,datetime.now(),  'Active', access_level, datetime.now(), datetime.now(), default_project, business_team_id, user_role)
        cursor.execute(mySql_insert_query, record)


        connection.commit()

    except mysql.connector.Error as error:
        sts = "Failed"
        if error.errno == 1062:  # Duplicate entry
            sts_description = f"Attempt to create a duplicate entry"
        elif error.errno == 1048:  # Column cannot be null
            sts_description = "Required field is missing. Please check all required fields are provided."
        elif error.errno == 1406:  # Data too long for column
            sts_description = "One or more fields exceed the maximum allowed length."
        else:
            sts_description = f"A database error has occurred. Please try again or contact support.{error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'user_id': user_id,
        'status': sts,
        'status_description': sts_description
    })


@account_and_project_blueprint.route('/api/update_user', methods=['PUT', 'POST'])
@token_required
@validate_access
def update_user(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    user_id = data.get('user_id')
    user_name = data.get('user_name')
    password = data.get('password')
    access_level = data.get('access_level')
    default_project = data.get('default_project', None)
    requesting_for_corporate_account = data.get('requesting_for_corporate_account')
    user_status = data.get('user_status')
    business_team_id = data.get('business_team_id', None)
    user_role = data.get('user_role', None)


    logging.info(f"corporate_account: {corporate_account}")
    logging.info(f"requesting for corporate_account: {requesting_for_corporate_account}")


    # When Super Admin is logged in, they can see all users across other accounts
    if requesting_for_corporate_account:
        corporate_account = requesting_for_corporate_account





    logging.info("input to update_user: %s", data)

    user_id = user_id.strip()

    if not access_level:
        access_level = 1
    if not str(access_level).isdigit():
        access_level = 1

    if not validate_corporate_account(corporate_account):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Corporate account is not valid'
        })

    if not user_id:
        return jsonify({
            'status': 'Failed',
            'status_description': 'User Id is required'
        })

    # # Check if user exists (opposite of create logic)
    # if not validate_user_id(corporate_account, user_id):
    #     return jsonify({
    #         'status': 'Failed',
    #         'status_description': 'User Id does not exist'
    #     })

    if not re.match(r"[^@]+@[^@]+\.[^@]+", user_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'User Id must be a valid email address'
        })

    if not user_name.strip():
        return jsonify({
            'status': 'Failed',
            'status_description': 'User name is required'
        })



    logging.info(f"access_level: {access_level}")

    if not isinstance(access_level, int) or not (1 <= access_level <= 9):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Access level must be a numeric value between 1 and 9'
        })

    if  default_project and not validate_project_id(corporate_account, default_project):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Invalid default project'
        })


    if not validate_status(corporate_account, 'ALL PROJECTS', 'USER_STATUS', user_status):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Invalid user status'
        })


    sts = "Success"
    sts_description = "User updated successfully"

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        if password:
            password_hash = hash_password(password)
            mySql_update_query = """UPDATE USER_ACCOUNTS 
                                  SET USER_NAME = %s, 
                                      ACCESS_LEVEL = %s, 
                                      UPDATED_DATE = %s,
                                        DEFAULT_PROJECT = %s,
                                        PASSWORD_HASH = %s, 
                                        PASSWORD_RESET_TOKEN = NULL, 
                                        PASSWORD_RESET_EXPIRES = NULL, 
                                        LAST_PASSWORD_CHANGE = %s,
                                        STATUS = %s,
                                        BUSINESS_TEAM_ID = %s,
                                        USER_ROLE = %s
                                  WHERE CORPORATE_ACCOUNT = %s 
                                    AND USER_ID = %s"""
            record = (user_name, access_level, datetime.now(),default_project, password_hash, datetime.now(), user_status, business_team_id, user_role, corporate_account, user_id)
        else:
            mySql_update_query = """UPDATE USER_ACCOUNTS 
                                  SET USER_NAME = %s, 
                                      ACCESS_LEVEL = %s, 
                                      UPDATED_DATE = %s,
                                        DEFAULT_PROJECT = %s,
                                        STATUS = %s,
                                        BUSINESS_TEAM_ID = %s,
                                        USER_ROLE = %s
                                  WHERE CORPORATE_ACCOUNT = %s 
                                    AND USER_ID = %s"""
            record = (user_name, access_level, datetime.now(), default_project, user_status, business_team_id, user_role, corporate_account, user_id)

        cursor.execute(mySql_update_query, record)
        rows_impacted = cursor.rowcount
        if rows_impacted == 0:
            sts = "Failed"
            sts_description = "User not updated. Please verify the user ID exists"

        logging.info(f" executed SQL-1 is: {cursor._executed}")
        connection.commit()

    except mysql.connector.Error as error:
        sts = "Failed"
        if error.errno == 1062:  # Duplicate entry
            sts_description = f"Attempt to create a duplicate entry"
        elif error.errno == 1048:  # Column cannot be null
            sts_description = "Required field is missing. Please check all required fields are provided."
        elif error.errno == 1406:  # Data too long for column
            sts_description = "One or more fields exceed the maximum allowed length."
        else:
            sts_description = f"A database error has occurred. Please try again or contact support.{error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'user_id': user_id,
        'status': sts,
        'status_description': sts_description
    })


@account_and_project_blueprint.route('/api/update_user_access_level', methods=['PUT', 'POST'])
@token_required
@validate_access
def update_user_access_level(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    user_ids = data.get('user_ids', [])
    access_level = data.get('access_level')
    requesting_for_corporate_account = data.get('requesting_for_corporate_account')

    logging.info(f"corporate_account: {corporate_account}")
    logging.info(f"requesting for corporate_account: {requesting_for_corporate_account}")
    placeholders = ''

    # When Super Admin is logged in, they can see all users across other accounts
    if requesting_for_corporate_account:
        corporate_account = requesting_for_corporate_account

    logging.info("input to update_user_access_level: %s", data)

    if not validate_corporate_account(corporate_account):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Corporate account is not valid'
        })

    if not user_ids or not isinstance(user_ids, list):
        return jsonify({
            'status': 'Failed',
            'status_description': 'User Ids must be provided as an array'
        })

    # Validate access level
    if access_level is None:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Access level is required'
        })

    # Convert access_level to int and validate range
    try:
        access_level = int(access_level)
        if not (1 <= access_level <= 9):
            return jsonify({
                'status': 'Failed',
                'status_description': 'Access level must be a numeric value between 1 and 9'
            })
    except (ValueError, TypeError):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Access level must be a numeric value between 1 and 9'
        })

    # Check if each user exists
    for user_id in user_ids:
        if not validate_user_id(corporate_account, user_id):
            return jsonify({
                'status': 'Failed',
                'status_description': f'User Id {user_id} does not exist or Inactive'
            })

    sts = "Success"
    sts_description = "Access level updated successfully"
    placeholders = ','.join(['%s'] * len(user_ids))

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        mySql_update_query = f"""UPDATE USER_ACCOUNTS 
                              SET ACCESS_LEVEL = %s,
                                  UPDATED_DATE = %s
                              WHERE CORPORATE_ACCOUNT = %s 
                                AND USER_ID IN({placeholders})"""

        record = [access_level, datetime.now(), corporate_account]
        record.extend(user_ids)

        cursor.execute(mySql_update_query, record)
        connection.commit()

        # Log the number of affected rows
        affected_rows = cursor.rowcount
        logging.info(f"Updated access level for {affected_rows} users")

        if affected_rows == 0:
            sts = "Failed"
            sts_description = "No users were updated. Please verify the user IDs exist."

    except mysql.connector.Error as error:
        sts = "Failed"
        if error.errno == 1062:  # Duplicate entry
            sts_description = f"Attempt to create a duplicate entry"
        elif error.errno == 1048:  # Column cannot be null
            sts_description = "Required field is missing. Please check all required fields are provided."
        elif error.errno == 1406:  # Data too long for column
            sts_description = "One or more fields exceed the maximum allowed length."
        else:
            sts_description = f"A database error has occurred. Please try again or contact support. {error}"
        logging.error(f"Database error in update_user_access_level: {error}")

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'status': sts,
        'status_description': sts_description,
        'affected_users': len(user_ids) if sts == "Success" else 0
    })

@account_and_project_blueprint.route('/api/update_user_password', methods=['PUT', 'POST'])
@token_required
@validate_access
def update_user_password(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    user_ids = data.get('user_ids',[])
    password = data.get('password')
    requesting_for_corporate_account = data.get('requesting_for_corporate_account')


    logging.info(f"corporate_account: {corporate_account}")
    logging.info(f"requesting for corporate_account: {requesting_for_corporate_account}")
    placeholders = ''


    # When Super Admin is logged in, they can see all users across other accounts
    if requesting_for_corporate_account:
        corporate_account = requesting_for_corporate_account


    logging.info("input to update_user_password: %s", data)


    if not validate_corporate_account(corporate_account):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Corporate account is not valid'
        })

    logging.info("check 1")

    if not user_ids or not isinstance(user_ids, list):
        return jsonify({
            'status': 'Failed',
            'status_description': 'User Ids must be provided as an array'
        })

    logging.info("check 2")

    # Check if each user exists (opposite of create logic)
    for user_id in user_ids:
        if not validate_user_id(corporate_account, user_id):
            return jsonify({
                'status': 'Failed',
                'status_description': f'User Id {user_id} does not exist or Inactive'
            })

    logging.info("check 3")

    sts = "Success"
    sts_description = "Password updated successfully"
    placeholders = ','.join(['%s'] * len(user_ids))

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        logging.info("check 4")

        if password:
            logging.info("check 5")

            password_hash = hash_password(password)
            mySql_update_query = f"""UPDATE USER_ACCOUNTS 
                                  SET PASSWORD_HASH = %s, 
                                        PASSWORD_RESET_TOKEN = NULL, 
                                        PASSWORD_RESET_EXPIRES = NULL, 
                                        LAST_PASSWORD_CHANGE = %s,
                                        UPDATED_DATE = %s
                                  WHERE CORPORATE_ACCOUNT = %s 
                                    AND USER_ID IN({placeholders})"""
            record = [password_hash, datetime.now() , datetime.now() , corporate_account]
            record.extend(user_ids)

            cursor.execute(mySql_update_query, record)
            connection.commit()
            logging.info("check 6")

    except mysql.connector.Error as error:
        sts = "Failed"
        if error.errno == 1062:  # Duplicate entry
            sts_description = f"Attempt to create a duplicate entry"
        elif error.errno == 1048:  # Column cannot be null
            sts_description = "Required field is missing. Please check all required fields are provided."
        elif error.errno == 1406:  # Data too long for column
            sts_description = "One or more fields exceed the maximum allowed length."
        else:
            sts_description = f"A database error has occurred. Please try again or contact support.{error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'status': sts,
        'status_description': sts_description
    })




@account_and_project_blueprint.route('/api/update_user_status', methods=['PUT', 'POST'])
@token_required
@validate_access
def update_user_status(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    user_ids = data.get('user_ids',[])
    user_status = data.get('user_status')
    requesting_for_corporate_account = data.get('requesting_for_corporate_account')


    logging.info(f"corporate_account: {corporate_account}")
    logging.info(f"requesting for corporate_account: {requesting_for_corporate_account}")
    placeholders = ''


    # When Super Admin is logged in, they can see all users across other accounts
    if requesting_for_corporate_account:
        corporate_account = requesting_for_corporate_account


    logging.info("input to update_user_status: %s", data)


    if not validate_corporate_account(corporate_account):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Corporate account is not valid'
        })


    if not user_ids or not isinstance(user_ids, list):
        return jsonify({
            'status': 'Failed',
            'status_description': 'User Ids must be provided as an array'
        })


    # for user_id in user_ids:
    #     if not validate_user_id(corporate_account, user_id):
    #         return jsonify({
    #             'status': 'Failed',
    #             'status_description': f'User Id {user_id} does not exist'
    #         })

    if not validate_status(corporate_account, 'ALL PROJECTS', 'USER_STATUS', user_status):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Invalid user status'
        })


    sts = "Success"
    sts_description = "User status updated successfully"
    placeholders = ','.join(['%s'] * len(user_ids))

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_update_query = f"""UPDATE USER_ACCOUNTS 
                              SET STATUS = %s,
                              UPDATED_DATE = %s
                              WHERE CORPORATE_ACCOUNT = %s 
                                AND USER_ID IN({placeholders})"""
        record = [user_status, datetime.now(), corporate_account]
        record.extend(user_ids)

        cursor.execute(mySql_update_query, record)
        connection.commit()

    except mysql.connector.Error as error:
        sts = "Failed"
        if error.errno == 1062:  # Duplicate entry
            sts_description = f"Attempt to create a duplicate entry"
        elif error.errno == 1048:  # Column cannot be null
            sts_description = "Required field is missing. Please check all required fields are provided."
        elif error.errno == 1406:  # Data too long for column
            sts_description = "One or more fields exceed the maximum allowed length."
        else:
            sts_description = f"A database error has occurred. Please try again or contact support.{error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'status': sts,
        'status_description': sts_description
    })


@account_and_project_blueprint.route('/api/get_access_levels_definition_NOT_REQD', methods=['GET','POST'])
@token_required
def get_access_levels_definition_NOT_REQD(current_user):
    data = request.json
    corporate_account= data.get('corporate_account')
    access_level = data.get('access_level')
    logging.info("corporate_account: {corporate_account}")
    logging.info("access_level: {access_level}")

    sts = "Success"
    sts_description = "Access levels retrieved successfully"
    access_details = {}
    access_list = []
    access_level_str = "LEVEL_" + str(access_level)


    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_select_query = """SELECT CATEGORY_HEADER, CATEGORY_SUB_HEADER, CATEGORY_ID FROM ACCOUNT_ACCESS_LEVELS
        WHERE STATUS = %s AND CORPORATE_ACCOUNT = %s AND """ + access_level_str + """ = True ORDER BY CATEGORY_ID"""
        record = ('Active', corporate_account)


        cursor.execute(mySql_select_query, record)
        logging.info(f" executed SQL is: {cursor._executed}")
        for result in cursor.fetchall():

            access_details = {
                'category_header': result[0],
                'category_sub_header': result[1],
                'category_id': result[2]
            }
            access_list.append(access_details)


        if len(access_list) == 0:
            sts = "Failed"
            sts_description = "No matching access rows found"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the access details: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'access_list': access_list,
        'status': sts,
        'status_description': sts_description
    })

@account_and_project_blueprint.route('/api/get_user_api_access_level', methods=['GET','POST'])
@token_required
def get_user_api_access_level(current_user):
    data = request.json
    user_id = data.get('user_id')
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    api_name = data.get('api_name')

    sts = "Success"
    sts_description = "Insufficient access to perform this function"
    access_level = 1
    access_status = False

    logging.info("Inside get user access level - data =  ${data}")

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_select_query = """SELECT ACCESS_LEVEL FROM USER_PROJECTS 
        WHERE USER_ID = %s AND CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s"""
        record = (user_id, corporate_account, project_id)

        cursor.execute(mySql_select_query, record)
        result = cursor.fetchone()  # Changed to fetchone() instead of fetchall()
        logging.info(f" executed SQL 1 is: {cursor._executed}")

        if result:
                access_level = result[0]
        else:

            mySql_select_query = """SELECT ACCESS_LEVEL FROM USER_ACCOUNTS
                    WHERE USER_ID = %s AND CORPORATE_ACCOUNT = %s"""
            record = (user_id, corporate_account)

            cursor.execute(mySql_select_query, record)
            result = cursor.fetchone()  # Changed to fetchone() instead of fetchall()
            logging.info(f" executed SQL 2 is: {cursor._executed}")

            if result:
                access_level = result[0]

            else:
                sts = "Failed"
                sts_description = "No matching access row found"
                logging.info("check 1")

        if sts == 'Success' and api_name:

            mySql_select_query = f"""SELECT COUNT(*) FROM ACCOUNT_ACCESS_LEVELS A, API_ACCESS_LEVELS B
            WHERE A.CATEGORY_ID = B.CATEGORY_ID AND LEVEL_{access_level} = TRUE AND API_NAME = %s"""

            record = (api_name, )

            cursor.execute(mySql_select_query, record)
            result = cursor.fetchone()  # Changed to fetchone() instead of fetchall()
            logging.info(f" executed SQL 3 is: {cursor._executed}")

            if result:
                if result[0] > 0:
                    access_status = True
                    sts_description = "Access level retrieved successfully"


    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the access details: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'access_level': access_level,
        'access_status': access_status,
        'status': sts,
        'status_description': sts_description
    })


@account_and_project_blueprint.route('/api/get_user_access_level_new', methods=['GET','POST'])
@token_required
def get_user_access_level_new(current_user):
    data = request.json
    user_id = data.get('user_id')
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    category_header = data.get('category_header')
    category_sub_header = data.get('category_sub_header',[])

    logging.info(f"Inside get user access level new - data = {data}")
    sts = "Success"
    sts_description = "Access level retrieved successfully"
    access_level = 1
    user_actions = {}
    user_actions_list = []

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_select_query = """SELECT ACCESS_LEVEL FROM USER_PROJECTS 
        WHERE USER_ID = %s AND CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s"""
        record = (user_id, corporate_account, project_id)

        cursor.execute(mySql_select_query, record)
        result = cursor.fetchone()  # Changed to fetchone() instead of fetchall()
        logging.info(f" executed SQL 1 is: {cursor._executed}")

        if result:
                access_level = result[0]
        else:

            mySql_select_query = """SELECT ACCESS_LEVEL FROM USER_ACCOUNTS
                    WHERE USER_ID = %s AND CORPORATE_ACCOUNT = %s"""
            record = (user_id, corporate_account)

            cursor.execute(mySql_select_query, record)
            result = cursor.fetchone()  # Changed to fetchone() instead of fetchall()
            logging.info(f" executed SQL 2 is: {cursor._executed}")

            if result:
                access_level = result[0]

            else:
                logging.info("check 1")
                sts = "Failed"
                sts_description = "No matching access row found"
                logging.info("check 2")


        if sts != 'Success':
            logging.info("check 2")
            sts = "Failed"
            sts_description = "No matching access row found"
            logging.info("check 3")

        else:

            if len(category_sub_header) > 0:
                placeholders = ','.join(['%s'] * len(category_sub_header))

                mySql_select_query = f"""SELECT CATEGORY_SUB_HEADER, LEVEL_{access_level} FROM ACCOUNT_ACCESS_LEVELS 
                WHERE CORPORATE_ACCOUNT = %s 
                AND CATEGORY_HEADER = %s AND CATEGORY_SUB_HEADER IN ({placeholders})"""
                record = (corporate_account, category_header) + tuple(category_sub_header)
            else:
                mySql_select_query = f"""SELECT CATEGORY_SUB_HEADER, LEVEL_{access_level} FROM ACCOUNT_ACCESS_LEVELS  
                WHERE CORPORATE_ACCOUNT = %s AND CATEGORY_HEADER = %s"""
                record = (corporate_account, category_header)

            cursor.execute(mySql_select_query, record)
            logging.info(f" executed SQL 3 is: {cursor._executed}")
            for result in cursor.fetchall():
                user_actions = {
                    'user_action': result[0],
                    'has_access': True if result[1] == 1 else False
                }
                user_actions_list.append(user_actions)


            logging.info(f" executed SQL 3 is: {cursor._executed}")



    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the access details: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'access_level': access_level,
        'user_actions_list': user_actions_list,
        'status': sts,
        'status_description': sts_description
    })









@account_and_project_blueprint.route('/api/get_access_level_category_headers', methods=['GET','POST'])
@token_required
@validate_access
def get_access_level_category_headers(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    requesting_for_corporate_account = data.get('requesting_for_corporate_account')

    if requesting_for_corporate_account:
        corporate_account = requesting_for_corporate_account

    logging.info(f"current_user is : {current_user}")

    sts = "Success"
    sts_description = "Category header list retrieved successfully"
    category_header_list = []

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        mySql_select_query = f"""SELECT ACCESS_LEVEL FROM USER_ACCOUNTS WHERE CORPORATE_ACCOUNT = %s AND USER_ID = %s"""
        record = (corporate_account, current_user['user_id'])
        cursor.execute(mySql_select_query, record)


        result = cursor.fetchone()
        if result:
            access_level = result[0]
            logging.info(f"Access level for user {current_user['user_id']} is {access_level}")
        else:
            access_level = 1
            logging.info(f"No access level found for user {current_user['user_id']}, defaulting to {access_level}")



        mySql_select_query = f"SELECT LEVEL_{access_level} FROM ACCOUNT_ACCESS_LEVELS WHERE CORPORATE_ACCOUNT = %s AND CATEGORY_HEADER = 'Super Admin Functions' AND CATEGORY_SUB_HEADER = 'Manage Super Admin Access'"
        record = (corporate_account, )
        cursor.execute(mySql_select_query, record)
        result = cursor.fetchone()
        if result and result[0] == 1:
            include_super_admin = True
            logging.info("User has Super Admin access, including Super Admin Functions in category headers")
        else:
            include_super_admin = False
            logging.info("User does not have Super Admin access, excluding Super Admin Functions from category headers")


        if include_super_admin:
            mySql_select_query = """SELECT DISTINCT CATEGORY_HEADER FROM ACCOUNT_ACCESS_LEVELS
            WHERE CORPORATE_ACCOUNT = %s"""
        else:
            mySql_select_query = """SELECT DISTINCT CATEGORY_HEADER FROM ACCOUNT_ACCESS_LEVELS
            WHERE CORPORATE_ACCOUNT = %s AND CATEGORY_HEADER NOT IN ('Super Admin Functions')"""


        record = (corporate_account,)

        cursor.execute(mySql_select_query, record)
        for result in cursor.fetchall():
            category_header = {
                'category_header': result[0]
            }
            category_header_list.append(category_header)


        if len(category_header_list) == 0:
            sts = "Failed"
            sts_description = "No matching categories found"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the category list: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'category_header_list': category_header_list,
        'status': sts,
        'status_description': sts_description
    })




@account_and_project_blueprint.route('/api/get_admin_access_level_definitions', methods=['GET','POST'])
@token_required
@validate_access
def get_admin_access_level_definitions(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    category_header = data.get('category_header')
    requesting_for_corporate_account = data.get('requesting_for_corporate_account')

    if requesting_for_corporate_account:
        corporate_account = requesting_for_corporate_account

    sts = "Success"
    sts_description = "Category sub-header list retrieved successfully"
    category_sub_header_list = []

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_select_query = """SELECT CATEGORY_SUB_HEADER,
            LEVEL_1,
            LEVEL_2,
            LEVEL_3,
            LEVEL_4,
            LEVEL_5,
            LEVEL_6,
            LEVEL_7,
            LEVEL_8,
            LEVEL_9
         FROM ACCOUNT_ACCESS_LEVELS
        WHERE CORPORATE_ACCOUNT = %s AND CATEGORY_HEADER = %s"""
        record = (corporate_account, category_header,)

        cursor.execute(mySql_select_query, record)
        for result in cursor.fetchall():
            category_sub_headers = {
                'category_sub_header': result[0],
                'level_1': True if result[1] == 1 else False,
                'level_2': True if result[2] == 1 else False,
                'level_3': True if result[3] == 1 else False,
                'level_4': True if result[4] == 1 else False,
                'level_5': True if result[5] == 1 else False,
                'level_6': True if result[6] == 1 else False,
                'level_7': True if result[7] == 1 else False,
                'level_8': True if result[8] == 1 else False,
                'level_9': True if result[9] == 1 else False
            }
            category_sub_header_list.append(category_sub_headers)


        if len(category_sub_header_list) == 0:
            sts = "Failed"
            sts_description = "No matching categories sub-headers found"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the category sub-header list: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'category_sub_header_list': category_sub_header_list,
        'status': sts,
        'status_description': sts_description
    })


@account_and_project_blueprint.route('/api/update_admin_access_level_definition', methods=['POST'])
@token_required
@validate_access
def update_admin_access_level_definition(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    requesting_for_corporate_account = data.get('requesting_for_corporate_account')
    category_header = data.get('category_header')
    category_sub_header = data.get('category_sub_header')

    if requesting_for_corporate_account:
        corporate_account = requesting_for_corporate_account
    logging.info(f" update access level data: {data}")

    # Build dynamic update query based on provided levels
    update_fields = []
    update_values = []

    # Check which levels are provided in the request and add them to update
    level_fields = ['level_1', 'level_2', 'level_3', 'level_4', 'level_5',
                    'level_6', 'level_7', 'level_8', 'level_9']

    for level_field in level_fields:
        if level_field in data:
            update_fields.append(f"{level_field.upper()} = %s")
            update_values.append(data.get(level_field, False))

    # If no level fields are provided, return error
    if not update_fields:
        return jsonify({
            'status': 'Failed',
            'status_description': 'No access level fields provided for update. Please provide at least one level (level_1 through level_9).'
        })

    # Always update the updated_date
    update_fields.append("UPDATED_DATE = %s")
    update_values.append(datetime.now())

    # Add WHERE clause parameters
    update_values.extend([corporate_account, category_header, category_sub_header])

    sts = "Success"
    sts_description = f"Access level definition updated successfully. Updated fields: {', '.join([field.split(' = ')[0] for field in update_fields[:-1]])}"

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        # Build dynamic SQL query
        mySql_update_query = f"""UPDATE ACCOUNT_ACCESS_LEVELS SET 
            {', '.join(update_fields)}
        WHERE CORPORATE_ACCOUNT = %s
        AND CATEGORY_HEADER = %s
        AND CATEGORY_SUB_HEADER = %s"""

        cursor.execute(mySql_update_query, tuple(update_values))
        connection.commit()
        rows_impacted = cursor.rowcount
        logging.info(f"Executed SQL is: {cursor._executed}")

        if rows_impacted == 0:
            sts = "Failed"
            sts_description = "No matching access level definition found to update. Please verify the category header and sub-header exist."

    except mysql.connector.Error as error:
        sts = "Failed"
        if error.errno == 1062:  # Duplicate entry
            sts_description = f"Attempt to create a duplicate entry"
        elif error.errno == 1048:  # Column cannot be null
            sts_description = "Required field is missing. Please check all required fields are provided."
        elif error.errno == 1406:  # Data too long for column
            sts_description = "One or more fields exceed the maximum allowed length."
        else:
            sts_description = f"A database error has occurred. Please try again or contact support. {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'status': sts,
        'status_description': sts_description
    })


@account_and_project_blueprint.route('/api/get_access_level_roles', methods=['GET','POST'])
@token_required
@validate_access
def get_access_level_roles(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    requesting_for_corporate_account = data.get('requesting_for_corporate_account')

    if requesting_for_corporate_account:
        corporate_account = requesting_for_corporate_account

    sts = "Success"
    sts_description = "Access level roles retrieved successfully"
    access_level_roles  = []

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_select_query = """SELECT LEVEL_NAME, ROLE_NAME FROM ACCOUNT_ACCESS_LEVELS_ROLES
        WHERE CORPORATE_ACCOUNT = %s """
        record = (corporate_account, )

        cursor.execute(mySql_select_query, record)
        for result in cursor.fetchall():
            access_level_role = {
                'level_name': result[0],
                'role_name': result[1]
            }
            access_level_roles.append(access_level_role)


        if len(access_level_roles) == 0:
            sts = "Failed"
            sts_description = "No matching access level roles found"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve access level roles: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'access_level_roles': access_level_roles,
        'status': sts,
        'status_description': sts_description
    })


@account_and_project_blueprint.route('/api/update_account_access_levels_roles', methods=['POST'])
@token_required
@validate_access
def update_account_access_levels_roles(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    requesting_for_corporate_account = data.get('requesting_for_corporate_account')

    if requesting_for_corporate_account:
        corporate_account = requesting_for_corporate_account

    logging.info(f"update account access levels roles data: {data}")

    # Validate required corporate_account
    if not corporate_account:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Missing required field: corporate_account is required.'
        })

    # Extract level-role mappings from the request
    # Expected format: {"level_1": "role1", "level_2": "role2", ...}
    level_fields = ['level_1', 'level_2', 'level_3', 'level_4', 'level_5',
                    'level_6', 'level_7', 'level_8', 'level_9']

    # Collect all level-role pairs to be updated
    level_role_pairs = []
    for level_field in level_fields:
        if level_field in data:
            role = data.get(level_field)
            if role is not None and role != "":
                level_role_pairs.append((level_field, role))

    # If no level-role pairs are provided, return error
    if not level_role_pairs:
        return jsonify({
            'status': 'Failed',
            'status_description': 'No access level-role pairs provided for update. Please provide at least one level with an associated role.'
        })

    sts = "Success"
    updated_records = []
    failed_updates = []

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        # Process each level-role pair
        for level_name, role_name in level_role_pairs:
            try:
                # Check if record exists
                check_query = """SELECT COUNT(*) FROM ACCOUNT_ACCESS_LEVELS_ROLES 
                               WHERE CORPORATE_ACCOUNT = %s AND LEVEL_NAME = %s"""
                cursor.execute(check_query, (corporate_account, level_name))
                logging.info(f"Executed SQL-1 for checking existence: {cursor._executed}")
                record_exists = cursor.fetchone()[0] > 0

                if record_exists:
                    # Update existing record
                    update_query = """UPDATE ACCOUNT_ACCESS_LEVELS_ROLES SET 
                                    UPDATED_DATE = %s, ROLE_NAME = %s
                                    WHERE CORPORATE_ACCOUNT = %s 
                                    AND LEVEL_NAME = %s 
                                    """
                    cursor.execute(update_query, (datetime.now(),  role_name, corporate_account, level_name))
                    logging.info(f"Executed SQL-2 for checking existence: {cursor._executed}")
                    updated_records.append(f"{level_name}:{role_name}")
                else:
                    # Insert new record
                    insert_query = """INSERT INTO ACCOUNT_ACCESS_LEVELS_ROLES 
                                    (CORPORATE_ACCOUNT, LEVEL_NAME, ROLE_NAME, CREATED_DATE, UPDATED_DATE)
                                    VALUES (%s, %s, %s, %s, %s)"""
                    current_time = datetime.now()
                    cursor.execute(insert_query, (corporate_account, level_name, role_name, current_time, current_time))
                    logging.info(f"Executed SQL-3 for inserting new record: {cursor._executed}")
                    updated_records.append(f"{level_name}:{role_name} (new)")

            except mysql.connector.Error as individual_error:
                failed_updates.append(f"{level_name}:{role_name} - {str(individual_error)}")
                logging.info(f"Error updating {level_name}:{role_name}: {individual_error}")

        connection.commit()
        logging.info(f"Executed SQL operations for corporate_account: {corporate_account}")

        # Prepare response message
        if updated_records and not failed_updates:
            sts_description = f"Access level roles updated successfully. Updated/Created: {', '.join(updated_records)}"
        elif updated_records and failed_updates:
            sts = "Partial Success"
            sts_description = f"Some updates successful: {', '.join(updated_records)}. Failed: {', '.join(failed_updates)}"
        else:
            sts = "Failed"
            sts_description = f"All updates failed: {', '.join(failed_updates)}"

    except mysql.connector.Error as error:
        sts = "Failed"
        if error.errno == 1062:  # Duplicate entry
            sts_description = f"Duplicate entry error occurred during batch update."
        elif error.errno == 1048:  # Column cannot be null
            sts_description = "Required field is missing. Please check all required fields are provided."
        elif error.errno == 1406:  # Data too long for column
            sts_description = "One or more fields exceed the maximum allowed length."
        else:
            sts_description = f"A database error has occurred. Please try again or contact support. {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'status': sts,
        'status_description': sts_description
    })




@account_and_project_blueprint.route('/api/change_user_default_project', methods=['POST'])
@token_required
def change_user_default_project(current_user):
    data = request.json
    corporate_account= data.get('corporate_account')
    project_id = data.get('project_id')
    user_id = data.get('user_id')

    project_id = project_id.strip()
    if not validate_corporate_account(corporate_account):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Corporate account is not valid'
        })

    if not project_id.strip():
        return jsonify({
            'status': 'Failed',
            'status_description': 'Project Id is required'
        })

    if  not validate_project_id(corporate_account, project_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Invalid Project Id'
        })

    if  not validate_user_id(corporate_account, user_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Invalid User Id'
        })

    sts = "Success"
    sts_description = "Default project for the user updated successfully"

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_insert_query = """UPDATE USER_ACCOUNTS SET DEFAULT_PROJECT = %s, UPDATED_DATE = %s WHERE CORPORATE_ACCOUNT = %s AND USER_ID = %s AND STATUS = %s"""

        record = (project_id, datetime.now(), corporate_account, user_id,'Active')
        cursor.execute(mySql_insert_query, record)
        connection.commit()

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed update the default project: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'status': sts,
        'status_description': sts_description
    })



@account_and_project_blueprint.route('/api/get_user_info', methods=['GET','POST'])
@token_required
def get_user_info(current_user):


    data = request.json
    user_id = data.get('user_id')
    project_id = data.get('project_id')

    sts = "Success"
    sts_description = "User info retrieved successfully"
    access_level = 1
    corporate_account = ''
    user_name = ''
    created_date = ''
    update_date = ''
    default_project = ''
    account_description = ''


    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        mySql_select_query = """SELECT A.CORPORATE_ACCOUNT, A.USER_NAME, A.CREATED_DATE, A.UPDATED_DATE, A.ACCESS_LEVEL, A.DEFAULT_PROJECT, B.ACCOUNT_DESCRIPTION
                FROM USER_ACCOUNTS A, CORPORATE_ACCOUNTS B 
                WHERE A.USER_ID = %s AND A.CORPORATE_ACCOUNT = B.CORPORATE_ACCOUNT"""
        record = (user_id, )
        cursor.execute(mySql_select_query, record)
        result = cursor.fetchone()  # Changed to fetchone() instead of fetchall()
        if not result:
            sts = "Failed"
            sts_description = "No matching user found"
        else:
            corporate_account = result[0]
            user_name = result[1]
            created_date = result[2]
            update_date = result[3]
            access_level = result[4]
            default_project = result[5]
            account_description = result[6]

            logging.info(f"project Id inside get_user_info: {default_project}")

            if project_id and not validate_project_id(corporate_account, project_id):
                return jsonify({
                    'status': 'Failed',
                    'status_description': 'Invalid Project Id'
                })
            else:
                if not project_id:
                    project_id = default_project

                mySql_select_query = """SELECT ACCESS_LEVEL FROM USER_PROJECTS 
                WHERE USER_ID = %s AND CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s"""
                record = (user_id, corporate_account, project_id)
                cursor.execute(mySql_select_query, record)
                result = cursor.fetchone()  # Changed to fetchone() instead of fetchall()
                if result:
                        access_level = result[0]

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the user info: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'corporate_account': corporate_account,
        'account_description': account_description,
        'user_name': user_name,
        'created_date': created_date,
        'updated_date': update_date,
        'access_level': access_level,
        'default_project': default_project,
        'status': sts,
        'status_description': sts_description,
        'user_id': user_id
    })


if __name__ == '__main__':
    app.run(debug=True)