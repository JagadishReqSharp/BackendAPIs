from flask import Flask, request, jsonify, Blueprint
import mysql.connector
from datetime import datetime
import config
import logging
from foundational_v2 import generate_next_sequence, validate_project_id, validate_level_id, validate_req_id, \
    validate_status, validate_user_id, is_user_authorized_to_approve, validate_integration_id, validate_raid_log_entry, \
    get_project_prefix, get_link_details
from foundational_v2 import validate_corporate_account, validate_usecase_id, validate_testcase_id, \
    validate_key_attribute_list_id, validate_product_id, validate_req_classification, get_functional_level_children
from utils import token_required
from access_validation_at_api_level import validate_access
import os
import uuid


# Create a blueprint
raid_log_blueprint = Blueprint('raid_log', __name__)


# 1. Create RAID Log Entry
@raid_log_blueprint.route('/api/create_raid_log', methods=['POST', 'PUT'])
@token_required
@validate_access
def create_raid_log(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    raid_type = data.get('raid_type')
    raid_description = data.get('raid_description')
    raid_logged_by_user = data.get('raid_logged_by_user')
    criticality = data.get('criticality')
    priority = data.get('priority')
    resolution = data.get('resolution', '')
    comments = data.get('comments', '')
    status = data.get('status')
    due_date = data.get('due_date')

    logging.info(f"Inside create_raid_log data: {data}")

    if not validate_corporate_account(corporate_account):
        return jsonify({
            'raid_id': None,
            'status': 'Failed',
            'status_description': 'Corporate account is not valid'
        })
    if not validate_project_id(corporate_account, project_id):
        return jsonify({
            'raid_id': None,
            'status': 'Failed',
            'status_description': 'Project Id is not valid'
        })

    if not raid_type or not raid_type.strip():
        return jsonify({
            'raid_id': None,
            'status': 'Failed',
            'status_description': 'RAID Type is required'
        })

    if not validate_status(corporate_account, project_id, 'RAID_TYPE', raid_type):
        return jsonify({
            'raid_id': None,
            'status': 'Failed',
            'status_description': 'Invalid RAID Type'
        })

    if not raid_description or not raid_description.strip():
        return jsonify({
            'raid_id': None,
            'status': 'Failed',
            'status_description': 'RAID description is required'
        })

    if not validate_user_id(corporate_account, raid_logged_by_user):
        return jsonify({
            'raid_id': None,
            'status': 'Failed',
            'status_description': 'RAID logged by user is not valid'
        })

    if not status or not status.strip():
        return jsonify({
            'raid_id': None,
            'status': 'Failed',
            'status_description': 'RAID status is required'
        })

    if not validate_status(corporate_account, project_id, 'RAID_STATUS', status):
        return jsonify({
            'raid_id': None,
            'status': 'Failed',
            'status_description': 'Invalid RAID status'
        })

    if not criticality or not criticality.strip():
        return jsonify({
            'raid_id': None,
            'status': 'Failed',
            'status_description': 'RAID criticality is required'
        })

    if not validate_status(corporate_account, project_id, 'RAID_CRITICALITY', criticality):
        return jsonify({
            'raid_id': None,
            'status': 'Failed',
            'status_description': 'Invalid RAID criticality'
        })

    if not priority or not priority.strip():
        return jsonify({
            'raid_id': None,
            'status': 'Failed',
            'status_description': 'RAID priority is required'
        })

    if not validate_status(corporate_account, project_id, 'RAID_PRIORITY', priority):
        return jsonify({
            'raid_id': None,
            'status': 'Failed',
            'status_description': 'Invalid RAID priority'
        })

    if due_date and not isinstance(due_date, str):
        return jsonify({
            'raid_id': None,
            'status': 'Failed',
            'status_description': 'Due date must be a string'
        })

    # if due_date and due_date.strip():
    if due_date.strip() == '':
        due_date = None
    else:
        logging.info(f"Due date is not empty 2: {due_date}")
        try:
            parsed_due_date = datetime.strptime(due_date, '%Y-%m-%d')  # Adjust format as needed
            if parsed_due_date < datetime.now().replace(hour=0, minute=0, second=0, microsecond=0):
                return jsonify({
                    'raid_id': None,
                    'status': 'Failed',
                    'status_description': 'Due date cannot be in the past'
                })
        except ValueError:
            return jsonify({
                'raid_id': None,
                'status': 'Failed',
                'status_description': 'Due date must be in YYYY-MM-DD format'
            })




    sts = "Success"
    sts_description = "RAID log entry added successfully"
    raid_id = None
    raid_id_with_prefix = None

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        raid_id, seq_status, seq_status_description = generate_next_sequence(corporate_account, project_id, 'RAID_LOG')

        if seq_status == "Failed":
            sts = "Failed"
            sts_description = seq_status_description
            return jsonify({
                'raid_id': raid_id,
                'status': sts,
                'status_description': sts_description
            })

        # For RAID log, we use the raid_type as the prefix
        raid_id_with_prefix = f"{raid_type.strip()}-{raid_id}"

        mySql_insert_query = """INSERT INTO RAID_LOG (CORPORATE_ACCOUNT, PROJECT_ID, RAID_ID, RAID_ID_WITH_PREFIX, 
                                RAID_TYPE, RAID_DESCRIPTION, RAID_LOGGED_BY_USER, CRITICALITY, PRIORITY, RESOLUTION, COMMENTS, STATUS, 
                                CREATED_DATE, UPDATED_DATE, DUE_DATE)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        record = (
        corporate_account, project_id, raid_id, raid_id_with_prefix, raid_type, raid_description, raid_logged_by_user,
        criticality, priority, resolution, comments, status, datetime.now(), datetime.now(), due_date)





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
        'raid_id': raid_id,
        'raid_id_with_prefix': raid_id_with_prefix,
        'status': sts,
        'status_description': sts_description
    })


@raid_log_blueprint.route('/api/update_raid_log', methods=['PUT', 'POST'])
@token_required
@validate_access
def update_raid_log(current_user):
    data = request.json
    raid_id = data.get('raid_id')
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    raid_type = data.get('raid_type')
    raid_description = data.get('raid_description')
    raid_logged_by_user = data.get('raid_logged_by_user')
    criticality = data.get('criticality')
    priority = data.get('priority')
    resolution = data.get('resolution', '')
    comments = data.get('comments', '')
    status = data.get('status')
    due_date = data.get('due_date')

    sts = "Success"
    sts_description = "RAID log entry updated successfully"
    rows_impacted = 0

    logging.info(f"top of update_raid_log {data}")

    if not validate_corporate_account(corporate_account):
        return jsonify({
            'raid_id': raid_id,
            'status': 'Failed',
            'status_description': 'Corporate account is not valid',
            'rows_impacted': rows_impacted
        })

    if not validate_project_id(corporate_account, project_id):
        return jsonify({
            'raid_id': raid_id,
            'status': 'Failed',
            'status_description': 'Project Id is not valid',
            'rows_impacted': rows_impacted
        })

    if not raid_type or not raid_type.strip():
        return jsonify({
            'raid_id': raid_id,
            'status': 'Failed',
            'status_description': 'RAID Type is required',
            'rows_impacted': rows_impacted
        })

    if not validate_status(corporate_account, project_id, 'RAID_TYPE', raid_type):
        return jsonify({
            'raid_id': raid_id,
            'status': 'Failed',
            'status_description': 'Invalid RAID Type',
            'rows_impacted': rows_impacted
        })

    if not raid_description or not raid_description.strip():
        return jsonify({
            'raid_id': raid_id,
            'status': 'Failed',
            'status_description': 'RAID description is required',
            'rows_impacted': rows_impacted
        })

    if not validate_user_id(corporate_account, raid_logged_by_user):
        return jsonify({
            'raid_id': raid_id,
            'status': 'Failed',
            'status_description': 'RAID logged by user is not valid',
            'rows_impacted': rows_impacted
        })

    if not status or not status.strip():
        return jsonify({
            'raid_id': raid_id,
            'status': 'Failed',
            'status_description': 'RAID status is required',
            'rows_impacted': rows_impacted
        })

    if not validate_status(corporate_account, project_id, 'RAID_STATUS', status):
        return jsonify({
            'raid_id': raid_id,
            'status': 'Failed',
            'status_description': 'Invalid RAID status',
            'rows_impacted': rows_impacted
        })

    if not criticality or not criticality.strip():
        return jsonify({
            'raid_id': raid_id,
            'status': 'Failed',
            'status_description': 'RAID criticality is required',
            'rows_impacted': rows_impacted
        })

    if not validate_status(corporate_account, project_id, 'RAID_CRITICALITY', criticality):
        return jsonify({
            'raid_id': raid_id,
            'status': 'Failed',
            'status_description': 'Invalid RAID criticality',
            'rows_impacted': rows_impacted
        })

    if not priority or not priority.strip():
        return jsonify({
            'raid_id': raid_id,
            'status': 'Failed',
            'status_description': 'RAID priority is required',
            'rows_impacted': rows_impacted
        })

    if not validate_status(corporate_account, project_id, 'RAID_PRIORITY', priority):
        return jsonify({
            'raid_id': raid_id,
            'status': 'Failed',
            'status_description': 'Invalid RAID priority',
            'rows_impacted': rows_impacted
        })

    if due_date and not isinstance(due_date, str):
        return jsonify({
            'raid_id': raid_id,
            'status': 'Failed',
            'status_description': 'Due date must be a string',
            'rows_impacted': rows_impacted
        })

    if due_date:
        try:
            parsed_due_date = datetime.strptime(due_date, '%Y-%m-%d')  # Adjust format as needed
            if parsed_due_date < datetime.now().replace(hour=0, minute=0, second=0, microsecond=0):
                return jsonify({
                    'raid_id': raid_id,
                    'status': 'Failed',
                    'status_description': 'Due date cannot be in the past',
                    'rows_impacted': rows_impacted
                })
        except ValueError:
            return jsonify({
                'raid_id': raid_id,
                'status': 'Failed',
                'status_description': 'Due date must be in YYYY-MM-DD format',
                'rows_impacted': rows_impacted
            })

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        mySql_update_query = """UPDATE RAID_LOG SET RAID_TYPE = %s, RAID_DESCRIPTION = %s, RAID_LOGGED_BY_USER = %s,
                               CRITICALITY = %s, PRIORITY = %s, RESOLUTION = %s, COMMENTS = %s, STATUS = %s, 
                               UPDATED_DATE = %s, DUE_DATE = %s
                               WHERE RAID_ID = %s AND CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s"""

        record = (raid_type, raid_description, raid_logged_by_user, criticality, priority, resolution,
                  comments, status, datetime.now(), due_date, raid_id, corporate_account, project_id)
        cursor.execute(mySql_update_query, record)

        rows_impacted = cursor.rowcount
        if rows_impacted == 0:
            sts = "Failed"
            sts_description = "No matching RAID log entry found to update"

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
        'raid_id': raid_id,
        'status': sts,
        'status_description': sts_description,
        'rows_impacted': rows_impacted
    })

# 3. Update RAID Log Status
@raid_log_blueprint.route('/api/update_raid_log_status', methods=['PUT', 'POST'])
@token_required
@validate_access
def update_raid_log_status(current_user):
    data = request.json
    raid_ids = data.get('raid_ids', [])
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    status = data.get('status')

    sts = "Success"
    sts_description = "RAID log status updated successfully"
    rows_impacted = 0
    placeholders = ''

    if not raid_ids or not isinstance(raid_ids, list):
        return jsonify({
            'status': 'Failed',
            'status_description': 'RAID Ids must be provided as an array'
        })

    if not validate_status(corporate_account, project_id, 'RAID_STATUS', status):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Invalid status'
        })

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        placeholders = ','.join(['%s'] * len(raid_ids))
        mySql_update_query = f"""UPDATE RAID_LOG SET STATUS = %s, UPDATED_DATE = %s 
                                WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND RAID_ID IN({placeholders})"""

        record = [status, datetime.now(), corporate_account, project_id]
        record.extend(raid_ids)
        cursor.execute(mySql_update_query, record)
        logging.info(f"Executed SQL is: {cursor._executed}")

        rows_impacted = cursor.rowcount
        if rows_impacted == 0:
            sts = "Failed"
            sts_description = "No matching RAID log entries found to update"

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
        'status_description': sts_description,
        'rows_impacted': rows_impacted
    })


# 8. Get RAID Log Details
@raid_log_blueprint.route('/api/get_raid_log_details', methods=['GET', 'POST'])
@token_required
def get_raid_log_details(current_user):
    data = request.json
    raid_id = data.get('raid_id')
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')

    logging.info(f"data for get_raid_log_details: {data}")

    if not validate_corporate_account(corporate_account):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Corporate account is not valid',
            'raid_log_details': {}
        })

    if not validate_project_id(corporate_account, project_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Project Id is not valid',
            'raid_log_details': {}
        })

    if not raid_id:
        return jsonify({
            'status': 'Failed',
            'status_description': 'RAID ID is required',
            'raid_log_details': {}
        })

    sts = "Success"
    sts_description = "RAID log details retrieved successfully"
    raid_log_details = {}

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        mySql_select_query = """SELECT R.RAID_ID, R.RAID_ID_WITH_PREFIX, R.RAID_TYPE, R.RAID_DESCRIPTION, 
                               R.RAID_LOGGED_BY_USER, R.CRITICALITY, R.PRIORITY, R.RESOLUTION, R.COMMENTS, 
                               R.STATUS, R.CREATED_DATE, R.UPDATED_DATE, R.DUE_DATE, U.USER_NAME AS LOGGED_BY_USER_NAME
                               FROM RAID_LOG R
                               LEFT JOIN USER_ACCOUNTS U ON R.CORPORATE_ACCOUNT = U.CORPORATE_ACCOUNT 
                                    AND R.RAID_LOGGED_BY_USER = U.USER_ID
                               WHERE R.RAID_ID = %s AND R.CORPORATE_ACCOUNT = %s AND R.PROJECT_ID = %s"""

        record = (raid_id, corporate_account, project_id)
        cursor.execute(mySql_select_query, record)
        logging.info(f"Executed SQL is: {cursor._executed}")
        result = cursor.fetchone()

        if result:
            raid_log_details = {
                'raid_id': result[0],
                'raid_id_with_prefix': result[1],
                'raid_type': result[2],
                'raid_description': result[3],
                'raid_logged_by_user': result[4],
                'criticality': result[5],
                'priority': result[6],
                'resolution': result[7],
                'comments': result[8],
                'status': result[9],
                'created_date': result[10],
                'updated_date': result[11],
                'due_date': result[12],
                'logged_by_user_name': result[13]
            }
        else:
            sts = "Failed"
            sts_description = "No matching RAID log entry found"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the RAID log details: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'raid_log_details': raid_log_details,
        'status': sts,
        'status_description': sts_description
    })


# 9. Add RAID Log Assignee
@raid_log_blueprint.route('/api/add_raid_log_assignee', methods=['POST'])
@token_required
@validate_access
def add_raid_log_assignee(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    raid_type = data.get('raid_type')
    raid_id = data.get('raid_id')
    raid_owner_user_id = data.get('raid_owner_user_id')
    raid_owner_type = data.get('raid_owner_type')

    if not validate_corporate_account(corporate_account):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Corporate account is not valid'
        })
    if not validate_project_id(corporate_account, project_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Project Id is not valid'
        })
    if not validate_raid_log_entry(corporate_account, project_id, raid_type, raid_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'RAID Id is not valid'
        })
    if not validate_user_id(corporate_account, raid_owner_user_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'User Id is not valid'
        })

    if not validate_status(corporate_account, project_id, 'RAID_OWNER_TYPE', raid_owner_type):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Invalid assignee type'
        })

    sts = "Success"
    sts_description = "User added as an assignee successfully"

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        mySql_insert_query = """INSERT INTO RAID_LOG_ASSIGNEES 
                              (CORPORATE_ACCOUNT, PROJECT_ID, RAID_ID, RAID_OWNER_USER_ID, RAID_OWNER_TYPE, CREATED_DATE, UPDATED_DATE)
                              VALUES (%s, %s, %s, %s, %s, %s, %s)"""

        record = (corporate_account, project_id, raid_id, raid_owner_user_id, raid_owner_type, datetime.now(), datetime.now())
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


@raid_log_blueprint.route('/api/update_raid_log_assignee', methods=['PUT'])
@token_required
@validate_access
def update_raid_log_assignee(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    raid_type = data.get('raid_type')
    raid_id = data.get('raid_id')
    raid_owner_user_id = data.get('raid_owner_user_id')
    raid_owner_type = data.get('raid_owner_type')

    if not validate_corporate_account(corporate_account):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Corporate account is not valid'
        })

    if not validate_project_id(corporate_account, project_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Project Id is not valid'
        })

    if not validate_raid_log_entry(corporate_account, project_id, raid_type, raid_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'RAID Id is not valid'
        })

    if not validate_user_id(corporate_account, raid_owner_user_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'User Id is not valid'
        })

    if not validate_status(corporate_account, project_id, 'RAID_OWNER_TYPE', raid_owner_type):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Invalid assignee type'
        })


    sts = "Success"
    sts_description = "Assignee updated successfully"

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        # First check if the assignee record exists
        check_query = """SELECT COUNT(*) FROM RAID_LOG_ASSIGNEES 
                         WHERE CORPORATE_ACCOUNT = %s 
                         AND PROJECT_ID = %s 
                         AND RAID_ID = %s 
                         AND RAID_OWNER_USER_ID = %s"""

        cursor.execute(check_query, (corporate_account, project_id, raid_id, raid_owner_user_id ))
        result = cursor.fetchone()

        if result[0] == 0:
            return jsonify({
                'status': 'Failed',
                'status_description': 'Assignee record not found'
            })

        # Update the assignee record
        update_query = """UPDATE RAID_LOG_ASSIGNEES 
                          SET RAID_OWNER_TYPE = %s, 
                              UPDATED_DATE = %s 
                          WHERE CORPORATE_ACCOUNT = %s 
                          AND PROJECT_ID = %s 
                          AND RAID_ID = %s 
                          AND RAID_OWNER_USER_ID = %s"""

        record = ( raid_owner_type, datetime.now(),
                  corporate_account, project_id, raid_id, raid_owner_user_id)

        cursor.execute(update_query, record)
        connection.commit()

        if cursor.rowcount == 0:
            sts = "Failed"
            sts_description = "No records were updated"

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


# 10. Delete RAID Log Assignee
@raid_log_blueprint.route('/api/delete_raid_log_assignee', methods=['PUT', 'POST'])
@token_required
@validate_access
def delete_raid_log_assignee(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    raid_type = data.get('raid_type')
    raid_id = data.get('raid_id')
    raid_owner_user_id = data.get('raid_owner_user_id')

    if not validate_corporate_account(corporate_account):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Corporate account is not valid'
        })
    if not validate_project_id(corporate_account, project_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Project Id is not valid'
        })
    if not validate_raid_log_entry(corporate_account, project_id, raid_type, raid_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'RAID Id is not valid'
        })
    if not validate_user_id(corporate_account, raid_owner_user_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'User Id is not valid'
        })

    sts = "Success"
    sts_description = "RAID log assignee successfully deleted"

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        mySql_delete_query = """DELETE FROM RAID_LOG_ASSIGNEES 
                              WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND RAID_ID = %s AND RAID_OWNER_USER_ID = %s"""

        record = (corporate_account, project_id, raid_id, raid_owner_user_id)
        cursor.execute(mySql_delete_query, record)
        connection.commit()

        rows_impacted = cursor.rowcount
        if rows_impacted == 0:
            sts = "Failed"
            sts_description = "No matching assignee found to delete"

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


# 11. Get RAID Log Assignee List
@raid_log_blueprint.route('/api/get_raid_log_assignee_list', methods=['GET', 'POST'])
@token_required
def get_raid_log_assignee_list(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    raid_type = data.get('raid_type')
    raid_id = data.get('raid_id')

    logging.info(f"data for get_raid_log_assignee_list: {data}")

    if not validate_corporate_account(corporate_account):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Corporate account is not valid'
        })
    if not validate_project_id(corporate_account, project_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Project Id is not valid'
        })
    if (raid_id or  raid_type)  and not validate_raid_log_entry(corporate_account, project_id, raid_type, raid_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'RAID Id is not valid'
        })

    sts = "Success"
    sts_description = "RAID log assignees retrieved successfully"
    assignee_details = {}
    assignee_list = []

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()


        if raid_id or raid_type:
            mySql_select_query = """SELECT   A.RAID_OWNER_USER_ID, A.RAID_OWNER_TYPE, B.USER_NAME,
                                   A.CREATED_DATE, A.UPDATED_DATE
                                   FROM RAID_LOG_ASSIGNEES A
                                   JOIN USER_ACCOUNTS B ON A.CORPORATE_ACCOUNT = B.CORPORATE_ACCOUNT AND A.RAID_OWNER_USER_ID = B.USER_ID
                                   WHERE A.CORPORATE_ACCOUNT = %s AND A.PROJECT_ID = %s AND A.RAID_ID = %s"""
            params = [corporate_account, project_id, raid_id]
        else:
            mySql_select_query = """SELECT   DISTINCT A.RAID_OWNER_USER_ID, 'NA', B.USER_NAME, NOW(), NOW()
                                   FROM RAID_LOG_ASSIGNEES A
                                   JOIN USER_ACCOUNTS B ON A.CORPORATE_ACCOUNT = B.CORPORATE_ACCOUNT AND A.RAID_OWNER_USER_ID = B.USER_ID
                                   WHERE A.CORPORATE_ACCOUNT = %s AND A.PROJECT_ID = %s"""
            params = [corporate_account, project_id]

        cursor.execute(mySql_select_query, params)
        logging.info(f"Executed SQL is: {cursor._executed}")

        for result in cursor.fetchall():
            assignee_details = {
                'raid_owner_user_id': result[0],
                'raid_owner_type': result[1],
                'user_name': result[2],
                'created_date': result[3],
                'updated_date': result[4]
            }
            assignee_list.append(assignee_details)

        if len(assignee_list) == 0:
            sts = "Failed"
            sts_description = "No matching assignees found"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the assignee details: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'assignee_list': assignee_list,
        'status': sts,
        'status_description': sts_description
    })


# 4. Update RAID Log Criticality
@raid_log_blueprint.route('/api/update_raid_log_criticality', methods=['PUT', 'POST'])
@token_required
@validate_access
def update_raid_log_criticality(current_user):
    data = request.json
    raid_ids = data.get('raid_ids', [])
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    criticality = data.get('criticality')

    sts = "Success"
    sts_description = "RAID log criticality updated successfully"
    rows_impacted = 0
    placeholders = ''

    if not raid_ids or not isinstance(raid_ids, list):
        return jsonify({
            'status': 'Failed',
            'status_description': 'RAID Ids must be provided as an array'
        })

    if not validate_status(corporate_account, project_id, 'RAID_CRITICALITY', criticality):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Invalid criticality'
        })

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        placeholders = ','.join(['%s'] * len(raid_ids))
        mySql_update_query = f"""UPDATE RAID_LOG SET CRITICALITY = %s, UPDATED_DATE = %s 
                                WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND RAID_ID IN({placeholders})"""

        record = [criticality, datetime.now(), corporate_account, project_id]
        record.extend(raid_ids)
        cursor.execute(mySql_update_query, record)
        logging.info(f"Executed SQL is: {cursor._executed}")

        rows_impacted = cursor.rowcount
        if rows_impacted == 0:
            sts = "Failed"
            sts_description = "No matching RAID log entries found to update"

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
        'status_description': sts_description,
        'rows_impacted': rows_impacted
    })


# 5. Update RAID Log Priority
@raid_log_blueprint.route('/api/update_raid_log_priority', methods=['PUT', 'POST'])
@token_required
@validate_access
def update_raid_log_priority(current_user):
    data = request.json
    raid_ids = data.get('raid_ids', [])
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    priority = data.get('priority')

    sts = "Success"
    sts_description = "RAID log priority updated successfully"
    rows_impacted = 0
    placeholders = ''

    if not raid_ids or not isinstance(raid_ids, list):
        return jsonify({
            'status': 'Failed',
            'status_description': 'RAID Ids must be provided as an array'
        })

    if not validate_status(corporate_account, project_id, 'RAID_PRIORITY', priority):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Invalid priority'
        })

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        placeholders = ','.join(['%s'] * len(raid_ids))
        mySql_update_query = f"""UPDATE RAID_LOG SET PRIORITY = %s, UPDATED_DATE = %s 
                                WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND RAID_ID IN({placeholders})"""

        record = [priority, datetime.now(), corporate_account, project_id]
        record.extend(raid_ids)
        cursor.execute(mySql_update_query, record)
        logging.info(f"Executed SQL is: {cursor._executed}")

        rows_impacted = cursor.rowcount
        if rows_impacted == 0:
            sts = "Failed"
            sts_description = "No matching RAID log entries found to update"

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
        'status_description': sts_description,
        'rows_impacted': rows_impacted
    })


# Flask route for your backend API to handle copying RAID logs with attachments

@raid_log_blueprint.route('/api/copy_raid_log_entry', methods=['POST'])
@token_required
@validate_access
def copy_raid_log_entry(current_user):
    try:
        # Get parameters from request
        data = request.json
        corporate_account = data.get('corporate_account')
        from_project_id = data.get('from_project_id')
        from_raid_type = data.get('from_raid_type')
        from_raid_id = data.get('from_raid_id')
        to_project_id = data.get('to_project_id')
        to_raid_type = data.get('to_raid_type')
        to_status = data.get('to_status', 'Created')
        copy_attachments = data.get('copy_attachments', False)  # New parameter


        logging.info(f"Copying RAID log entry from {from_project_id} to {to_project_id} with type {from_raid_type} and ID {from_raid_id} with copy_attachments={copy_attachments}")
        # Validate required parameters
        if not all([corporate_account, from_project_id, from_raid_type, from_raid_id,
                    to_project_id, to_raid_type]):
            return jsonify({
                'status': 'Error',
                'status_description': 'Missing required parameters'
            }), 400

        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor(dictionary=True)

        logging.info("hello 1")
        # First, get the original RAID log entry
        cursor.execute("""
            SELECT * FROM RAID_LOG
            WHERE CORPORATE_ACCOUNT = %s 
            AND PROJECT_ID = %s 
            AND RAID_TYPE = %s 
            AND RAID_ID = %s
        """, (corporate_account, from_project_id, from_raid_type, from_raid_id))

        original_raid = cursor.fetchone()
        logging.info("hello 2")

        if not original_raid:
            cursor.close()
            connection.close()
            return jsonify({
                'status': 'Error',
                'status_description': 'Original RAID log entry not found'
            }), 404


        new_raid_id, seq_status, seq_status_description = generate_next_sequence(corporate_account, to_project_id,'RAID_LOG')

        if seq_status == "Failed":
            sts = "Failed"
            sts_description = seq_status_description
            return jsonify({
                'status': sts,
                'status_description': sts_description
            })


        new_raid_id_with_prefix = f"{to_raid_type.strip()}-{new_raid_id}"

        logging.info("hello 3")

        # # Insert the new RAID log with copied data and new status
        # current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        cursor.execute("""
            INSERT INTO RAID_LOG SELECT 
                CORPORATE_ACCOUNT, PROJECT_ID, %s, %s, %s, 
                RAID_DESCRIPTION, CRITICALITY, PRIORITY, RAID_LOGGED_BY_USER,  
                RESOLUTION, COMMENTS, %s, DUE_DATE, 
                NOW(), NOW() FROM RAID_LOG WHERE 
                CORPORATE_ACCOUNT = %s
                AND PROJECT_ID = %s
                AND RAID_TYPE = %s
                AND RAID_ID = %s
        """, (new_raid_id, new_raid_id_with_prefix, to_raid_type, to_status, corporate_account, from_project_id, from_raid_type, from_raid_id))




        logging.info("hello 4")

        attachment_count = 0

        # Handle attachment copying if requested
        if copy_attachments:
            logging.info("hello 5")

            # Get the attachments of the original RAID log
            from_req_id = f"RAID-{from_raid_id}"
            to_req_id = f"RAID-{new_raid_id}"

            logging.info(f"Copying attachments from {from_req_id} to {to_req_id}")

            cursor.execute("""
                SELECT * FROM REQUIREMENT_ATTACHMENTS
                WHERE CORPORATE_ACCOUNT = %s
                AND PROJECT_ID = %s
                AND REQ_ID = %s
            """, (corporate_account, from_project_id, from_req_id))

            original_attachments = cursor.fetchall()
            attachment_count = len(original_attachments)

            logging.info("hello 6")

            logging.info(f"Found {attachment_count} attachments to copy")

            for attachment in original_attachments:
                logging.info("hello 7")

                # Extract original file information
                original_file_path = attachment['FILE_PATH']
                original_filename = attachment['FILE_NAME']

                logging.info(f"Processing attachment: {original_filename}, path: {original_file_path}")

                # Build the full path to the original file
                full_original_path = os.path.join(config.ATTACHMENTS_FOLDER_PATH, original_file_path)

                if not os.path.exists(full_original_path):
                    logging.warning(f"Original file not found: {full_original_path}")
                    continue  # Skip if original file doesn't exist

                # Create a new unique filename but keep the extension
                file_extension = original_filename.rsplit('.', 1)[1].lower() if '.' in original_filename else ''
                unique_filename = f"{uuid.uuid4().hex}.{file_extension}" if file_extension else f"{uuid.uuid4().hex}"

                # Create target directory path
                target_rel_dir = f"{corporate_account}/{to_project_id}/{to_req_id}"
                target_full_dir = os.path.join(config.ATTACHMENTS_FOLDER_PATH, target_rel_dir)

                # Ensure the target directory exists
                logging.info(f"Creating directory: {target_full_dir}")
                os.makedirs(target_full_dir, exist_ok=True)

                # Create the full path for the new file
                target_rel_path = f"{target_rel_dir}/{unique_filename}"
                target_full_path = os.path.join(config.ATTACHMENTS_FOLDER_PATH, target_rel_path)

                logging.info(f"Copying file to: {target_full_path}")

                # Copy the file
                try:
                    with open(full_original_path, 'rb') as src_file:
                        file_content = src_file.read()
                        with open(target_full_path, 'wb') as dest_file:
                            dest_file.write(file_content)

                    logging.info(f"File copied successfully")

                    # Insert the new attachment record
                    cursor.execute("""
                        INSERT INTO REQUIREMENT_ATTACHMENTS 
                        (CORPORATE_ACCOUNT, PROJECT_ID, REQ_ID, FILE_NAME, FILE_PATH, FILE_SIZE, FILE_TYPE, UPLOADED_BY)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        corporate_account,
                        to_project_id,
                        to_req_id,
                        original_filename,
                        target_rel_path,
                        attachment['FILE_SIZE'],
                        attachment['FILE_TYPE'],
                        current_user['user_id']
                    ))
                    logging.info("hello 8")

                    logging.info(f"Database record created for copied attachment")

                except Exception as file_error:
                    logging.error(f"Error copying file: {str(file_error)}")

        connection.commit()

        # Return the new RAID ID information
        raid_id_with_prefix = f"{to_raid_type}-{new_raid_id}"

        cursor.close()
        connection.close()

        return jsonify({
            'status': 'Success',
            'status_description': 'RAID log entry copied successfully',
            'raid_id': new_raid_id,
            'raid_id_with_prefix': raid_id_with_prefix,
            'raid_type': to_raid_type,
            'attachments_copied': copy_attachments,
            'attachment_count': attachment_count
        })

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
        logging.info(f"Failed to retrieve the requirement details: {error}")

        return jsonify({
            'status': 'Error',
            'status_description': sts_description
        }), 500



 # 7. Get RAID Log List
@raid_log_blueprint.route('/api/get_raid_log_list', methods=['GET', 'POST'])
@token_required
def get_raid_log_list(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    filter_by_status = data.get('filter_by_status', [])
    filter_by_type = data.get('filter_by_type', [])
    filter_by_criticality = data.get('filter_by_criticality', [])
    filter_by_priority = data.get('filter_by_priority', [])
    filter_by_raid_logged_by_user = data.get('filter_by_raid_logged_by_user', [])
    filter_by_assignees = data.get('filter_by_assignees', [])
    search_query = data.get('search_query')
    sort_criteria = data.get('sort_criteria')
    created_date_start = data.get('created_date_start')
    created_date_end = data.get('created_date_end')
    updated_date_start = data.get('updated_date_start')
    updated_date_end = data.get('updated_date_end')
    due_date_start = data.get('due_date_start')  # Added filter
    due_date_end = data.get('due_date_end')  # Added filter

    logging.info(f"data for get_raid_log_list: {data}")

    if not validate_corporate_account(corporate_account):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Corporate account is not valid',
            'raid_log_list': []
        })

    if not validate_project_id(corporate_account, project_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Project Id is not valid',
            'raid_log_list': []
        })

    sts = "Success"
    sts_description = "RAID log entries retrieved successfully"
    raid_log_details = {}
    raid_log_list = []

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        # Base query without ORDER BY
        mySql_select_query = """SELECT R.RAID_ID, R.RAID_ID_WITH_PREFIX, R.RAID_TYPE, R.RAID_DESCRIPTION, 
                               R.RAID_LOGGED_BY_USER, R.CRITICALITY, R.PRIORITY, R.RESOLUTION, R.COMMENTS, 
                               R.STATUS, R.CREATED_DATE, R.UPDATED_DATE, R.DUE_DATE,
                               (SELECT COUNT(*) FROM RAID_LOG_ASSIGNEES A 
                                WHERE R.CORPORATE_ACCOUNT = A.CORPORATE_ACCOUNT 
                                AND R.PROJECT_ID = A.PROJECT_ID 
                                AND R.RAID_ID = A.RAID_ID) AS NUMBER_OF_ASSIGNEES,
                               U.USER_NAME AS LOGGED_BY_USER_NAME  /* Get the user name */
                               FROM RAID_LOG R
                               LEFT JOIN USER_ACCOUNTS U ON R.CORPORATE_ACCOUNT = U.CORPORATE_ACCOUNT 
                                    AND R.RAID_LOGGED_BY_USER = U.USER_ID
                               WHERE R.CORPORATE_ACCOUNT = %s AND R.PROJECT_ID = %s"""

        params = [corporate_account, project_id]

        # Add status filter if provided
        if filter_by_status and isinstance(filter_by_status, list) and len(filter_by_status) > 0:
            placeholders = ','.join(['%s'] * len(filter_by_status))
            mySql_select_query += f" AND R.STATUS IN({placeholders})"
            params.extend(filter_by_status)

        # Add type filter if provided
        if filter_by_type and isinstance(filter_by_type, list) and len(filter_by_type) > 0:
            placeholders = ','.join(['%s'] * len(filter_by_type))
            mySql_select_query += f" AND R.RAID_TYPE IN({placeholders})"
            params.extend(filter_by_type)

        # Add criticality filter if provided
        if filter_by_criticality and isinstance(filter_by_criticality, list) and len(filter_by_criticality) > 0:
            placeholders = ','.join(['%s'] * len(filter_by_criticality))
            mySql_select_query += f" AND R.CRITICALITY IN({placeholders})"
            params.extend(filter_by_criticality)

        # Add priority filter if provided
        if filter_by_priority and isinstance(filter_by_priority, list) and len(filter_by_priority) > 0:
            placeholders = ','.join(['%s'] * len(filter_by_priority))
            mySql_select_query += f" AND R.PRIORITY IN({placeholders})"
            params.extend(filter_by_priority)

        # Add raid_logged_by_user filter if provided
        if filter_by_raid_logged_by_user and isinstance(filter_by_raid_logged_by_user, list) and len(
                filter_by_raid_logged_by_user) > 0:
            placeholders = ','.join(['%s'] * len(filter_by_raid_logged_by_user))
            mySql_select_query += f" AND R.RAID_LOGGED_BY_USER IN({placeholders})"
            params.extend(filter_by_raid_logged_by_user)

        # Add approver filter if provided
        if filter_by_assignees and isinstance(filter_by_assignees, list) and len(
                filter_by_assignees) > 0:
            placeholders = ','.join(['%s'] * len(filter_by_assignees))
            mySql_select_query += f""" AND EXISTS (SELECT * FROM RAID_LOG_ASSIGNEES X WHERE R.RAID_ID = X.RAID_ID AND R.CORPORATE_ACCOUNT = X.CORPORATE_ACCOUNT 
            AND R.PROJECT_ID = X.PROJECT_ID AND X.RAID_OWNER_USER_ID IN({placeholders}) )"""
            params.extend(filter_by_assignees)


        # Add created_date range filter if provided
        if created_date_start:
            mySql_select_query += " AND R.CREATED_DATE >= %s"
            params.append(created_date_start)

        if created_date_end:
            mySql_select_query += " AND DATE(R.CREATED_DATE) <= DATE(%s)"
            params.append(created_date_end)

        # Add updated_date range filter if provided
        if updated_date_start:
            mySql_select_query += " AND R.UPDATED_DATE >= %s"
            params.append(updated_date_start)

        if updated_date_end:
            mySql_select_query += " AND DATE(R.UPDATED_DATE) <= DATE(%s)"
            params.append(updated_date_end)

        # Add due_date range filter if provided
        if due_date_start:
            mySql_select_query += " AND R.DUE_DATE >= %s"
            params.append(due_date_start)

        if due_date_end:
            mySql_select_query += " AND DATE(R.DUE_DATE) <= DATE(%s)"
            params.append(due_date_end)

        # Add search query filter if provided
        if search_query:
            mySql_select_query += " AND (R.RAID_ID_WITH_PREFIX LIKE %s OR R.RAID_DESCRIPTION LIKE %s OR R.RESOLUTION LIKE %s OR R.COMMENTS LIKE %s)"
            params.extend([f"%{search_query}%", f"%{search_query}%", f"%{search_query}%", f"%{search_query}%"])

        # Add ORDER BY at the end
        if not sort_criteria:
            sort_criteria = 'RAID_ID'

        mySql_select_query += f" ORDER BY {sort_criteria}"

        logging.info(f"Prepared SQL is: {mySql_select_query}")

        cursor.execute(mySql_select_query, tuple(params))
        logging.info(f"Executed SQL is: {cursor._executed}")

        for result in cursor.fetchall():
            raid_log_details = {
                'raid_id': result[0],
                'raid_id_with_prefix': result[1],
                'raid_type': result[2],
                'raid_description': result[3],
                'raid_logged_by_user': result[4],
                'criticality': result[5],
                'priority': result[6],
                'resolution': result[7],
                'comments': result[8],
                'status': result[9],
                'created_date': result[10],
                'updated_date': result[11],
                'due_date': result[12],
                'number_of_assignees': result[13],
                'logged_by_user_name': result[14]
            }
            raid_log_list.append(raid_log_details)

        if len(raid_log_list) == 0:
            sts = "Failed"
            sts_description = "No matching RAID log entries found"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the RAID log entries: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'raid_log_list': raid_log_list,
        'status': sts,
        'status_description': sts_description
    })

