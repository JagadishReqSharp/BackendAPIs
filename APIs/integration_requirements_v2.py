from flask import Flask, request, jsonify, Blueprint
import mysql.connector
from datetime import datetime
import config
import logging
from foundational_v2 import generate_next_sequence, validate_project_id, validate_level_id, validate_req_id, validate_status, validate_user_id, is_valid_field_name, get_functional_level_children, validate_product_id
from foundational_v2 import validate_corporate_account, validate_usecase_id, validate_testcase_id,  validate_key_attribute_list_id, validate_integration_system_id, validate_integration_id, validate_integration_field
from utils import token_required
from access_validation_at_api_level import validate_access
import os
import uuid

# Create a blueprint for user-related routes
integration_requirements_blueprint = Blueprint('integration_requirements', __name__)

app = Flask(__name__)

logging.basicConfig(filename='debugging.log', level=logging.DEBUG)

@integration_requirements_blueprint.route('/api/create_integration_system_OLD', methods=['POST'])
@token_required
@validate_access
def create_integration_system_OLD(current_user):

    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    system_name = data.get('system_name')
    system_description = data.get('system_description')
    system_acronym = data.get('system_acronym')


    if not validate_corporate_account(corporate_account):
        return jsonify({
            'req_id': None,
            'status': 'Failed',
            'status_description': 'Corporate account is not valid'
        })
    if not validate_project_id(corporate_account, project_id):
        return jsonify({
            'req_id': None,
            'status': 'Failed',
            'status_description': 'Project Id is not valid'
        })
    if not system_name.strip():
        return jsonify({
            'req_id': None,
            'status': 'Failed',
            'status_description': 'System name is required'
        })
    if not system_acronym.strip():
        return jsonify({
            'req_id': None,
            'status': 'Failed',
            'status_description': 'System acronym is required'
        })

    sts = "Success"
    sts_description = "System name added successfully"
    system_id = None
    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_insert_query = """INSERT INTO INTEGRATION_SYSTEMS (CORPORATE_ACCOUNT, PROJECT_ID, SYSTEM_ID, SYSTEM_NAME, SYSTEM_DESCRIPTION, SYSTEM_ACRONYM, STATUS, CREATED_DATE, UPDATED_DATE)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) """

        system_id, seq_status, seq_status_description  = generate_next_sequence(corporate_account, project_id, 'SYSTEM_ID')

        if seq_status == "Failed":
            sts = "Failed"
            sts_description = seq_status_description
            return jsonify({
                'system_id': system_id,
                'status': sts,
                'status_description': sts_description
            })
        record = (corporate_account, project_id, system_id, system_name, system_description,system_acronym, 'Active', datetime.now(), datetime.now())
        cursor.execute(mySql_insert_query, record)
        connection.commit()

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to add the new system: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'system_id': system_id,
        'status': sts,
        'status_description': sts_description
    })



@integration_requirements_blueprint.route('/api/update_integration_system_OLD', methods=['PUT', 'POST'])
@token_required
@validate_access
def update_integration_system_OLD(current_user):

    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    system_id = data.get('system_id')
    system_name = data.get('system_name')
    system_description = data.get('system_description')
    system_acronym = data.get('system_acronym')
    status = data.get('status', 'Active')


    sts = "Success"
    sts_description = "Integration system details updated successfully"
    rows_impacted = 0



    if not validate_corporate_account(corporate_account):
        return jsonify({
            'req_id': None,
            'status': 'Failed',
            'status_description': 'Corporate account is not valid'
        })
    if not validate_project_id(corporate_account, project_id):
        return jsonify({
            'req_id': None,
            'status': 'Failed',
            'status_description': 'Project Id is not valid'
        })
    if not system_name.strip():
        return jsonify({
            'req_id': None,
            'status': 'Failed',
            'status_description': 'System name is required'
        })
    if not system_acronym.strip():
        return jsonify({
            'req_id': None,
            'status': 'Failed',
            'status_description': 'System acronym is required'
        })
    if not validate_status(corporate_account, project_id, 'INTEGRATION_SYSTEM', status):
        return jsonify({
            'system_id': system_id,
            'status': 'Failed',
            'status_description': 'Invalid status',
            'rows_impacted': rows_impacted
        })

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_update_query = """UPDATE INTEGRATION_SYSTEMS SET SYSTEM_NAME = %s, SYSTEM_DESCRIPTION = %s, SYSTEM_ACRONYM = %s, STATUS = %s, UPDATED_DATE = %s WHERE SYSTEM_ID = %s AND CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s"""
        record = (system_name, system_description, system_acronym, status, datetime.now(), system_id, corporate_account, project_id )
        cursor.execute(mySql_update_query, record)
        connection.commit()
        rows_impacted = cursor.rowcount
        if rows_impacted == 0:
            sts = "Failed"
            sts_description = "No matching system found to update"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to update the system details: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'system_id': system_id,
        'status': sts,
        'status_description': sts_description,
        'rows_impacted': rows_impacted

    })



@integration_requirements_blueprint.route('/api/get_integration_systems_list_OLD', methods=['GET','POST'])
@token_required
def get_integration_systems_list_OLD(current_user):

    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    search_query = data.get('search_query')
    sort_criteria = data.get('sort_criteria')
    filter_by_status = data.get('filter_by_status', [])

    logging.info(f" corporate_account: {corporate_account} project_id: {project_id} search_query: {search_query} sort_criteria: {sort_criteria} filter_by_status: {filter_by_status}")


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

    if not filter_by_status or not isinstance(filter_by_status, list):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Filter by status must be provided as an array'
        })

    sts = "Success"
    sts_description = "Integration systems list retrieved successfully"
    integration_system_details = {}
    integration_systems_list = []
    placeholders = ','.join(['%s'] * len(filter_by_status))
    if not sort_criteria:
        sort_criteria = 'SYSTEM_NAME ASC'  # Default sort criteria if not provided


    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        mySql_select_query = f"""SELECT A.SYSTEM_ID, A.SYSTEM_NAME, A.SYSTEM_DESCRIPTION, A.SYSTEM_ACRONYM, A.STATUS,
        A.CREATED_DATE, A.UPDATED_DATE FROM INTEGRATION_SYSTEMS A WHERE A.CORPORATE_ACCOUNT = %s AND A.PROJECT_ID = %s 
        AND A.STATUS IN({placeholders}) """
        record = [corporate_account, project_id]
        record.extend(filter_by_status)

        if search_query:
            mySql_select_query += " AND ( A.SYSTEM_NAME LIKE %s OR A.SYSTEM_DESCRIPTION LIKE %s OR A.SYSTEM_ACRONYM LIKE %s) ORDER BY " + sort_criteria
            record.extend([f"%{search_query}%", f"%{search_query}%", f"%{search_query}%"])

        else:
                mySql_select_query += " ORDER BY " + sort_criteria

        cursor.execute(mySql_select_query, record)

        logging.info(f" executed SQL is: {cursor._executed}")


        for result in cursor.fetchall():

            integration_system_details = {
                'system_id': result[0],
                'system_name': result[1],
                'system_description': result[2],
                'system_acronym': result[3],
                'status': result[4],
                'created_date': result[5],
                'updated_date': result[6]
            }
            integration_systems_list.append(integration_system_details)


        if len(integration_systems_list) == 0:
            sts = "Failed"
            sts_description = "No matching integration systems found"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the integration system details: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'integration_systems_list': integration_systems_list,
        'status': sts,
        'status_description': sts_description
    })





@integration_requirements_blueprint.route('/api/get_integration_system_details_OLD', methods=['GET'])
@token_required
@validate_access
def get_integration_system_details_OLD(current_user):

    data = request.json
    system_id = data.get('system_id')
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')

    sts = "Success"
    sts_description = "Integration system details retrieved successfully"
    integration_system_details = {}

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_select_query = """SELECT SYSTEM_ID, CORPORATE_ACCOUNT, PROJECT_ID, SYSTEM_NAME, SYSTEM_DESCRIPTION, SYSTEM_ACRONYM, STATUS, CREATED_DATE, UPDATED_DATE 
        FROM INTEGRATION_SYSTEMS WHERE SYSTEM_ID = %s AND CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s"""
        record = (system_id, corporate_account, project_id)


        cursor.execute(mySql_select_query, record)
        result = cursor.fetchone()

        if result:
            integration_system_details = {
                'system_id': result[0],
                'corporate_account': result[1],
                'project_id': result[2],
                'system_name': result[3],
                'system_description': result[4],
                'system_acrnonym': result[5],
                'status': result[6],
                'created_date': result[7],
                'updated_date': result[8]
            }
        else:
            sts = "Failed"
            sts_description = "No matching system found"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the system details: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'integration_system_details': integration_system_details,
        'status': sts,
        'status_description': sts_description
    })




@integration_requirements_blueprint.route('/api/delete_integration_system_OLD', methods=['PUT', 'POST'])
@token_required
@validate_access
def delete_integration_system_OLD(current_user):

    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    system_ids = data.get('system_ids')

    logging.info(f" corporate_account: {corporate_account} project_id: {project_id} system_id: {system_ids}")

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

    sts = "Success"
    sts_description = "Integration system successfully deleted"
    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        mySql_insert_query = """DELETE FROM INTEGRATION_SYSTEMS   
        WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND SYSTEM_ID = %s """

        record = (corporate_account, project_id, system_id)
        cursor.execute(mySql_insert_query, record)
        logging.info(f" Executed SQL is: {cursor._executed}")
        connection.commit()
        rows_impacted = cursor.rowcount
        if rows_impacted == 0:
            sts = "Failed"
            sts_description = "No matching integration system found to delete"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to delete the integration system: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'status': sts,
        'status_description': sts_description
    })



@integration_requirements_blueprint.route('/api/create_integration_system', methods=['POST'])
@token_required
@validate_access
def create_integration_system(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    system_name = data.get('system_name')
    system_description = data.get('system_description')
    system_acronym = data.get('system_acronym')

    logging.info( f" corporate_account: {corporate_account} project_id: {project_id} system_name: {system_name} system_description: {system_description} system_acronym: {system_acronym}")

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

    if not system_name.strip():
        return jsonify({
            'status': 'Failed',
            'status_description': 'System name is required'
        })

    sts = "Success"
    sts_description = "Integration system added successfully"
    system_id = None

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_insert_query = """INSERT INTO INTEGRATION_SYSTEMS (CORPORATE_ACCOUNT, PROJECT_ID, SYSTEM_ID, SYSTEM_NAME, SYSTEM_DESCRIPTION, SYSTEM_ACRONYM,
                    STATUS, CREATED_DATE, UPDATED_DATE)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) """

        system_id, seq_status, seq_status_description  = generate_next_sequence(corporate_account, project_id, 'INTEGRATION_SYSTEM')

        if seq_status == "Failed":
            sts = "Failed"
            sts_description = seq_status_description
            return jsonify({
                'status': sts,
                'status_description': sts_description
            })
        record = (corporate_account, project_id, system_id, system_name, system_description, system_acronym,  'Active', datetime.now(), datetime.now())
        logging.info("record = " + str(record))
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

        logging.error(f"Database error creating integration system: {error}")

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'system_id': system_id,
        'status': sts,
        'status_description': sts_description
    })




@integration_requirements_blueprint.route('/api/update_integration_system', methods=['PUT', 'POST'])
@token_required
@validate_access
def update_integration_system(current_user):
    data = request.json
    system_id = data.get('system_id')
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    system_name  = data.get('system_name')
    system_description = data.get('system_description')
    system_acronym = data.get('system_acronym')


    sts = "Success"
    sts_description = "Integration system updated successfully"
    rows_impacted = 0


    if not system_name.strip():
        return jsonify({
            'status': 'Failed',
            'status_description': 'System name is required'
        })


    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_update_query = """UPDATE INTEGRATION_SYSTEMS SET SYSTEM_NAME = %s, SYSTEM_DESCRIPTION = %s, SYSTEM_ACRONYM = %s, 
        UPDATED_DATE = %s WHERE SYSTEM_ID = %s AND CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s"""
        record = (system_name, system_description, system_acronym, datetime.now(), system_id, corporate_account, project_id )
        cursor.execute(mySql_update_query, record)
        connection.commit()
        rows_impacted = cursor.rowcount
        if rows_impacted == 0:
            sts = "Failed"
            sts_description = "No matching integration system found to update"

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


        sts_description = f"Failed to update the integration system: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'system_id': system_id,
        'status': sts,
        'status_description': sts_description,
        'rows_impacted': rows_impacted

    })




@integration_requirements_blueprint.route('/api/delete_integration_system', methods=['POST'])
@token_required
@validate_access
def delete_integration_system(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    system_ids = data.get('system_ids', [])

    if not validate_corporate_account(corporate_account):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Corporate account is not valid'
        })

    if not project_id:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Project Id is required'
        })

    if not validate_project_id(corporate_account, project_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Project Id is not valid'
        })

    if not system_ids or not isinstance(system_ids, list):
        return jsonify({
            'status': 'Failed',
            'status_description': 'System IDs must be provided as an array'
        })

    sts = "Success"
    sts_description = "Integration system(s) deleted successfully"
    deleted_count = 0
    connection = None

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        # Start transaction
        connection.start_transaction()

        # Prepare the SQL query with multiple placeholders
        placeholders = ','.join(['%s'] * len(system_ids))
        mySql_delete_query = f"""DELETE FROM INTEGRATION_SYSTEMS
            WHERE CORPORATE_ACCOUNT = %s 
            AND PROJECT_ID = %s 
            AND SYSTEM_ID IN ({placeholders})"""

        record = (corporate_account, project_id, *system_ids)

        cursor.execute(mySql_delete_query, record)
        deleted_count = cursor.rowcount

        if deleted_count == 0:
            sts = "Failed"
            sts_description = "No matching integration systems found to delete"
        else:
            sts_description = f"Successfully deleted {deleted_count} integration systems"

        # Commit the transaction
        connection.commit()

    except mysql.connector.Error as error:
        if connection:
            connection.rollback()  # Rollback in case of error
        sts = "Failed"
        sts_description = f"Failed to delete the integration systems : {error}"
        logging.info(error)

    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'status': sts,
        'status_description': sts_description,
        'deleted_count': deleted_count
    })

@integration_requirements_blueprint.route('/api/get_integration_system_details', methods=['GET','POST'])
@token_required
def get_integration_system_details(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    system_id = data.get('system_id')


    sts = "Success"
    sts_description = "Integration system details retrieved successfully"
    system_name = None
    system_description = None
    system_acronym = None
    status = None
    created_date = None
    updated_date = None

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_select_query = """SELECT SYSTEM_NAME, SYSTEM_DESCRIPTION, SYSTEM_ACRONYM, STATUS, CREATED_DATE, UPDATED_DATE FROM INTEGRATION_SYSTEMS
        WHERE SYSTEM_ID = %s AND CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s"""
        record = (system_id, corporate_account, project_id)

        cursor.execute(mySql_select_query, record)
        result = cursor.fetchone()

        if result:
                system_name = result[0]
                system_description = result[1]
                system_acronym = result[2]
                status = result[3]
                created_date = result[4]
                updated_date = result[5]
        else:
                sts = "Failed"
                sts_description = "No matching integration system found"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the integration system details: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'system_name': system_name,
        'system_description': system_description,
        'system_acronym': system_acronym,
        'system_status': status,
        'created_date': created_date,
        'updated_date': updated_date,
        'status': sts,
        'status_description': sts_description
    })




@integration_requirements_blueprint.route('/api/get_integration_systems_list', methods=['GET', 'POST'])
@token_required
def get_integration_systems_list(current_user):

    data = request.json
    corporate_account= data.get('corporate_account')
    project_id = data.get('project_id')
    search_query = data.get('search_query')

    logging.info(f"inside get_business_team_list corporate acc: {corporate_account}")
    logging.info(f"inside get_business_team_list project id: {project_id}")
    logging.info(f"inside get_business_team_list search text: {search_query}")


    sts = "Success"
    sts_description = "Integration systems list retrieved successfully"
    integration_system_details = {}
    integration_systems_list = []

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_select_query = """SELECT SYSTEM_ID, SYSTEM_NAME, SYSTEM_DESCRIPTION, SYSTEM_ACRONYM, STATUS, CREATED_DATE, UPDATED_DATE FROM INTEGRATION_SYSTEMS
        WHERE STATUS = %s AND CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s """

        if search_query:
            mySql_select_query += " AND (SYSTEM_NAME LIKE %s OR SYSTEM_DESCRIPTION LIKE %s OR SYSTEM_ACRONYM LIKE %s)"
            record = ('Active', corporate_account, project_id, f"%{search_query}% ", f"%{search_query}%", f"%{search_query}%")
        else:
            record = ('Active', corporate_account, project_id)

        cursor.execute(mySql_select_query, record)
        for result in cursor.fetchall():

            integration_system_details = {
                'system_id': result[0],
                'system_name': result[1],
                'system_description': result[2],
                'system_acronym': result[3],
                'system_status': result[4],
                'created_date': result[5],
                'updated_date': result[6]
            }
            integration_systems_list.append(integration_system_details)

        if len(integration_systems_list) == 0:
            sts = "Failed"
            sts_description = "No matching integration systems found"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve integration system details: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'integration_systems_list': integration_systems_list,
        'status': sts,
        'status_description': sts_description
    })
# ********************Integration Requirements************************


@integration_requirements_blueprint.route('/api/create_integration_requirement', methods=['POST'])
@token_required
@validate_access
def create_integration_requirement(current_user):

    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    level_id = data.get('level_id')
    integration_name = data.get('integration_name')
    integration_description = data.get('integration_description')
    source_or_provider_system_id = data.get('source_or_provider_system_id')
    status = data.get('status')
    integration_criticality = data.get('integration_criticality')
    integration_priority = data.get('integration_priority')
    ref_field_1 = data.get('ref_field_1')
    ref_field_2 = data.get('ref_field_2')
    ref_field_3 = data.get('ref_field_3')
    ref_field_4 = data.get('ref_field_4')
    pattern = data.get('pattern')
    type = data.get('type')
    frequency = data.get('frequency')
    middleware = data.get('middleware')
    triggers_how = data.get('triggers_how')
    source_data_format = data.get('source_data_format')
    data_transfer_protocol = data.get('data_transfer_protocol')
    authentication = data.get('authentication')
    logging_reqs = data.get('logging_reqs')
    monitoring = data.get('monitoring')
    error_handling = data.get('error_handling')
    performance = data.get('performance')
    failover = data.get('failover')
    endpoints = data.get('endpoints')


    if not validate_corporate_account(corporate_account):
        return jsonify({
            'integration_id': None,
            'status': 'Failed',
            'status_description': 'Corporate account is not valid'
        })
    if not validate_project_id(corporate_account, project_id):
        return jsonify({
            'integration_id': None,
            'status': 'Failed',
            'status_description': 'Project Id is not valid'
        })
    if not validate_level_id(corporate_account, project_id, level_id):
        return jsonify({
            'req_id': None,
            'status': 'Failed',
            'status_description': 'Level Id is not valid'
        })

    if not integration_name.strip():
        return jsonify({
            'integration_id': None,
            'status': 'Failed',
            'status_description': 'Integration name is required'
        })

    if not validate_integration_system_id(corporate_account, project_id, source_or_provider_system_id):
        return jsonify({
            'system_id': None,
            'status': 'Failed',
            'status_description': 'Source/provider system Id is not valid'
        })

    if not status.strip():
        return jsonify({
            'req_id': None,
            'status': 'Failed',
            'status_description': 'Integration requirement status is required'
        })

    if not validate_status(corporate_account, project_id, 'REQUIREMENT', status):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Invalid integration requirement status'
        })

    if not integration_criticality.strip():
        return jsonify({
            'req_id': None,
            'status': 'Failed',
            'status_description': 'Integration requirement criticality is required'
        })

    if not validate_status(corporate_account, project_id, 'REQUIREMENT_CRITICALITY', integration_criticality):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Invalid integration requirement criticality'
        })


    if integration_priority and not validate_status(corporate_account, project_id, 'REQUIREMENT_PRIORITY', integration_priority):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Invalid integration priority'
        })


    sts = "Success"
    sts_description = "Integration requirement added successfully"
    system_id = None
    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()




        mySql_select_query = """SELECT PROJECT_PREFIX FROM CORPORATE_ACCOUNT_PROJECTS WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s"""
        record = (corporate_account, project_id)
        cursor.execute(mySql_select_query, record)
        result = cursor.fetchone()
        project_prefix = None
        if result:
            project_prefix = result[0]
        if project_prefix is None:
            return jsonify({
                'status': 'Failed',
                'status_description': 'Requirement prefix not defined'
            })




        mySql_insert_query = """INSERT INTO INTEGRATION_REQUIREMENTS (CORPORATE_ACCOUNT, PROJECT_ID, LEVEL_ID, INTEGRATION_ID, INTEGRATION_ID_WITH_PREFIX, INTEGRATION_NAME, 
        INTEGRATION_DESCRIPTION, SOURCE_OR_PROVIDER_SYSTEM_ID, STATUS, INTEGRATION_CRITICALITY, INTEGRATION_PRIORITY, REF_FIELD_1, REF_FIELD_2, REF_FIELD_3, REF_FIELD_4,
        CREATED_DATE, UPDATED_DATE,
        PATTERN,
        TYPE,
        FREQUENCY, 
        MIDDLEWARE,
        TRIGGERS_HOW,
        SOURCE_DATA_FORMAT, 
        DATA_TRANSFER_PROTOCOL,
        AUTHENTICATION, 
        LOGGING_REQS, 
        MONITORING, 
        ERROR_HANDLING, 
        PERFORMANCE,
        FAILOVER, ENDPOINTS

        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s , %s, %s,  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """

        integration_id, seq_status, seq_status_description  = generate_next_sequence(corporate_account, project_id, 'REQUIREMENT')

        if seq_status == "Failed":
            sts = "Failed"
            sts_description = seq_status_description
            return jsonify({
                'integration_id': integration_id,
                'status': sts,
                'status_description': sts_description
            })

        integration_id_with_prefix = f"{project_prefix.strip()}-{integration_id}"

        record = (corporate_account, project_id, level_id, integration_id, integration_id_with_prefix, integration_name,
        integration_description, source_or_provider_system_id, status,integration_criticality, integration_priority, ref_field_1, ref_field_2,ref_field_3,ref_field_4, datetime.now(), datetime.now(),
                  pattern,
                  type,
                  frequency,
                  middleware,
                  triggers_how,
                  source_data_format,
                  data_transfer_protocol,
                  authentication,
                  logging_reqs,
                  monitoring,
                  error_handling,
                  performance,
                  failover,
                  endpoints
                 )
        cursor.execute(mySql_insert_query, record)
        logging.info(f" Executed SQL is: {cursor._executed}")





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

        sts_description = f"Failed to add the new integration requirement: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'integration_id': integration_id_with_prefix,
        'status': sts,
        'status_description': sts_description
    })


@integration_requirements_blueprint.route('/api/update_integration_requirement', methods=['PUT', 'POST'])
@token_required
@validate_access
def update_integration_requirement(current_user):

    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    integration_id = data.get('integration_id')
    integration_name = data.get('integration_name')
    integration_description = data.get('integration_description')
    source_or_provider_system_id = data.get('source_or_provider_system_id')
    status = data.get('status')
    integration_criticality = data.get('integration_criticality')
    integration_priority = data.get('integration_priority')
    ref_field_1 = data.get('ref_field_1')
    ref_field_2 = data.get('ref_field_2')
    ref_field_3 = data.get('ref_field_3')
    ref_field_4 = data.get('ref_field_4')
    pattern = data.get('pattern')
    type = data.get('type')
    frequency = data.get('frequency')
    middleware = data.get('middleware')
    triggers_how = data.get('triggers_how')
    source_data_format = data.get('source_data_format')
    data_transfer_protocol = data.get('data_transfer_protocol')
    authentication = data.get('authentication')
    logging_reqs = data.get('logging_reqs')
    monitoring = data.get('monitoring')
    error_handling = data.get('error_handling')
    performance = data.get('performance')
    failover = data.get('failover')
    endpoints = data.get('endpoints')

    sts = "Success"
    sts_description = "Integration requirement details updated successfully"
    rows_impacted = 0


    if not validate_corporate_account(corporate_account):
        return jsonify({
            'req_id': None,
            'status': 'Failed',
            'status_description': 'Corporate account is not valid'
        })
    if not validate_project_id(corporate_account, project_id):
        return jsonify({
            'req_id': None,
            'status': 'Failed',
            'status_description': 'Project Id is not valid'
        })

    if not integration_name.strip():
        return jsonify({
            'integration_id': None,
            'status': 'Failed',
            'status_description': 'Integration name is required'
        })

    if not validate_integration_system_id(corporate_account, project_id, source_or_provider_system_id):
        return jsonify({
            'system_id': None,
            'status': 'Failed',
            'status_description': 'Source/provider system Id is not valid'
        })

    if not integration_criticality.strip():
        return jsonify({
            'status': 'Failed',
            'status_description': 'Integration requirement criticality is required'
        })

    if not validate_status(corporate_account, project_id, 'REQUIREMENT', status):
        return jsonify({
            'integration_id': integration_id,
            'status': 'Failed',
            'status_description': 'Invalid status',
            'rows_impacted': rows_impacted
        })

    if not validate_status(corporate_account, project_id, 'REQUIREMENT_CRITICALITY', integration_criticality):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Invalid integration requirement criticality'
        })


    if integration_priority and not validate_status(corporate_account, project_id, 'REQUIREMENT_PRIORITY', integration_priority):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Invalid integration priority'
        })




    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()



        mySql_update_query = """UPDATE INTEGRATION_REQUIREMENTS SET INTEGRATION_NAME = %s, INTEGRATION_DESCRIPTION = %s, SOURCE_OR_PROVIDER_SYSTEM_ID = %s, 
        STATUS = %s, UPDATED_DATE = %s ,
        INTEGRATION_CRITICALITY = %s,
        INTEGRATION_PRIORITY = %s,
        REF_FIELD_1 = %s ,
        REF_FIELD_2 = %s ,
        REF_FIELD_3 = %s ,
        REF_FIELD_4 = %s ,
        PATTERN = %s ,
        TYPE = %s ,
        FREQUENCY = %s ,
        MIDDLEWARE = %s ,
        TRIGGERS_HOW = %s ,
        SOURCE_DATA_FORMAT = %s ,
        DATA_TRANSFER_PROTOCOL = %s ,
        AUTHENTICATION = %s ,
        LOGGING_REQS = %s ,
        MONITORING = %s ,
        ERROR_HANDLING = %s ,
        PERFORMANCE = %s ,
        FAILOVER = %s ,
        ENDPOINTS = %s 
        WHERE INTEGRATION_ID = %s AND CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s"""
        record = (integration_name, integration_description, source_or_provider_system_id, status, datetime.now(),
                  integration_criticality,
                    integration_priority,
                  ref_field_1  ,
                  ref_field_2  ,
                  ref_field_3  ,
                  ref_field_4  ,
                  pattern  ,
                  type  ,
                  frequency  ,
                  middleware  ,
                  triggers_how ,
                  source_data_format  ,
                  data_transfer_protocol ,
                  authentication ,
                  logging_reqs  ,
                  monitoring  ,
                  error_handling ,
                  performance ,
                  failover  ,
                  endpoints ,
                  integration_id, corporate_account, project_id )
        cursor.execute(mySql_update_query, record)

        rows_impacted = cursor.rowcount
        if rows_impacted == 0:
            sts = "Failed"
            sts_description = "No matching integration requirement found to update"



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
        'integration_id': integration_id,
        'status': sts,
        'status_description': sts_description,
        'rows_impacted': rows_impacted
    })


@integration_requirements_blueprint.route('/api/copy_integration_requirement', methods=['PUT', 'POST'])
@token_required
@validate_access
def copy_integration_requirement(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    from_project_id = data.get('from_project_id')
    from_integration_id = data.get('from_integration_id')
    to_project_id = data.get('to_project_id')
    to_level_id = data.get('to_level_id')
    to_status = data.get('to_status')
    copy_attachments = data.get('copy_attachments', False)  # New parameter for attachment copying

    if not validate_corporate_account(corporate_account):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Corporate account is not valid'
        })
    if not validate_project_id(corporate_account, from_project_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'From-Project Id is not valid'
        })

    if not validate_project_id(corporate_account, to_project_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'To-Project Id is not valid'
        })

    if not from_integration_id:
        return jsonify({
            'status': 'Failed',
            'status_description': 'From-Integration requirement Id is required'
        })

    if not validate_integration_id(corporate_account, from_project_id, from_integration_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'From-Integration requirement Id is not valid'
        })

    if not validate_status(corporate_account, to_project_id, 'REQUIREMENT', to_status):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Invalid status under the target project'
        })

    sts = "Success"
    sts_description = "Integration successfully copied to the target project"
    attachment_count = 0  # Initialize attachment count
    to_integration_id = None
    to_integration_id_with_prefix = None

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor(dictionary=True)  # Changed to dictionary cursor for consistency

        # Check if source integration exists
        check_query = """SELECT INTEGRATION_NAME FROM INTEGRATION_REQUIREMENTS WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s  
        AND INTEGRATION_ID = %s"""
        check_params = (corporate_account, from_project_id, from_integration_id)

        cursor.execute(check_query, check_params)
        result = cursor.fetchone()

        if result:
            # Generate new integration ID
            to_integration_id, seq_status, seq_status_description = generate_next_sequence(corporate_account,
                                                                                           to_project_id, 'REQUIREMENT')

            if seq_status == "Failed":
                sts = "Failed"
                sts_description = seq_status_description
                return jsonify({
                    'status': sts,
                    'status_description': sts_description
                })

            # Get project prefix
            prefix_query = """SELECT PROJECT_PREFIX FROM CORPORATE_ACCOUNT_PROJECTS WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s"""
            prefix_params = (corporate_account, to_project_id)

            cursor.execute(prefix_query, prefix_params)
            prefix_result = cursor.fetchone()

            project_prefix = None
            if prefix_result:
                project_prefix = prefix_result['PROJECT_PREFIX']

            if project_prefix is None:
                return jsonify({
                    'integration_id': None,
                    'status': 'Failed',
                    'status_description': 'Requirement prefix not defined'
                })

            # Create complete integration ID with prefix
            to_integration_id_with_prefix = f"{project_prefix.strip()}-{to_integration_id}"

            # Insert the new integration requirement
            insert_query = """INSERT INTO INTEGRATION_REQUIREMENTS 
                (CORPORATE_ACCOUNT, PROJECT_ID, LEVEL_ID,
                INTEGRATION_ID,  
                INTEGRATION_ID_WITH_PREFIX,
                INTEGRATION_NAME,
                INTEGRATION_DESCRIPTION,
                SOURCE_OR_PROVIDER_SYSTEM_ID,
                STATUS,
                INTEGRATION_CRITICALITY,
                INTEGRATION_PRIORITY,
                REF_FIELD_1,
                REF_FIELD_2,
                REF_FIELD_3,
                REF_FIELD_4,
                CREATED_DATE,
                UPDATED_DATE,
               PATTERN,
                TYPE,
               FREQUENCY,
               MIDDLEWARE,
                TRIGGERS_HOW,
                SOURCE_DATA_FORMAT,
                DATA_TRANSFER_PROTOCOL,
               AUTHENTICATION,
               ENDPOINTS,
              LOGGING_REQS,
              MONITORING,
              ERROR_HANDLING,
              PERFORMANCE,
              FAILOVER)
            SELECT 
                CORPORATE_ACCOUNT, 
                %s,   
                %s, 
                %s,  
                %s,
                INTEGRATION_NAME,
                INTEGRATION_DESCRIPTION,
                SOURCE_OR_PROVIDER_SYSTEM_ID,
                %s,
                INTEGRATION_CRITICALITY,
                INTEGRATION_PRIORITY,
                REF_FIELD_1,
                REF_FIELD_2,
                REF_FIELD_3,
                REF_FIELD_4,
                NOW(),
                NOW(),
               PATTERN,
                TYPE,
               FREQUENCY,
               MIDDLEWARE,
                TRIGGERS_HOW,
                SOURCE_DATA_FORMAT,
                DATA_TRANSFER_PROTOCOL,
               AUTHENTICATION,
               ENDPOINTS,
              LOGGING_REQS,
              MONITORING,
              ERROR_HANDLING,
              PERFORMANCE,
              FAILOVER
            FROM INTEGRATION_REQUIREMENTS
            WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND INTEGRATION_ID = %s"""

            insert_params = (
                to_project_id,
                to_level_id,
                to_integration_id,
                to_integration_id_with_prefix,
                to_status,
                corporate_account,
                from_project_id,
                from_integration_id)

            cursor.execute(insert_query, insert_params)
            connection.commit()

            # Copy key attributes
            key_attributes_query = """INSERT INTO KEY_ATTRIBUTES_LIST_REQUIREMENTS(CORPORATE_ACCOUNT, PROJECT_ID, REQ_ID, LEVEL_ID, KEY_ATTRIBUTE_LIST_ID,
            INCLUDE_EXCLUDE, CREATED_DATE, UPDATED_DATE)
            SELECT CORPORATE_ACCOUNT, %s, %s, LEVEL_ID, KEY_ATTRIBUTE_LIST_ID,
            INCLUDE_EXCLUDE, NOW(), NOW() FROM KEY_ATTRIBUTES_LIST_REQUIREMENTS
            WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND REQ_ID = %s"""

            key_attributes_params = (
                to_project_id,
                to_integration_id,
                corporate_account,
                from_project_id,
                from_integration_id)

            cursor.execute(key_attributes_query, key_attributes_params)
            connection.commit()

            # Copy integration fields
            fields_query = """INSERT INTO INTEGRATION_REQUIREMENTS_FIELDS (CORPORATE_ACCOUNT, PROJECT_ID, TARGET_OR_CONSUMER_SYSTEM_ID, SYSTEM_TYPE, FIELD_NAME,
            FIELD_DESCRIPTION, INTEGRATION_ID, FIELD_DATA_TYPE, FIELD_SIZE, FIELD_OPTIONAL_OR_MANDATORY,
            FIELD_TRANSFORMATION, FIELD_DATA_VALIDATION, FIELD_DATA_SECURITY, STATUS, CREATED_DATE, UPDATED_DATE) 
            SELECT CORPORATE_ACCOUNT, %s, TARGET_OR_CONSUMER_SYSTEM_ID, SYSTEM_TYPE, FIELD_NAME,
            FIELD_DESCRIPTION, %s, FIELD_DATA_TYPE, FIELD_SIZE, FIELD_OPTIONAL_OR_MANDATORY,
            FIELD_TRANSFORMATION, FIELD_DATA_VALIDATION, FIELD_DATA_SECURITY, STATUS, NOW(), NOW() 
            FROM INTEGRATION_REQUIREMENTS_FIELDS 
            WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND INTEGRATION_ID = %s """

            fields_params = (
                to_project_id,
                to_integration_id,
                corporate_account,
                from_project_id,
                from_integration_id)

            cursor.execute(fields_query, fields_params)
            connection.commit()

            # Copy consumers
            consumers_query = """INSERT INTO INTEGRATION_REQUIREMENTS_CONSUMERS(CORPORATE_ACCOUNT, PROJECT_ID, INTEGRATION_ID,
            TARGET_OR_CONSUMER_SYSTEM_ID, CONSUMER_DESCRIPTION, INTEGRATION_TYPE, STATUS, CREATED_DATE, UPDATED_DATE, TARGET_DATA_FORMAT)
            SELECT CORPORATE_ACCOUNT, %s, %s, TARGET_OR_CONSUMER_SYSTEM_ID, CONSUMER_DESCRIPTION,
            INTEGRATION_TYPE, STATUS, NOW(), NOW(), TARGET_DATA_FORMAT FROM INTEGRATION_REQUIREMENTS_CONSUMERS
            WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND INTEGRATION_ID = %s"""

            consumers_params = (
                to_project_id,
                to_integration_id,
                corporate_account,
                from_project_id,
                from_integration_id)

            cursor.execute(consumers_query, consumers_params)
            connection.commit()

            # Handle attachment copying if requested
            if copy_attachments:
                logging.info(
                    f"Copying attachments for integration requirement from {from_integration_id} to {to_integration_id}")

                # Get existing attachments
                attachments_query = """
                    SELECT * FROM REQUIREMENT_ATTACHMENTS
                    WHERE CORPORATE_ACCOUNT = %s
                    AND PROJECT_ID = %s
                    AND REQ_ID = %s
                """
                cursor.execute(attachments_query, (corporate_account, from_project_id, from_integration_id))
                original_attachments = cursor.fetchall()
                attachment_count = len(original_attachments)

                logging.info(f"Found {attachment_count} attachments to copy")

                # Process each attachment
                for attachment in original_attachments:
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
                    target_rel_dir = f"{corporate_account}/{to_project_id}/{to_integration_id}"
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
                        attachment_insert_query = """
                            INSERT INTO REQUIREMENT_ATTACHMENTS 
                            (CORPORATE_ACCOUNT, PROJECT_ID, REQ_ID, FILE_NAME, FILE_PATH, FILE_SIZE, FILE_TYPE, UPLOADED_BY)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        attachment_params = (
                            corporate_account,
                            to_project_id,
                            to_integration_id,
                            original_filename,
                            target_rel_path,
                            attachment['FILE_SIZE'],
                            attachment['FILE_TYPE'],
                            current_user['user_id']
                        )

                        cursor.execute(attachment_insert_query, attachment_params)
                        connection.commit()

                        logging.info(f"Database record created for copied attachment")

                    except Exception as file_error:
                        logging.error(f"Error copying file: {str(file_error)}")

        else:
            sts = "Failed"
            sts_description = "No matching integration found to copy from"

    except mysql.connector.Error as error:
        sts = "Failed"
        if hasattr(error, 'errno') and error.errno == 1062:  # MySQL error code for duplicate entry
            record_info = f"corporate_account: {corporate_account}, project: {to_project_id}, integration: {to_integration_id}"
            sts_description = f"Error: Attempt to create duplicate record: ({record_info})"
        else:
            sts_description = f"Failed to copy the integration requirement: {error}"
        logging.error(f"Database error copying integration requirement: {error}")

    except Exception as e:
        sts = "Failed"
        sts_description = f"Failed to copy the integration requirement: {str(e)}"
        logging.error(f"Error copying integration requirement: {str(e)}")

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'integration_id_with_prefix': to_integration_id_with_prefix,
        'integration_id': to_integration_id,
        'status': sts,
        'status_description': sts_description,
        'attachments_copied': copy_attachments,
        'attachment_count': attachment_count
    })

@integration_requirements_blueprint.route('/api/add_integration_requirement_consumer', methods=['POST'])
@token_required
@validate_access
def add_integration_requirement_consumer(current_user):

    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    integration_id = data.get('integration_id')
    target_or_consumer_system_id = data.get('target_or_consumer_system_id')
    consumer_description = data.get('consumer_description')
    integration_type = data.get('integration_type')
    target_data_format   = data.get('target_data_format')
    status = data.get('status', 'Created')

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
    if not validate_integration_id (corporate_account, project_id, integration_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Integration requirement Id is not valid'
        })
    if not validate_integration_system_id(corporate_account, project_id, target_or_consumer_system_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Target/consumer system Id is not valid'
        })

    if not validate_status(corporate_account, project_id, 'REQUIREMENT', status):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Invalid status',
        })

    sts = "Success"
    sts_description = "System added as consumer/target successfully"
    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()


        mySql_insert_query = """INSERT INTO INTEGRATION_REQUIREMENTS_CONSUMERS (CORPORATE_ACCOUNT, PROJECT_ID, INTEGRATION_ID, TARGET_OR_CONSUMER_SYSTEM_ID, 
        CONSUMER_DESCRIPTION, INTEGRATION_TYPE, TARGET_DATA_FORMAT, STATUS, CREATED_DATE, UPDATED_DATE) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
        record = (corporate_account, project_id, integration_id, target_or_consumer_system_id, consumer_description, integration_type, target_data_format, status, datetime.now(), datetime.now())
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

@integration_requirements_blueprint.route('/api/delete_integration_requirement_consumer', methods=['PUT', 'POST'])
@token_required
@validate_access
def delete_integration_requirement_consumer(current_user):

    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    integration_id = data.get('integration_id')
    target_or_consumer_system_id = data.get('target_or_consumer_system_id')

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
    if not validate_integration_id(corporate_account, project_id, integration_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Integration Id is not valid'
        })

    sts = "Success"
    sts_description = "Target/consumer system successfully deleted for the requirement"
    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()


        mySql_insert_query = """DELETE FROM INTEGRATION_REQUIREMENTS_CONSUMERS 
        WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND INTEGRATION_ID = %s AND TARGET_OR_CONSUMER_SYSTEM_ID = %s """

        record = (corporate_account, project_id, integration_id, target_or_consumer_system_id)
        cursor.execute(mySql_insert_query, record)
        connection.commit()
        rows_impacted = cursor.rowcount
        if rows_impacted == 0:
            sts = "Failed"
            sts_description = "No matching target/consumer found to delete"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to delete the consumer/target for the integration: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'status': sts,
        'status_description': sts_description
    })


@integration_requirements_blueprint.route('/api/update_integration_requirement_consumer', methods=['PUT', 'POST'])
@token_required
@validate_access
def update_integration_requirement_consumer(current_user):

    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    integration_id = data.get('integration_id')
    target_or_consumer_system_id = data.get('target_or_consumer_system_id')
    consumer_description = data.get('consumer_description')
    integration_type = data.get('integration_type')
    status = data.get('status')
    target_data_format   = data.get('target_data_format')

    logging.info(f"INSIDE UPDATE CONSUMER DETAILS - 1: {data}")


    sts = "Success"
    sts_description = "Integration consumer details updated successfully"
    rows_impacted = 0

    if not validate_corporate_account(corporate_account):
        return jsonify({
            'req_id': None,
            'status': 'Failed',
            'status_description': 'Corporate account is not valid'
        })
    if not validate_project_id(corporate_account, project_id):
        return jsonify({
            'req_id': None,
            'status': 'Failed',
            'status_description': 'Project Id is not valid'
        })

    if not validate_integration_id(corporate_account, project_id, integration_id):
        return jsonify({
            'integration_id': None,
            'status': 'Failed',
            'status_description': 'Integration Id is not valid'
        })

    if not validate_integration_system_id(corporate_account, project_id, target_or_consumer_system_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Target/consumer system Id is not valid'
        })

    if not validate_status(corporate_account, project_id, 'REQUIREMENT', status):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Invalid status',
        })
    logging.info(f"INSIDE UPDATE CONSUMER DETAILS - 2: {data}")

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_update_query = """UPDATE INTEGRATION_REQUIREMENTS_CONSUMERS SET CONSUMER_DESCRIPTION = %s, INTEGRATION_TYPE = %s,  
        STATUS = %s, UPDATED_DATE = %s, TARGET_DATA_FORMAT = %s
        WHERE INTEGRATION_ID = %s AND CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND TARGET_OR_CONSUMER_SYSTEM_ID = %s"""
        record = (consumer_description, integration_type, status, datetime.now(), target_data_format, integration_id, corporate_account, project_id, target_or_consumer_system_id )
        cursor.execute(mySql_update_query, record)
        connection.commit()
        rows_impacted = cursor.rowcount
        if rows_impacted == 0:
            sts = "Failed"
            sts_description = "No matching integration consumer found to update"


        logging.info(f"INSIDE UPDATE CONSUMER DETAILS - 3: {data}")


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
    })





@integration_requirements_blueprint.route('/api/add_integration_requirement_field', methods=['POST'])
@token_required
@validate_access
def add_integration_requirement_field(current_user):

    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    system_id = data.get('system_id')
    system_type = data.get('system_type')
    field_name = data.get('field_name')
    field_description = data.get('field_description')
    integration_id = data.get('integration_id')
    field_data_type = data.get('field_data_type')
    field_size = data.get('field_size')
    field_optional_or_mandatory = data.get('field_optional_or_mandatory')
    field_transformation = data.get('field_transformation')
    field_data_validation = data.get('field_data_validation')
    field_data_security = data.get('field_data_security')
    status  = data.get('status')
    maps_to_provider_system_field_name = data.get('maps_to_provider_system_field_name', None)

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

    if system_type not in ['PROVIDER', 'CONSUMER']:
        return jsonify({
            'status': 'Failed',
            'status_description': 'System type must be either PROVIDER or CONSUMER'
        })

    if not validate_integration_system_id(corporate_account, project_id, system_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Provider/consumer system Id is not valid'
        })

    if not field_name.strip():
        return jsonify({
            'status': 'Failed',
            'status_description': 'Field name is required'
        })

    if not is_valid_field_name(field_name):
            return jsonify({
            'status': 'Failed',
            'status_description': 'Field name cannot contain special characters including space'
        })

    if integration_id and not validate_integration_id (corporate_account, project_id, integration_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Integration requirement Id is not valid'
        })

    if not integration_id:
        integration_id = 0

    if system_type == 'PROVIDER' and maps_to_provider_system_field_name:
        maps_to_provider_system_field_name = None  # Provider fields cannot map to another provider field

    sts2, sts_description = validate_integration_field(corporate_account, project_id,  field_name, integration_id, system_id, system_type)
    if not sts2:
        return jsonify({
            'status': 'Failed',
            'status_description': sts_description
        })



    sts = "Success"
    sts_description = "Integration field added successfully"
    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        mySql_insert_query = """INSERT INTO INTEGRATION_REQUIREMENTS_FIELDS (CORPORATE_ACCOUNT, PROJECT_ID, SYSTEM_ID, SYSTEM_TYPE, FIELD_NAME, FIELD_DESCRIPTION,
        INTEGRATION_ID, STATUS, CREATED_DATE, UPDATED_DATE,
        FIELD_DATA_TYPE,
        FIELD_SIZE,
        FIELD_OPTIONAL_OR_MANDATORY,
        FIELD_TRANSFORMATION,
        FIELD_DATA_VALIDATION,
        FIELD_DATA_SECURITY, MAPS_TO_PROVIDER_SYSTEM_FIELD_NAME
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,   %s, %s ,   %s, %s, %s) """
        record = (corporate_account, project_id, system_id, system_type, field_name, field_description, integration_id,   status, datetime.now(), datetime.now(),
                  field_data_type,
                  field_size,
                  field_optional_or_mandatory,
                  field_transformation,
                  field_data_validation,
                  field_data_security,
                  maps_to_provider_system_field_name
                  )
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


@integration_requirements_blueprint.route('/api/delete_integration_requirement_field', methods=['PUT', 'POST'])
@token_required
@validate_access
def delete_integration_requirement_field(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    field_name = data.get('field_name')
    integration_id = data.get('integration_id')
    system_id = data.get('system_id')
    system_type = data.get('system_type')

    # level_id = data.get('level_id')
    # req_id = data.get('req_id')

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

    if system_type not in ['PROVIDER', 'CONSUMER']:
        return jsonify({
            'status': 'Failed',
            'status_description': 'System type must be either PROVIDER or CONSUMER'
        })

    if not validate_integration_system_id(corporate_account, project_id, system_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Provider/consumer system Id is not valid'
        })

    if not field_name.strip():
        return jsonify({
            'status': 'Failed',
            'status_description': 'Field name is required'
        })

    if not is_valid_field_name(field_name):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Field name cannot contain special characters including space'
        })

    if integration_id and not validate_integration_id(corporate_account, project_id, integration_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Integration Id is not valid'
        })

    # if level_id and not validate_level_id(corporate_account, project_id, level_id):
    #     return jsonify({
    #         'status': 'Failed',
    #         'status_description': 'Level Id is not valid'
    #     })
    #
    # if req_id and not validate_req_id(corporate_account, project_id, req_id):
    #     return jsonify({
    #         'status': 'Failed',
    #         'status_description': 'Requirement Id is not valid'
    #     })

    if not integration_id:
        integration_id = 0

    # if not level_id:
    #     level_id = 0
    #
    # if not req_id:
    #     req_id = 0

    # if integration_id and level_id and req_id:
    #     return jsonify({
    #         'status': 'Failed',
    #         'status_description': 'Only one of integration level, functional level or requirement level can be provided'
    #     })
    # if integration_id and level_id:
    #     return jsonify({
    #         'status': 'Failed',
    #         'status_description': 'Only one of integration level, functional level or requirement level can be provided'
    #     })
    # if level_id and req_id:
    #     return jsonify({
    #         'status': 'Failed',
    #         'status_description': 'Only one of integration level, functional level or requirement level can be provided'
    #     })
    # if integration_id and req_id:
    #     return jsonify({
    #         'status': 'Failed',
    #         'status_description': 'Only one of integration level, functional level or requirement level  can be provided'
    #     })

    sts = "Success"
    sts_description = "Integration field successfully deleted"
    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        # mySql_insert_query = """delete from INTEGRATION_REQUIREMENTS_FIELDS where corporate_account = %s and project_id = %s and field_name = %s and integration_id = %s and level_id = %s and req_id = %s """
        # record = (corporate_account, project_id, field_name,  integration_id, level_id, req_id)

        mySql_insert_query = """DELETE FROM INTEGRATION_REQUIREMENTS_FIELDS 
        WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND FIELD_NAME = %s AND INTEGRATION_ID = %s AND SYSTEM_ID = %s AND SYSTEM_TYPE = %s """

        record = (corporate_account, project_id, field_name, integration_id, system_id, system_type)
        cursor.execute(mySql_insert_query, record)
        connection.commit()
        rows_impacted = cursor.rowcount
        if rows_impacted == 0:
            sts = "Failed"
            sts_description = "No matching integration field requirement found to delete"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to delete the integration field: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'status': sts,
        'status_description': sts_description
    })


@integration_requirements_blueprint.route('/api/update_integration_requirement_field_OLD', methods=['PUT', 'POST'])
@token_required
@validate_access
def update_integration_requirement_field_OLD(current_user):
    data = request.json

    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    field_name = data.get('field_name')
    field_description = data.get('field_description')
    integration_id = data.get('integration_id')
    level_id = data.get('level_id')
    req_id = data.get('req_id')
    status = data.get('status')

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

    if not field_name.strip():
        return jsonify({
            'status': 'Failed',
            'status_description': 'Field name is required'
        })

    if ' ' in field_name:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Field name cannot contain spaces'
        })

    if integration_id and not validate_integration_id(corporate_account, project_id, integration_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Integration requirement Id is not valid'
        })

    if level_id and not validate_level_id(corporate_account, project_id, level_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Level Id is not valid'
        })

    if req_id and not validate_req_id(corporate_account, project_id, req_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Requirement Id is not valid'
        })

    if not validate_status(corporate_account, project_id, 'REQUIREMENT', status):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Invalid status',
        })

    if not integration_id:
        integration_id = 0

    if not level_id:
        level_id = 0

    if not req_id:
        req_id = 0

    if integration_id and level_id and req_id:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Only one of integration Id, functional level or requirement ID can be specified'
        })
    if integration_id and level_id:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Only one of integration Id, functional level or requirement ID can be specified'
        })
    if level_id and req_id:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Only one of integration Id, functional level or requirement ID can be specified'
        })
    if integration_id and req_id:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Only one of integration Id, functional level or requirement ID can be specified'
        })

    sts = "Success"
    sts_description = "Integration field name updated successfully"

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_update_query = """UPDATE INTEGRATION_REQUIREMENTS_FIELDS SET FIELD_DESCRIPTION = %s, STATUS = %s, UPDATED_DATE = %s 
       WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND FIELD_NAME = %s AND LEVEL_ID = %s AND INTEGRATION_ID = %s AND REQ_ID = %s"""
        record = (
        field_description, status, datetime.now(), corporate_account, project_id, field_name, level_id, integration_id,
        req_id)
        cursor.execute(mySql_update_query, record)
        connection.commit()
        rows_impacted = cursor.rowcount
        if rows_impacted == 0:
            sts = "Failed"
            sts_description = "No matching field name found to update"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to update the integration field details: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'status': sts,
        'status_description': sts_description,
    })


@integration_requirements_blueprint.route('/api/update_integration_requirement_field', methods=['PUT', 'POST'])
@token_required
@validate_access
def update_integration_requirement_field(current_user):
    data = request.json

    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    system_id = data.get('system_id')
    system_type = data.get('system_type')
    field_name = data.get('field_name')
    field_description = data.get('field_description')
    integration_id = data.get('integration_id')
    field_data_type = data.get('field_data_type')
    field_size = data.get('field_size')
    field_optional_or_mandatory = data.get('field_optional_or_mandatory')
    field_transformation = data.get('field_transformation')
    field_data_validation = data.get('field_data_validation')
    field_data_security = data.get('field_data_security')
    status = data.get('status')
    maps_to_provider_system_field_name = data.get('maps_to_provider_system_field_name', None)

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

    if system_type not in ['PROVIDER', 'CONSUMER']:
        return jsonify({
            'status': 'Failed',
            'status_description': 'System type must be either PROVIDER or CONSUMER'
        })
    if not validate_integration_system_id(corporate_account, project_id, system_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Provider/consumer system Id is not valid'
        })

    if not field_name.strip():
        return jsonify({
            'status': 'Failed',
            'status_description': 'Field name is required'
        })

    if not is_valid_field_name(field_name):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Field name cannot contain special characters including space'
        })

    if integration_id and not validate_integration_id(corporate_account, project_id, integration_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Integration requirement Id is not valid'
        })

    if not validate_status(corporate_account, project_id, 'REQUIREMENT', status):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Invalid status',
        })

    if system_type == 'PROVIDER' and maps_to_provider_system_field_name:
        maps_to_provider_system_field_name = None  # Provider fields cannot map to another provider field

    if not integration_id:
        integration_id = 0

    sts = "Success"
    sts_description = "Integration field name updated successfully"

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_update_query = """UPDATE INTEGRATION_REQUIREMENTS_FIELDS SET FIELD_DESCRIPTION = %s, STATUS = %s, UPDATED_DATE = %s ,
        FIELD_DATA_TYPE = %s,
        FIELD_SIZE =  %s,
        FIELD_OPTIONAL_OR_MANDATORY =  %s,
        FIELD_TRANSFORMATION =   %s,
        FIELD_DATA_VALIDATION =   %s,
        FIELD_DATA_SECURITY =   %s, MAPS_TO_PROVIDER_SYSTEM_FIELD_NAME = %s
        WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND FIELD_NAME = %s AND INTEGRATION_ID = %s AND SYSTEM_ID = %s AND SYSTEM_TYPE = %s"""
        record = (field_description, status, datetime.now(),
                  field_data_type,
                  field_size,
                  field_optional_or_mandatory,
                  field_transformation,
                  field_data_validation,
                  field_data_security, maps_to_provider_system_field_name,
                  corporate_account, project_id, field_name, integration_id, system_id, system_type)
        cursor.execute(mySql_update_query, record)
        connection.commit()
        rows_impacted = cursor.rowcount
        if rows_impacted == 0:
            sts = "Failed"
            sts_description = "No matching field name found to update"

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
    })


@integration_requirements_blueprint.route('/api/copy_integration_requirement_field', methods=['PUT', 'POST'])
@token_required
@validate_access
def copy_integration_requirement_field(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    from_project_id = data.get('from_project_id')
    from_field_name = data.get('from_field_name')
    from_integration_id = data.get('from_integration_id')
    from_system_id = data.get('from_system_id')
    from_system_type = data.get('from_system_type')
    to_project_id = data.get('to_project_id')
    to_field_name = data.get('to_field_name')
    to_integration_id = data.get('to_integration_id')
    to_status = data.get('to_status')

    if not validate_corporate_account(corporate_account):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Corporate account is not valid'
        })
    if not validate_project_id(corporate_account, from_project_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'From-Project Id is not valid'
        })

    if not validate_project_id(corporate_account, to_project_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'To-Project Id is not valid'
        })

    if from_integration_id and not validate_integration_id(corporate_account, from_project_id, from_integration_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'From-Integration requirement Id is not valid'
        })

    if to_integration_id and not validate_integration_id(corporate_account, to_project_id, to_integration_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'To-Integration requirement Id is not valid'
        })

    if from_system_type not in ['PROVIDER', 'CONSUMER']:
        return jsonify({
            'status': 'Failed',
            'status_description': 'System type must be either PROVIDER or CONSUMER'
        })
    if not validate_integration_system_id(corporate_account, from_project_id, from_system_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Provider/consumer system Id is not valid'
        })

    if not from_field_name.strip():
        return jsonify({
            'status': 'Failed',
            'status_description': 'From-Field name is required'
        })

    if not to_field_name.strip():
        return jsonify({
            'status': 'Failed',
            'status_description': 'To-Field name is required'
        })

    if not is_valid_field_name(from_field_name):
        return jsonify({
            'status': 'Failed',
            'status_description': 'From-Field name cannot contain special characters including space'
        })

    if not is_valid_field_name(to_field_name):
        return jsonify({
            'status': 'Failed',
            'status_description': 'To-Field name cannot contain special characters including space'
        })

    if not validate_status(corporate_account, to_project_id, 'REQUIREMENT', to_status):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Invalid status under the target project'
        })

    if not from_integration_id:
        from_integration_id = 0

    if not to_integration_id:
        to_integration_id = 0

    sts = "Success"
    sts_description = "Integration field successfully copied"
    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        mySql_select_query = """SELECT FIELD_NAME FROM INTEGRATION_REQUIREMENTS_FIELDS WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND FIELD_NAME = %s 
        AND INTEGRATION_ID = %s AND SYSTEM_ID = %s AND SYSTEM_TYPE = %s"""
        record = (
        corporate_account, from_project_id, from_field_name, from_integration_id, from_system_id, from_system_type)

        cursor.execute(mySql_select_query, record)
        result = cursor.fetchone()

        if result:

            mySql_insert_query = """INSERT INTO INTEGRATION_REQUIREMENTS_FIELDS 
            (CORPORATE_ACCOUNT, PROJECT_ID, SYSTEM_ID, SYSTEM_TYPE, FIELD_NAME, FIELD_DESCRIPTION,
            INTEGRATION_ID,  
            FIELD_DATA_TYPE,
            FIELD_SIZE,
            FIELD_OPTIONAL_OR_MANDATORY,
            FIELD_TRANSFORMATION,
            FIELD_DATA_VALIDATION,
            FIELD_DATA_SECURITY, STATUS, CREATED_DATE, UPDATED_DATE, MAPS_TO_PROVIDER_SYSTEM_FIELD_NAME)
            SELECT CORPORATE_ACCOUNT, %s, SYSTEM_ID, SYSTEM_TYPE, %s, FIELD_DESCRIPTION,
            %s,  
            FIELD_DATA_TYPE,
            FIELD_SIZE,
            FIELD_OPTIONAL_OR_MANDATORY,
            FIELD_TRANSFORMATION,
            FIELD_DATA_VALIDATION,
            FIELD_DATA_SECURITY, %s, NOW(), NOW() , MAPS_TO_PROVIDER_SYSTEM_FIELD_NAME
            FROM INTEGRATION_REQUIREMENTS_FIELDS 
            WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND INTEGRATION_ID = %s AND FIELD_NAME = %s AND SYSTEM_ID = %s AND SYSTEM_TYPE = %s"""

            record = (to_project_id, to_field_name, to_integration_id, to_status, corporate_account, from_project_id,
                      from_integration_id, from_field_name, from_system_id, from_system_type)
            cursor.execute(mySql_insert_query, record)
            connection.commit()

        else:
            sts = "Failed"
            sts_description = "No matching integration field found to copy from"


    except mysql.connector.Error as error:
        sts = "Failed"
        if hasattr(error, 'errno') and error.errno == 1062:  # MySQL error code for duplicate entry
            field_info = f"corporate_account: {corporate_account}, project: {to_project_id}, field: {to_field_name}, integration: {to_integration_id}"
            sts_description = f"Error: Attempt to create duplicate record:  ({field_info})"
        else:
            sts_description = f"Failed to copy the integration field: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'status': sts,
        'status_description': sts_description
    })


@integration_requirements_blueprint.route('/api/get_integration_requirement_field_list', methods=['GET','POST'])
@token_required
def get_integration_requirement_field_list(current_user):

    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    system_id = data.get('system_id')
    system_type = data.get('system_type')
    integration_id = data.get('integration_id')
    search_query = data.get('search_query')
    sort_criteria = data.get('sort_criteria')
    filter_by_status = data.get('filter_by_status', [])

    logging.info(f"get integration field list : {data} ")

    if integration_id is None:
        logging.info("null value...")

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

    if system_type not in ['PROVIDER', 'CONSUMER']:
        return jsonify({
            'status': 'Failed',
            'status_description': 'System type must be either PROVIDER or CONSUMER'
        })
    if not validate_integration_system_id(corporate_account, project_id, system_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Provider/consumer system Id is not valid'
        })


    if integration_id is not None and not validate_integration_id(corporate_account, project_id, integration_id):
         return jsonify({
            'status': 'Failed',
            'status_description': 'Integration requirement Id is not valid'
        })

    if  filter_by_status and  not isinstance(filter_by_status, list):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Filter by status must be provided as an array'
        })

    sts = "Success"
    sts_description = "Integration field list retrieved successfully"

    if integration_id is None:
        integration_id = 0

    integration_fields_list = []
    placeholders = ','.join(['%s'] * len(filter_by_status))
    # if not sort_criteria:
    #     sort_criteria = 'field_name ASC'  # Default sort criteria if not provided

    sort_criteria = 'FIELD_NAME ASC'  # Default sort criteria if not provided

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        mySql_select_query = f"""SELECT 
                    FIELD_NAME, 
                    FIELD_DESCRIPTION,
                    STATUS, 
                    FIELD_DATA_TYPE,
                    FIELD_SIZE,
                    FIELD_OPTIONAL_OR_MANDATORY,
                    FIELD_TRANSFORMATION,
                    FIELD_DATA_VALIDATION,
                    FIELD_DATA_SECURITY,
                    CREATED_DATE,
                    UPDATED_DATE,
                    MAPS_TO_PROVIDER_SYSTEM_FIELD_NAME       
        FROM INTEGRATION_REQUIREMENTS_FIELDS WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND INTEGRATION_ID = %s AND SYSTEM_ID = %s AND SYSTEM_TYPE = %s """

        record = [corporate_account, project_id, integration_id, system_id, system_type]
        if filter_by_status:
            mySql_select_query += f" AND STATUS IN({placeholders}) "
            record.extend(filter_by_status)

        if search_query:
            mySql_select_query += " AND ( FIELD_NAME LIKE %s OR FIELD_DESCRIPTION LIKE %s ) ORDER BY " + sort_criteria
            record.extend([f"%{search_query}%", f"%{search_query}%"])

        else:
                mySql_select_query += " ORDER BY " + sort_criteria

        cursor.execute(mySql_select_query, record)

        logging.info(f" executed SQL is: {cursor._executed}")


        for result in cursor.fetchall():

            integration_field_details = {
                "field_name": result[0],
                "field_description": result[1],
                "status": result[2],
                "field_data_type": result[3],
                "field_size": result[4],
                "field_optional_or_mandatory": result[5],
                "field_transformation": result[6],
                "field_data_validation": result[7],
                "field_data_security": result[8],
                "created_date": result[9],
                "updated_date": result[10],
                "maps_to_provider_system_field_name": result[11]
                }
            integration_fields_list.append(integration_field_details)


        if len(integration_fields_list) == 0:
            sts = "Failed"
            sts_description = "No matching integration fields found"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the integration fields details: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'integration_fields_list': integration_fields_list,
        'status': sts,
        'status_description': sts_description
    })


@integration_requirements_blueprint.route('/api/create_integration_requirements_mapping_to_functional_requirement_NR', methods=['POST'])
@token_required
@validate_access
def create_integration_requirements_mapping_to_functional_requirement_NR(current_user):

    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    integration_id = data.get('integration_id')
  #  target_or_consumer_system_id = data.get('target_or_consumer_system_id')
    level_id = data.get('level_id')
    req_id = data.get('req_id')


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
    if not validate_integration_id (corporate_account, project_id, integration_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Integration requirement Id is not valid'
        })
  #  if target_or_consumer_system_id and not validate_integration_system_id(corporate_account, project_id, target_or_consumer_system_id):
  #      return jsonify({
  #          'status': 'Failed',
  #          'status_description': 'Target/consumer system Id is not valid'
  #      })

    if level_id and not validate_level_id(corporate_account, project_id, level_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Level Id is not valid'
        })

    if req_id and not validate_req_id(corporate_account, project_id, req_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Requirement Id is not valid'
        })

    if not level_id:
        level_id = 0

    if not req_id:
        req_id = 0

    #if not target_or_consumer_system_id:
    target_or_consumer_system_id = 0

    if level_id and req_id:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Only one of functional level or requirement level mapping can be made'
        })

    sts = "Success"
    sts_description = "Integration mapping to functional requirement created successfully"
    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()


        mySql_insert_query = """INSERT INTO INTEGRATION_REQUIREMENTS_MAPPING_TO_FUNCTIONAL_REQS (CORPORATE_ACCOUNT, PROJECT_ID, INTEGRATION_ID, 
        TARGET_OR_CONSUMER_SYSTEM_ID, LEVEL_ID, REQ_ID, STATUS, CREATED_DATE, UPDATED_DATE) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) """
        record = (corporate_account, project_id, integration_id, target_or_consumer_system_id, level_id, req_id, 'Created', datetime.now(), datetime.now())
        cursor.execute(mySql_insert_query, record)
        connection.commit()

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to add the integration mapping to functional requirement: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'status': sts,
        'status_description': sts_description
    })




@integration_requirements_blueprint.route('/api/delete_integration_requirements_mapping_to_functional_requirement_NR', methods=['PUT'])
@token_required
@validate_access
def delete_integration_requirements_mapping_to_functional_requirement_NR(current_user):

    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    integration_id = data.get('integration_id')
    target_or_consumer_system_id = data.get('target_or_consumer_system_id')
    level_id = data.get('level_id')
    req_id = data.get('req_id')


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
    if not validate_integration_id (corporate_account, project_id, integration_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Integration requirement Id is not valid'
        })
    if target_or_consumer_system_id and not validate_integration_system_id(corporate_account, project_id, target_or_consumer_system_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Target/consumer system Id is not valid'
        })

    if level_id and not validate_level_id(corporate_account, project_id, level_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Level Id is not valid'
        })

    if req_id and not validate_req_id(corporate_account, project_id, req_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Requirement Id is not valid'
        })

    if not level_id:
        level_id = 0

    if not req_id:
        req_id = 0

    if not target_or_consumer_system_id:
        target_or_consumer_system_id = 0

    if level_id and req_id:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Only one of functional level or requirement level mapping can be provided'
        })

    sts = "Success"
    sts_description = "Integration mapping to functional requirement successfully deleted"
    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()


        mySql_insert_query = """DELETE FROM INTEGRATION_REQUIREMENTS_MAPPING_TO_FUNCTIONAL_REQS 
        WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND INTEGRATION_ID = %s AND TARGET_OR_CONSUMER_SYSTEM_ID = %s AND LEVEL_ID = %s AND REQ_ID = %s """

        record = (corporate_account, project_id, integration_id, target_or_consumer_system_id, level_id, req_id)
        cursor.execute(mySql_insert_query, record)
        connection.commit()
        rows_impacted = cursor.rowcount
        if rows_impacted == 0:
            sts = "Failed"
            sts_description = "No matching integration mapping to functional requirement found to delete"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to delete the integration mapping to functional requirement: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'status': sts,
        'status_description': sts_description
    })




@integration_requirements_blueprint.route('/api/get_integration_requirement_details', methods=['GET','POST'])
@token_required
def get_integration_requirement_details(current_user):

    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    integration_id = data.get('integration_id')


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
    if not validate_integration_id (corporate_account, project_id, integration_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Integration requirement Id is not valid'
        })

    sts = "Success"
    sts_description = "Integration details retrieved successfully"
    integration_details = {}

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        mySql_select_query = """SELECT A.CORPORATE_ACCOUNT, A.PROJECT_ID, A.INTEGRATION_ID, A.INTEGRATION_NAME, A.INTEGRATION_DESCRIPTION, 
        A.STATUS INTEGRATION_STATUS, A.CREATED_DATE, A.UPDATED_DATE,
        A.SOURCE_OR_PROVIDER_SYSTEM_ID, B.SYSTEM_NAME, B.SYSTEM_DESCRIPTION, B.SYSTEM_ACRONYM, B.STATUS SOURCE_OR_PROVIDER_SYSTEM_STATUS,
        A.INTEGRATION_CRITICALITY,
        A.INTEGRATION_PRIORITY,
        A.REF_FIELD_1,
        A.REF_FIELD_2,
        A.REF_FIELD_3,
        A.REF_FIELD_4,
        A.PATTERN,
        A.TYPE,
        A.FREQUENCY,
        A.MIDDLEWARE,
        A.TRIGGERS_HOW,
        A.SOURCE_DATA_FORMAT,
        A.DATA_TRANSFER_PROTOCOL,
        A.AUTHENTICATION,
        A.ENDPOINTS,
        A.LOGGING_REQS,
        A.MONITORING,
        A.ERROR_HANDLING,
        A.PERFORMANCE,
        A.FAILOVER, A.LEVEL_ID
        FROM INTEGRATION_REQUIREMENTS A, INTEGRATION_SYSTEMS B WHERE A.SOURCE_OR_PROVIDER_SYSTEM_ID = B.SYSTEM_ID AND A.CORPORATE_ACCOUNT = B.CORPORATE_ACCOUNT 
        AND A.PROJECT_ID = B.PROJECT_ID AND A.CORPORATE_ACCOUNT = %s AND A.PROJECT_ID = %s AND A.INTEGRATION_ID = %s """

        record = (corporate_account, project_id, integration_id)

        cursor.execute(mySql_select_query, record)
        result = cursor.fetchone()

        mySql_select_query = """SELECT A.TARGET_OR_CONSUMER_SYSTEM_ID, A.CONSUMER_DESCRIPTION, B.SYSTEM_NAME, B.SYSTEM_DESCRIPTION, B.SYSTEM_ACRONYM, A.STATUS TARGET_OR_CONSUMER_SYSTEM_STATUS, A.INTEGRATION_TYPE, A.TARGET_DATA_FORMAT 
                FROM INTEGRATION_REQUIREMENTS_CONSUMERS A, INTEGRATION_SYSTEMS B WHERE A.TARGET_OR_CONSUMER_SYSTEM_ID = B.SYSTEM_ID AND A.CORPORATE_ACCOUNT = B.CORPORATE_ACCOUNT 
                AND A.PROJECT_ID = B.PROJECT_ID AND A.CORPORATE_ACCOUNT = %s AND A.PROJECT_ID = %s AND A.INTEGRATION_ID = %s """
        record = (corporate_account, project_id, integration_id)
        cursor.execute(mySql_select_query, record)
        consumer_list = []
        for result2 in cursor.fetchall():
            consumer_details = {
                'target_or_consumer_system_id': result2[0],
                'consumer_description': result2[1],
                'system_name': result2[2],
                'system_description': result2[3],
                'system_acronym': result2[4],
                'status': result2[5],
                'integration_type': result2[6],
                'target_data_format': result2[7]
            }
            consumer_list.append(consumer_details)

        mySql_select_query = """SELECT FIELD_NAME, FIELD_DESCRIPTION, STATUS FROM INTEGRATION_REQUIREMENTS_FIELDS 
        WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND INTEGRATION_ID = %s """
        record = (corporate_account, project_id, integration_id)
        cursor.execute(mySql_select_query, record)
        field_list = []
        for result3 in cursor.fetchall():
            field_details = {
                'field_name': result3[0],
                'field_description': result3[1],
                'status': result3[2]
            }
            field_list.append(field_details)

        if result:

            source_or_provider_system_details = {
                'system_id': result[8],
                'system_name': result[9],
                'system_description': result[10],
                'system_acrnonym': result[11],
                'system_status': result[12]
            }
            integration_details = {
                'corporate_account': result[0],
                'project_id': result[1],
                'integration_id': result[2],
                'integration_name': result[3],
                'integration_description': result[4],
                'integration_status': result[5],
                'created_date': result[6],
                'updated_date': result[7],
                'source_or_provider_system_details':  source_or_provider_system_details,
                'consumer_list': consumer_list,
                'field_list': field_list,

            'integration_criticality': result[13],
            'integration_priority': result[14],
            'ref_field_1': result[15],
            'ref_field_2': result[16],
            'ref_field_3': result[17],
            'ref_field_4': result[18],
            'pattern': result[19],
            'type': result[20],
            'frequency': result[21],
            'middleware': result[22],
            'triggers_how': result[23],
            'source_data_format': result[24],
            'data_transfer_protocol': result[25],
            'authentication': result[26],
            'endpoints': result[27],
            'logging_reqs': result[28],
            'monitoring': result[29],
            'error_handling': result[30],
            'performance': result[31],
            'failover': result[32],
            'level_id': result[33]

            }

        else:
            sts = "Failed"
            sts_description = "No matching integration found"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the integration details: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'integration_details': integration_details,
        'status': sts,
        'status_description': sts_description
    })


@integration_requirements_blueprint.route('/api/get_integration_requirement_list', methods=['GET', 'POST'])
@token_required
def get_integration_requirement_list(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    level_id = data.get('level_id')
    req_id = data.get('req_id')
    filter_by_status = data.get('filter_by_status', [])
    search_query = data.get('search_query')
    sort_criteria = data.get('sort_criteria')
    # Add product_ids parameter
    product_ids = data.get('product_ids', [])
    # Add new filter parameters
    source_systems = data.get('source_systems', [])
    target_systems = data.get('target_systems', [])
    integration_criticality = data.get('integration_criticality', [])
    integration_priority = data.get('integration_priority', [])
    created_date_start = data.get('created_date_start')
    created_date_end = data.get('created_date_end')
    updated_date_start = data.get('updated_date_start')
    updated_date_end = data.get('updated_date_end')
    include_child_levels_flag = data.get('include_child_levels_flag', False)

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

    if level_id and not validate_level_id(corporate_account, project_id, level_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Level Id is not valid'
        })

    if req_id and not validate_req_id(corporate_account, project_id, req_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Requirement Id is not valid'
        })

    if not level_id:
        level_id = 0

    if not req_id:
        req_id = 0

    if level_id and req_id:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Only one of functional level or requirement can be provided'
        })

    sts = "Success"
    sts_description = "Integrations list retrieved successfully"
    integration_details = {}
    integration_list = []

    # Add child levels processing
    level_condition = ""
    child_levels_list = []
    if level_id and include_child_levels_flag:
        child_levels_list = get_functional_level_children(corporate_account, project_id, level_id)
        if child_levels_list:
            # Convert list of child levels to a comma-separated string for SQL IN clause
            child_levels_placeholders = ','.join(['%s'] * len(child_levels_list))
            logging.info(f"child_levels_placeholders: {child_levels_placeholders}")
            logging.info(f"child_levels_list: {child_levels_list}")
            level_condition = f" AND A.LEVEL_ID IN ({child_levels_placeholders})"
        else:
            # If no child levels found, fallback to the provided level_id
            level_condition = " AND A.LEVEL_ID = %s "
            child_levels_list = [level_id]
    else:
        # If no child levels or include_child_levels_flag is False, use the provided level_id directly
        if level_id:
            level_condition = " AND A.LEVEL_ID = %s "
            child_levels_list = [level_id] if level_id else []
    logging.info(f"level_condition: {level_condition}")
    logging.info(f"child_levels_list: {child_levels_list}")

    # Product query similar to the reference implementation
    product_query = """ (SELECT A.INTEGRATION_ID, X.PRODUCT_ID, Y.PRODUCT_NAME, X.REQ_CLASSIFICATION, X.CREATED_DATE, X.UPDATED_DATE
     FROM INTEGRATION_REQUIREMENTS A LEFT OUTER JOIN REQUIREMENT_CLASSIFICATION X ON
     A.CORPORATE_ACCOUNT = X.CORPORATE_ACCOUNT AND A.PROJECT_ID = X.PROJECT_ID AND A.INTEGRATION_ID = X.REQ_ID
     LEFT OUTER JOIN PRODUCTS_BY_PROJECT Y ON X.PRODUCT_ID = Y.PRODUCT_ID AND X.CORPORATE_ACCOUNT = Y.CORPORATE_ACCOUNT AND X.PROJECT_ID = Y.PROJECT_ID
     , FUNCTIONAL_LEVELS B WHERE
     A.CORPORATE_ACCOUNT = B.CORPORATE_ACCOUNT AND A.PROJECT_ID = B.PROJECT_ID AND A.LEVEL_ID = B.LEVEL_ID AND
      X.PRODUCT_ID = """

    # Initialize params list for query parameters
    params = [corporate_account, project_id]

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        if (req_id == 0):
            # Base query for requirements without mapping
            mySql_select_query = """SELECT A.CORPORATE_ACCOUNT, A.PROJECT_ID, A.LEVEL_ID, C.LEVEL_DESCRIPTION, A.INTEGRATION_ID, A.INTEGRATION_ID_WITH_PREFIX, 
             A.INTEGRATION_NAME, A.INTEGRATION_DESCRIPTION, 
             A.STATUS, A.INTEGRATION_CRITICALITY, A.INTEGRATION_PRIORITY, A.REF_FIELD_1, A.REF_FIELD_2, A.REF_FIELD_3, A.REF_FIELD_4,
             A.CREATED_DATE, A.UPDATED_DATE,
             A.SOURCE_OR_PROVIDER_SYSTEM_ID, B.SYSTEM_NAME, B.SYSTEM_DESCRIPTION, B.SYSTEM_ACRONYM,
             (SELECT COUNT(*) FROM KEY_ATTRIBUTES_LIST_REQUIREMENTS D WHERE A.CORPORATE_ACCOUNT = D.CORPORATE_ACCOUNT AND A.PROJECT_ID = D.PROJECT_ID AND A.INTEGRATION_ID = D.REQ_ID) NUMBER_OF_EXCEPTIONS
             , (SELECT COUNT(*) FROM REQUIREMENTS_APPROVERS E WHERE A.CORPORATE_ACCOUNT = E.CORPORATE_ACCOUNT AND A.PROJECT_ID = E.PROJECT_ID AND A.INTEGRATION_ID = E.REQ_ID) NUMBER_OF_APPROVERS
             FROM INTEGRATION_REQUIREMENTS A, INTEGRATION_SYSTEMS B, FUNCTIONAL_LEVELS C
             WHERE A.SOURCE_OR_PROVIDER_SYSTEM_ID = B.SYSTEM_ID AND A.CORPORATE_ACCOUNT = B.CORPORATE_ACCOUNT AND A.PROJECT_ID = B.PROJECT_ID
             AND A.CORPORATE_ACCOUNT = C.CORPORATE_ACCOUNT AND A.PROJECT_ID = C.PROJECT_ID AND A.LEVEL_ID = C.LEVEL_ID 
             AND A.CORPORATE_ACCOUNT = %s AND A.PROJECT_ID = %s"""
        else:
            # Base query for requirements with mapping
            mySql_select_query = """SELECT A.CORPORATE_ACCOUNT, A.PROJECT_ID, A.LEVEL_ID, C.LEVEL_DESCRIPTION, A.INTEGRATION_ID, A.INTEGRATION_ID_WITH_PREFIX, 
             A.INTEGRATION_NAME, A.INTEGRATION_DESCRIPTION, 
             A.STATUS, A.INTEGRATION_CRITICALITY, A.INTEGRATION_PRIORITY, A.REF_FIELD_1, A.REF_FIELD_2, A.REF_FIELD_3, A.REF_FIELD_4,
             A.CREATED_DATE, A.UPDATED_DATE,
             A.SOURCE_OR_PROVIDER_SYSTEM_ID, B.SYSTEM_NAME, B.SYSTEM_DESCRIPTION, B.SYSTEM_ACRONYM,
             (SELECT COUNT(*) FROM KEY_ATTRIBUTES_LIST_REQUIREMENTS D WHERE A.CORPORATE_ACCOUNT = D.CORPORATE_ACCOUNT AND A.PROJECT_ID = D.PROJECT_ID AND A.INTEGRATION_ID = D.REQ_ID) NUMBER_OF_EXCEPTIONS
             , (SELECT COUNT(*) FROM REQUIREMENTS_APPROVERS E WHERE A.CORPORATE_ACCOUNT = E.CORPORATE_ACCOUNT AND A.PROJECT_ID = E.PROJECT_ID AND A.INTEGRATION_ID = E.REQ_ID) NUMBER_OF_APPROVERS
             FROM INTEGRATION_REQUIREMENTS A, INTEGRATION_SYSTEMS B, FUNCTIONAL_LEVELS C 
             WHERE A.SOURCE_OR_PROVIDER_SYSTEM_ID = B.SYSTEM_ID AND A.CORPORATE_ACCOUNT = B.CORPORATE_ACCOUNT AND A.PROJECT_ID = B.PROJECT_ID 
             AND A.CORPORATE_ACCOUNT = C.CORPORATE_ACCOUNT AND A.PROJECT_ID = C.PROJECT_ID AND A.LEVEL_ID = C.LEVEL_ID 
             AND A.CORPORATE_ACCOUNT = %s AND A.PROJECT_ID = %s AND A.INTEGRATION_ID = %s"""
            params.append(req_id)

        # Add status filter
        if filter_by_status and len(filter_by_status) > 0:
            placeholders = ','.join(['%s'] * len(filter_by_status))
            mySql_select_query += f" AND A.STATUS IN({placeholders})"
            params.extend(filter_by_status)

        # Add source systems filter if provided
        if source_systems and isinstance(source_systems, list) and len(source_systems) > 0:
            source_systems_placeholders = ','.join(['%s'] * len(source_systems))
            mySql_select_query += f" AND A.SOURCE_OR_PROVIDER_SYSTEM_ID IN({source_systems_placeholders})"
            params.extend(source_systems)

        # Add criticality filter if provided
        if integration_criticality and isinstance(integration_criticality, list) and len(integration_criticality) > 0:
            criticality_placeholders = ','.join(['%s'] * len(integration_criticality))
            mySql_select_query += f" AND A.INTEGRATION_CRITICALITY IN({criticality_placeholders})"
            params.extend(integration_criticality)

        # Add priority filter if provided
        if integration_priority and isinstance(integration_priority, list) and len(integration_priority) > 0:
            priority_placeholders = ','.join(['%s'] * len(integration_priority))
            mySql_select_query += f" AND A.INTEGRATION_PRIORITY IN({priority_placeholders})"
            params.extend(integration_priority)

        # Add created_date range filter if provided
        if created_date_start:
            mySql_select_query += " AND A.CREATED_DATE >= %s"
            params.append(created_date_start)

        if created_date_end:
            mySql_select_query += " AND DATE(A.CREATED_DATE) <= DATE(%s)"
            params.append(created_date_end)

        # Add updated_date range filter if provided
        if updated_date_start:
            mySql_select_query += " AND A.UPDATED_DATE >= %s"
            params.append(updated_date_start)

        if updated_date_end:
            mySql_select_query += " AND DATE(A.UPDATED_DATE) <= DATE(%s)"
            params.append(updated_date_end)

        # Update search query and level_id handling
        if search_query:
            if level_id:
                mySql_select_query += level_condition
                params.extend(child_levels_list)
                mySql_select_query += " AND (A.INTEGRATION_ID_WITH_PREFIX = %s OR A.INTEGRATION_DESCRIPTION LIKE %s OR A.INTEGRATION_NAME LIKE %s)"
                params.extend([search_query, f"%{search_query}%", f"%{search_query}%"])
            else:
                mySql_select_query += " AND (A.INTEGRATION_ID_WITH_PREFIX = %s OR A.INTEGRATION_DESCRIPTION LIKE %s OR A.INTEGRATION_NAME LIKE %s)"
                params.extend([search_query, f"%{search_query}%", f"%{search_query}%"])
        else:
            if level_id:
                mySql_select_query += level_condition
                params.extend(child_levels_list)

        # Handling product_ids similar to get_requirements_list
        if not product_ids or len(product_ids) == 0:
            # If no product_ids are provided, just use the base query
            final_sql_query = mySql_select_query
        else:
            # If product_ids are provided, create a CTE for each product
            if len(product_ids) > 5:
                return jsonify({
                    'status': 'Failed',
                    'status_description': 'No more than 5 product IDs can be provided'
                })

            if not all(validate_product_id(corporate_account, project_id, pid) for pid in product_ids):
                return jsonify({
                    'status': 'Failed',
                    'status_description': 'One or more Product IDs are not valid'
                })

            if len(product_ids) == 1:
                final_sql_query = "WITH INTEGRATION AS ( " + mySql_select_query + " ), "
                final_sql_query += "PRODUCT_1 AS " + product_query + product_ids[0] + " )"
                final_sql_query += """SELECT P.*,
                Q.PRODUCT_ID PRODUCT_ID_1, Q.PRODUCT_NAME PRODUCT_NAME_1, Q.REQ_CLASSIFICATION REQUIREMENT_CLASSIFICATION_1, Q.CREATED_DATE CREATED_DATE_1, Q.UPDATED_DATE UPDATED_DATE_1
                 FROM INTEGRATION P
                LEFT OUTER JOIN PRODUCT_1 Q ON P.INTEGRATION_ID = Q.INTEGRATION_ID"""
            elif len(product_ids) == 2:
                final_sql_query = "WITH INTEGRATION AS ( " + mySql_select_query + " ), "
                final_sql_query += "PRODUCT_1 AS " + product_query + product_ids[0] + " ),"
                final_sql_query += "PRODUCT_2 AS " + product_query + product_ids[1] + " )"
                final_sql_query += """SELECT P.*,
                Q.PRODUCT_ID PRODUCT_ID_1, Q.PRODUCT_NAME PRODUCT_NAME_1, Q.REQ_CLASSIFICATION REQUIREMENT_CLASSIFICATION_1, Q.CREATED_DATE CREATED_DATE_1, Q.UPDATED_DATE UPDATED_DATE_1,
                R.PRODUCT_ID PRODUCT_ID_2, R.PRODUCT_NAME PRODUCT_NAME_2, R.REQ_CLASSIFICATION REQUIREMENT_CLASSIFICATION_2, R.CREATED_DATE CREATED_DATE_2, R.UPDATED_DATE UPDATED_DATE_2
                FROM INTEGRATION P
                LEFT OUTER JOIN PRODUCT_1 Q ON P.INTEGRATION_ID = Q.INTEGRATION_ID
                LEFT OUTER JOIN PRODUCT_2 R ON P.INTEGRATION_ID = R.INTEGRATION_ID"""
            elif len(product_ids) == 3:
                final_sql_query = "WITH INTEGRATION AS ( " + mySql_select_query + " ), "
                final_sql_query += "PRODUCT_1 AS " + product_query + product_ids[0] + " ),"
                final_sql_query += "PRODUCT_2 AS " + product_query + product_ids[1] + " ),"
                final_sql_query += "PRODUCT_3 AS " + product_query + product_ids[2] + " )"
                final_sql_query += """SELECT P.*,
                Q.PRODUCT_ID PRODUCT_ID_1, Q.PRODUCT_NAME PRODUCT_NAME_1, Q.REQ_CLASSIFICATION REQUIREMENT_CLASSIFICATION_1, Q.CREATED_DATE CREATED_DATE_1, Q.UPDATED_DATE UPDATED_DATE_1,
                R.PRODUCT_ID PRODUCT_ID_2, R.PRODUCT_NAME PRODUCT_NAME_2, R.REQ_CLASSIFICATION REQUIREMENT_CLASSIFICATION_2, R.CREATED_DATE CREATED_DATE_2, R.UPDATED_DATE UPDATED_DATE_2,
                S.PRODUCT_ID PRODUCT_ID_3, S.PRODUCT_NAME PRODUCT_NAME_3, S.REQ_CLASSIFICATION REQUIREMENT_CLASSIFICATION_3, S.CREATED_DATE CREATED_DATE_3, S.UPDATED_DATE UPDATED_DATE_3
                FROM INTEGRATION P
                LEFT OUTER JOIN PRODUCT_1 Q ON P.INTEGRATION_ID = Q.INTEGRATION_ID
                LEFT OUTER JOIN PRODUCT_2 R ON P.INTEGRATION_ID = R.INTEGRATION_ID
                LEFT OUTER JOIN PRODUCT_3 S ON P.INTEGRATION_ID = S.INTEGRATION_ID"""
            elif len(product_ids) == 4:
                final_sql_query = "WITH INTEGRATION AS ( " + mySql_select_query + " ), "
                final_sql_query += "PRODUCT_1 AS " + product_query + product_ids[0] + " ),"
                final_sql_query += "PRODUCT_2 AS " + product_query + product_ids[1] + " ),"
                final_sql_query += "PRODUCT_3 AS " + product_query + product_ids[2] + " ),"
                final_sql_query += "PRODUCT_4 AS " + product_query + product_ids[3] + " )"
                final_sql_query += """SELECT P.*,
                Q.PRODUCT_ID PRODUCT_ID_1, Q.PRODUCT_NAME PRODUCT_NAME_1, Q.REQ_CLASSIFICATION REQUIREMENT_CLASSIFICATION_1, Q.CREATED_DATE CREATED_DATE_1, Q.UPDATED_DATE UPDATED_DATE_1,
                R.PRODUCT_ID PRODUCT_ID_2, R.PRODUCT_NAME PRODUCT_NAME_2, R.REQ_CLASSIFICATION REQUIREMENT_CLASSIFICATION_2, R.CREATED_DATE CREATED_DATE_2, R.UPDATED_DATE UPDATED_DATE_2,
                S.PRODUCT_ID PRODUCT_ID_3, S.PRODUCT_NAME PRODUCT_NAME_3, S.REQ_CLASSIFICATION REQUIREMENT_CLASSIFICATION_3, S.CREATED_DATE CREATED_DATE_3, S.UPDATED_DATE UPDATED_DATE_3,
                T.PRODUCT_ID PRODUCT_ID_4, T.PRODUCT_NAME PRODUCT_NAME_4, T.REQ_CLASSIFICATION REQUIREMENT_CLASSIFICATION_4, T.CREATED_DATE CREATED_DATE_4, T.UPDATED_DATE UPDATED_DATE_4
                FROM INTEGRATION P
                LEFT OUTER JOIN PRODUCT_1 Q ON P.INTEGRATION_ID = Q.INTEGRATION_ID
                LEFT OUTER JOIN PRODUCT_2 R ON P.INTEGRATION_ID = R.INTEGRATION_ID
                LEFT OUTER JOIN PRODUCT_3 S ON P.INTEGRATION_ID = S.INTEGRATION_ID  
                LEFT OUTER JOIN PRODUCT_4 T ON P.INTEGRATION_ID = T.INTEGRATION_ID"""
            elif len(product_ids) == 5:
                final_sql_query = "WITH INTEGRATION AS ( " + mySql_select_query + " ), "
                final_sql_query += "PRODUCT_1 AS " + product_query + product_ids[0] + " ),"
                final_sql_query += "PRODUCT_2 AS " + product_query + product_ids[1] + " ),"
                final_sql_query += "PRODUCT_3 AS " + product_query + product_ids[2] + " ),"
                final_sql_query += "PRODUCT_4 AS " + product_query + product_ids[3] + " ),"
                final_sql_query += "PRODUCT_5 AS " + product_query + product_ids[4] + " )"
                final_sql_query += """SELECT P.*,
                Q.PRODUCT_ID PRODUCT_ID_1, Q.PRODUCT_NAME PRODUCT_NAME_1, Q.REQ_CLASSIFICATION REQUIREMENT_CLASSIFICATION_1, Q.CREATED_DATE CREATED_DATE_1, Q.UPDATED_DATE UPDATED_DATE_1,
                R.PRODUCT_ID PRODUCT_ID_2, R.PRODUCT_NAME PRODUCT_NAME_2, R.REQ_CLASSIFICATION REQUIREMENT_CLASSIFICATION_2, R.CREATED_DATE CREATED_DATE_2, R.UPDATED_DATE UPDATED_DATE_2,
                S.PRODUCT_ID PRODUCT_ID_3, S.PRODUCT_NAME PRODUCT_NAME_3, S.REQ_CLASSIFICATION REQUIREMENT_CLASSIFICATION_3, S.CREATED_DATE CREATED_DATE_3, S.UPDATED_DATE UPDATED_DATE_3,
                T.PRODUCT_ID PRODUCT_ID_4, T.PRODUCT_NAME PRODUCT_NAME_4, T.REQ_CLASSIFICATION REQUIREMENT_CLASSIFICATION_4, T.CREATED_DATE CREATED_DATE_4, T.UPDATED_DATE UPDATED_DATE_4,
                U.PRODUCT_ID PRODUCT_ID_5, U.PRODUCT_NAME PRODUCT_NAME_5, U.REQ_CLASSIFICATION REQUIREMENT_CLASSIFICATION_5, U.CREATED_DATE CREATED_DATE_5, U.UPDATED_DATE UPDATED_DATE_5
                FROM INTEGRATION P
                LEFT OUTER JOIN PRODUCT_1 Q ON P.INTEGRATION_ID = Q.INTEGRATION_ID
                LEFT OUTER JOIN PRODUCT_2 R ON P.INTEGRATION_ID = R.INTEGRATION_ID
                LEFT OUTER JOIN PRODUCT_3 S ON P.INTEGRATION_ID = S.INTEGRATION_ID  
                LEFT OUTER JOIN PRODUCT_4 T ON P.INTEGRATION_ID = T.INTEGRATION_ID 
                LEFT OUTER JOIN PRODUCT_5 U ON P.INTEGRATION_ID = U.INTEGRATION_ID"""

        # Add sort criteria
        if not sort_criteria:
            sort_criteria = 'INTEGRATION_ID'

        # Add ORDER BY at the end
        final_sql_query += " ORDER BY " + sort_criteria

        logging.info(f" Prepared SQL is: {final_sql_query}")

        # Execute the query with parameters
        cursor.execute(final_sql_query, tuple(params))
        logging.info(f"Executed SQL: {cursor._executed}")

        # Get column names from cursor
        column_names = [desc[0] for desc in cursor.description]

        # Process results
        for result in cursor.fetchall():
            result_dict = dict(zip(column_names, result))

            # Initialize product classification objects
            product_1_classification = None
            product_2_classification = None
            product_3_classification = None
            product_4_classification = None
            product_5_classification = None

            # Populate product classification objects based on product_ids
            if len(product_ids) >= 1:
                product_1_classification = {
                    'product_id': result_dict.get('PRODUCT_ID_1'),
                    'product_name': result_dict.get('PRODUCT_NAME_1'),
                    'product_classification': result_dict.get('REQUIREMENT_CLASSIFICATION_1'),
                    'created_date': result_dict.get('CREATED_DATE_1'),
                    'updated_date': result_dict.get('UPDATED_DATE_1')
                }
            if len(product_ids) >= 2:
                product_2_classification = {
                    'product_id': result_dict.get('PRODUCT_ID_2'),
                    'product_name': result_dict.get('PRODUCT_NAME_2'),
                    'product_classification': result_dict.get('REQUIREMENT_CLASSIFICATION_2'),
                    'created_date': result_dict.get('CREATED_DATE_2'),
                    'updated_date': result_dict.get('UPDATED_DATE_2')
                }
            if len(product_ids) >= 3:
                product_3_classification = {
                    'product_id': result_dict.get('PRODUCT_ID_3'),
                    'product_name': result_dict.get('PRODUCT_NAME_3'),
                    'product_classification': result_dict.get('REQUIREMENT_CLASSIFICATION_3'),
                    'created_date': result_dict.get('CREATED_DATE_3'),
                    'updated_date': result_dict.get('UPDATED_DATE_3')
                }
            if len(product_ids) >= 4:
                product_4_classification = {
                    'product_id': result_dict.get('PRODUCT_ID_4'),
                    'product_name': result_dict.get('PRODUCT_NAME_4'),
                    'product_classification': result_dict.get('REQUIREMENT_CLASSIFICATION_4'),
                    'created_date': result_dict.get('CREATED_DATE_4'),
                    'updated_date': result_dict.get('UPDATED_DATE_4')
                }
            if len(product_ids) >= 5:
                product_5_classification = {
                    'product_id': result_dict.get('PRODUCT_ID_5'),
                    'product_name': result_dict.get('PRODUCT_NAME_5'),
                    'product_classification': result_dict.get('REQUIREMENT_CLASSIFICATION_5'),
                    'created_date': result_dict.get('CREATED_DATE_5'),
                    'updated_date': result_dict.get('UPDATED_DATE_5')
                }

            source_or_provider_system_details = {
                'system_id': result_dict['SOURCE_OR_PROVIDER_SYSTEM_ID'],
                'system_name': result_dict['SYSTEM_NAME'],
                'system_description': result_dict['SYSTEM_DESCRIPTION'],
                'system_acronym': result_dict['SYSTEM_ACRONYM']
            }

            integration_details = {
                'corporate_account': result_dict['CORPORATE_ACCOUNT'],
                'project_id': result_dict['PROJECT_ID'],
                'level_id': result_dict['LEVEL_ID'],
                'level_description': result_dict['LEVEL_DESCRIPTION'],
                'integration_id': result_dict['INTEGRATION_ID'],
                'integration_id_with_prefix': result_dict['INTEGRATION_ID_WITH_PREFIX'],
                'integration_name': result_dict['INTEGRATION_NAME'],
                'integration_description': result_dict['INTEGRATION_DESCRIPTION'],
                'status': result_dict['STATUS'],
                'integration_criticality': result_dict['INTEGRATION_CRITICALITY'],
                'integration_priority': result_dict['INTEGRATION_PRIORITY'],
                'ref_field_1': result_dict['REF_FIELD_1'],
                'ref_field_2': result_dict['REF_FIELD_2'],
                'ref_field_3': result_dict['REF_FIELD_3'],
                'ref_field_4': result_dict['REF_FIELD_4'],
                'created_date': result_dict['CREATED_DATE'],
                'updated_date': result_dict['UPDATED_DATE'],
                'number_of_exceptions': result_dict['NUMBER_OF_EXCEPTIONS'],
                'number_of_approvers': result_dict['NUMBER_OF_APPROVERS'],
                'source_or_provider_system_details': source_or_provider_system_details,
                'product_1_classification': product_1_classification,
                'product_2_classification': product_2_classification,
                'product_3_classification': product_3_classification,
                'product_4_classification': product_4_classification,
                'product_5_classification': product_5_classification
            }
            integration_list.append(integration_details)

        if len(integration_list) == 0:
            sts = "Failed"
            sts_description = "No matching integration found"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the integration details: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'integration_list': integration_list,
        'status': sts,
        'status_description': sts_description
    })




@integration_requirements_blueprint.route('/api/get_integration_requirement_list_by_field_name', methods=['GET'])
@token_required
def get_integration_requirement_list_by_field_name(current_user):

    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    field_name = data.get('field_name')


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

    if not field_name.strip():
        return jsonify({
            'status': 'Failed',
            'status_description': 'Field name is required'
        })

    sts = "Success"
    sts_description = "Integrations list retrieved successfully"
    integration_details = {}
    integration_list = []

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_select_query = """SELECT A.CORPORATE_ACCOUNT, A.PROJECT_ID, A.INTEGRATION_ID, A.INTEGRATION_NAME, A.INTEGRATION_DESCRIPTION, 
        A.STATUS INTEGRATION_STATUS, A.CREATED_DATE, A.UPDATED_DATE,
        A.SOURCE_OR_PROVIDER_SYSTEM_ID, B.SYSTEM_NAME, B.SYSTEM_DESCRIPTION, B.SYSTEM_ACRONYM, B.STATUS SOURCE_OR_PROVIDER_SYSTEM_STATUS  
        FROM INTEGRATION_REQUIREMENTS A, INTEGRATION_SYSTEMS B
         WHERE A.SOURCE_OR_PROVIDER_SYSTEM_ID = B.SYSTEM_ID AND A.CORPORATE_ACCOUNT = B.CORPORATE_ACCOUNT AND A.PROJECT_ID = B.PROJECT_ID
          AND A.CORPORATE_ACCOUNT = %s AND A.PROJECT_ID = %s 
          AND EXISTS (SELECT * FROM INTEGRATION_REQUIREMENTS_FIELDS WHERE CORPORATE_ACCOUNT = A.CORPORATE_ACCOUNT AND PROJECT_ID = A.PROJECT_ID 
          AND INTEGRATION_ID = A.INTEGRATION_ID AND FIELD_NAME = %s) """
        record = (corporate_account, project_id, field_name)


        cursor.execute(mySql_select_query, record)
        for result in cursor.fetchall():

            source_or_provider_system_details = {
                'system_id': result[8],
                'system_name': result[9],
                'system_description': result[10],
                'system_acrnonym': result[11],
                'system_status': result[12]
            }
            integration_details = {
                'corporate_account': result[0],
                'project_id': result[1],
                'integration_id': result[2],
                'integration_name': result[3],
                'integration_description': result[4],
                'integration_status': result[5],
                'created_date': result[6],
                'updated_date': result[7],
                'source_or_provider_system_details':  source_or_provider_system_details
            }
            integration_list.append(integration_details)


        if len(integration_list) == 0:
            sts = "Failed"
            sts_description = "No matching integration found"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the integration details: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'integration_list': integration_list,
        'status': sts,
        'status_description': sts_description
    })



@integration_requirements_blueprint.route('/api/get_integration_requirement_list_by_system', methods=['GET'])
@token_required
def get_integration_requirement_list_by_system(current_user):

    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    system_id = data.get('system_id')


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

    if not system_id.strip():
        return jsonify({
            'status': 'Failed',
            'status_description': 'System Id is required'
        })

    sts = "Success"
    sts_description = "Integrations list retrieved successfully"
    integration_details = {}
    integration_list = []

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_select_query = """SELECT A.CORPORATE_ACCOUNT, A.PROJECT_ID, A.INTEGRATION_ID, A.INTEGRATION_NAME, A.INTEGRATION_DESCRIPTION, 
        A.STATUS INTEGRATION_STATUS, A.CREATED_DATE, A.UPDATED_DATE,
        A.SOURCE_OR_PROVIDER_SYSTEM_ID, B.SYSTEM_NAME, B.SYSTEM_DESCRIPTION, B.SYSTEM_ACRONYM, B.STATUS SOURCE_OR_PROVIDER_SYSTEM_STATUS  
        FROM INTEGRATION_REQUIREMENTS A, INTEGRATION_SYSTEMS B
         WHERE A.SOURCE_OR_PROVIDER_SYSTEM_ID = B.SYSTEM_ID AND A.CORPORATE_ACCOUNT = B.CORPORATE_ACCOUNT AND A.PROJECT_ID = B.PROJECT_ID
          AND A.CORPORATE_ACCOUNT = %s AND A.PROJECT_ID = %s AND ( A.SOURCE_OR_PROVIDER_SYSTEM_ID = %s 
          OR EXISTS (SELECT * FROM INTEGRATION_REQUIREMENTS_CONSUMERS WHERE CORPORATE_ACCOUNT = A.CORPORATE_ACCOUNT AND PROJECT_ID = A.PROJECT_ID 
          AND INTEGRATION_ID = A.INTEGRATION_ID AND TARGET_OR_CONSUMER_SYSTEM_ID = %s)) """
        record = (corporate_account, project_id, system_id, system_id)


        cursor.execute(mySql_select_query, record)
        for result in cursor.fetchall():

            source_or_provider_system_details = {
                'system_id': result[8],
                'system_name': result[9],
                'system_description': result[10],
                'system_acrnonym': result[11],
                'system_status': result[12]
            }
            integration_details = {
                'corporate_account': result[0],
                'project_id': result[1],
                'integration_id': result[2],
                'integration_name': result[3],
                'integration_description': result[4],
                'integration_status': result[5],
                'created_date': result[6],
                'updated_date': result[7],
                'source_or_provider_system_details':  source_or_provider_system_details
            }
            integration_list.append(integration_details)


        if len(integration_list) == 0:
            sts = "Failed"
            sts_description = "No matching integration found"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the integration details: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'integration_list': integration_list,
        'status': sts,
        'status_description': sts_description
    })

if __name__ == '__main__':
    app.run(debug=True)


