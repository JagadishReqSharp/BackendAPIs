import re
from flask import Flask, request, jsonify, Blueprint
from flask_cors import CORS
import mysql.connector
import config
from datetime import datetime, timedelta
import logging
import jwt
from config import TOKEN_EXPIRY_DAYS
from argon2 import PasswordHasher
from functools import wraps
from access_validation_at_api_level import validate_access



from config import SECRET_KEY
from foundational_v2 import generate_next_sequence , validate_corporate_account, validate_project_id, validate_functional_domain,  validate_user_id, validate_functional_level, get_functional_level_dependency_details, validate_functional_attribute_category, get_functional_level_details, validate_level_id
from utils import token_required


# Create a blueprint for user-related routes
initialsetup_blueprint = Blueprint('initialsetup', __name__)



app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": ["http://localhost:3000"]}})
logging.basicConfig(filename='debugging.log', level=logging.DEBUG)



@initialsetup_blueprint.route('/api/create_business_team', methods=['POST'])
@token_required
@validate_access
def create_business_team(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    business_team_description = data.get('business_team_description')


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

    if not business_team_description.strip():
        return jsonify({
            'status': 'Failed',
            'status_description': 'Business team description is required'
        })

    sts = "Success"
    sts_description = "Business team added successfully"
    business_team_id = None
    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_insert_query = """INSERT INTO BUSINESS_TEAMS (CORPORATE_ACCOUNT, PROJECT_ID, BUSINESS_TEAM_ID, BUSINESS_TEAM_DESCRIPTION, 
        STATUS, CREATED_DATE, UPDATED_DATE)
        VALUES (%s, %s, %s, %s, %s, %s, %s) """

        business_team_id, seq_status, seq_status_description  = generate_next_sequence(corporate_account, project_id, 'BUSINESS_TEAM_ID')

        if seq_status == "Failed":
            sts = "Failed"
            sts_description = seq_status_description
            return jsonify({
                'status': sts,
                'status_description': sts_description
            })
        record = (corporate_account, project_id, business_team_id,  business_team_description, 'Active', datetime.now(), datetime.now())
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
        'business_team_id': business_team_id,
        'status': sts,
        'status_description': sts_description
    })


@initialsetup_blueprint.route('/api/update_business_team', methods=['POST'])
@token_required
@validate_access
def update_business_team(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    business_team_id = data.get('business_team_id')
    business_team_description = data.get('business_team_description')

    logging.info(f"current_user: {current_user}")

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

    if not business_team_id:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Business team Id is required'
        })

    if not business_team_description.strip():
        return jsonify({
            'status': 'Failed',
            'status_description': 'Business team description is required'
        })

    sts = "Success"
    sts_description = "Business team details updated successfully"
    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_insert_query = """UPDATE BUSINESS_TEAMS SET BUSINESS_TEAM_DESCRIPTION = %s, UPDATED_DATE = %s WHERE 
        CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND BUSINESS_TEAM_ID = %s """


        record = (business_team_description, datetime.now(), corporate_account, project_id, business_team_id)
        cursor.execute(mySql_insert_query, record)
        connection.commit()


        rows_impacted = cursor.rowcount
        if rows_impacted == 0:
            sts = "Failed"
            sts_description = "No matching business team found to update"


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


@initialsetup_blueprint.route('/api/delete_business_team', methods=['POST'])
@token_required
@validate_access
def delete_business_team(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    business_team_ids = data.get('business_team_ids', [])  # Expect an array of IDs

    logging.info(f"input json: {data}")
    logging.info(f"input json: {business_team_ids}")


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

    if not business_team_ids or not isinstance(business_team_ids, list):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Business team IDs must be provided as an array'
        })

    sts = "Success"
    sts_description = "Business teams deleted successfully"
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
        placeholders = ','.join(['%s'] * len(business_team_ids))
        mySql_delete_query = f"""DELETE FROM BUSINESS_TEAMS 
            WHERE CORPORATE_ACCOUNT = %s 
            AND PROJECT_ID = %s 
            AND BUSINESS_TEAM_ID IN ({placeholders})"""

        # Prepare parameters: corporate_account, project_id, followed by all business_team_ids
        record = (corporate_account, project_id, *business_team_ids)

        cursor.execute(mySql_delete_query, record)
        deleted_count = cursor.rowcount

        if deleted_count == 0:
            sts = "Failed"
            sts_description = "No matching business teams found to delete"
        else:
            sts_description = f"Successfully deleted {deleted_count} business team(s)"

        # Commit the transaction
        connection.commit()

    except mysql.connector.Error as error:
        if connection:
            connection.rollback()  # Rollback in case of error
        sts = "Failed"
        sts_description = f"Failed to delete the business team details: {error}"
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


@initialsetup_blueprint.route('/api/get_business_team_details', methods=['GET','POST'])
@token_required
@validate_access
def get_business_team_details(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    business_team_id = data.get('business_team_id')


    sts = "Success"
    sts_description = "Business team details retrieved successfully"
    business_team_description = None
    status = None
    created_date = None
    updated_date = None

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_select_query = """SELECT BUSINESS_TEAM_DESCRIPTION, STATUS, CREATED_DATE, UPDATED_DATE FROM BUSINESS_TEAMS 
        WHERE BUSINESS_TEAM_ID = %s AND CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s"""
        record = (business_team_id, corporate_account, project_id)

        cursor.execute(mySql_select_query, record)
        result = cursor.fetchone()

        if result:
                business_team_description = result[0]
                status = result[1]
                created_date = result[2]
                updated_date = result[3]
        else:
                sts = "Failed"
                sts_description = "No matching business team found"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the business team details: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'business_team_description': business_team_description,
        'business_team_status': status,
        'created_date': created_date,
        'updated_date': updated_date,
        'status': sts,
        'status_description': sts_description
    })


@initialsetup_blueprint.route('/api/get_business_team_list', methods=['GET', 'POST'])
@token_required
def get_business_team_list(current_user):

    data = request.json
    corporate_account= data.get('corporate_account')
    project_id = data.get('project_id')
    search_query = data.get('search_query')

    logging.info(f"inside get_business_team_list corporate acc: {corporate_account}")
    logging.info(f"inside get_business_team_list project id: {project_id}")
    logging.info(f"inside get_business_team_list search text: {search_query}")


    sts = "Success"
    sts_description = "Business teams list retrieved successfully"
    business_team_details = {}
    business_team_list = []

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_select_query = """SELECT BUSINESS_TEAM_ID, BUSINESS_TEAM_DESCRIPTION,
        STATUS, CREATED_DATE, UPDATED_DATE FROM BUSINESS_TEAMS
        WHERE STATUS = %s AND CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s """

        if search_query:
            mySql_select_query += " AND BUSINESS_TEAM_DESCRIPTION LIKE %s"
            record = ('Active', corporate_account, project_id, f"%{search_query}%")
        else:
            record = ('Active', corporate_account, project_id)


        cursor.execute(mySql_select_query, record)
        for result in cursor.fetchall():

            business_team_details = {
                'business_team_id': result[0],
                'business_team_description': result[1],
                'business_team_status': result[2],
                'created_date': result[3],
                'updated_date': result[4]
            }
            business_team_list.append(business_team_details)

        logging.info(f"inside get_business_team_list  business_team_list: {business_team_list}")
        if len(business_team_list) == 0:
            sts = "Failed"
            sts_description = "No matching business team rows found"



    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the business team details: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'business_team_list': business_team_list,
        'status': sts,
        'status_description': sts_description
    })


@initialsetup_blueprint.route('/api/get_functional_levels', methods=['GET', 'POST'])
@token_required
def get_functional_levels(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    search_query = data.get('search_query')
    parent_level_id = data.get('parent_level_id')
    traverse_to_lowest_level = data.get('traverse_to_lowest_level', False)

    logging.info(f"inside get_functional_levels corporate acc: {corporate_account}")
    logging.info(f"inside get_functional_levels project id: {project_id}")
    logging.info(f"inside get_functional_levels search text: {search_query}")
    logging.info(f"inside get_functional_levels parent level Id: {parent_level_id}")
    logging.info(f"inside get_functional_levels traverse_to_lowest_level: {traverse_to_lowest_level}")

    # Convert traverse_to_lowest_level to boolean if it's a string
    if isinstance(traverse_to_lowest_level, str):
        traverse_to_lowest_level = traverse_to_lowest_level.lower() == 'true'

    sts = "Success"
    sts_description = "Functional levels retrieved successfully"
    parent_level_description = 'None'
    functional_level_details = {}
    functional_level_list = []
    parent_of_parent = '0'

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        logging.info(f"Before the first IF -->> parent level Id: {parent_level_id}")

        if parent_level_id == '0' or parent_level_id == None or not parent_level_id:
            logging.info(f"Inside the first IF -->> parent level Id: {parent_level_id}")

            mySql_select_query = """SELECT LEVEL_ID FROM FUNCTIONAL_LEVELS WHERE PARENT_LEVEL_ID = 0 AND 
             STATUS = %s AND CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s """
            record = ('Active', corporate_account, project_id)

            cursor.execute(mySql_select_query, record)
            result = cursor.fetchone()

            if result:
                parent_level_id = result[0]
            else:
                logging.info(f"SQL call failed to make JK {mySql_select_query}")
                logging.info(f"SQL call failed to make JK {corporate_account} {project_id}")
                sts = "Failed"
                sts_description = "No matching functional levels found"

            logging.info(f"inside get_functional_levels parent level Id (AFTER): {parent_level_id}")

        if sts == 'Success':
            # Dictionary to store level_id to level_description mapping for hierarchy building
            level_descriptions = {}
            processed_level_ids = set()  # To track already processed level IDs

            # Get all direct children first
            mySql_select_query = """SELECT A.LEVEL_ID, A.PARENT_LEVEL_ID, A.LEVEL_DESCRIPTION,  
                A.CREATED_DATE, A.UPDATED_DATE, B.LEVEL_DESCRIPTION 
                FROM FUNCTIONAL_LEVELS A, FUNCTIONAL_LEVELS B 
                WHERE A.PARENT_LEVEL_ID = B.LEVEL_ID 
                AND A.STATUS = %s AND A.CORPORATE_ACCOUNT = %s AND A.PROJECT_ID = %s"""

            if search_query:
                mySql_select_query += " AND A.LEVEL_DESCRIPTION LIKE %s"
                record = ('Active', corporate_account, project_id, f"%{search_query}%")
            else:
                mySql_select_query += " AND A.PARENT_LEVEL_ID = %s"
                record = ('Active', corporate_account, project_id, parent_level_id)

            logging.info(f"Initial SQL : {mySql_select_query}")
            cursor.execute(mySql_select_query, record)

            # Process initial results
            initial_results = cursor.fetchall()
            for result in initial_results:
                level_id = result[0]
                parent_id = result[1]
                level_description = result[2]
                parent_description = result[5]

                # Store level descriptions for hierarchy building
                level_descriptions[level_id] = level_description
                level_descriptions[parent_id] = parent_description

                functional_level_details = {
                    'level_id': level_id,
                    'parent_level_id': parent_id,
                    'parent_level_description': parent_description,
                    'level_description': level_description,
                    'created_date': result[3],
                    'updated_date': result[4]
                }
                logging.info(f"Initial level: {functional_level_details}")
                functional_level_list.append(functional_level_details)
                processed_level_ids.add(level_id)

            # Now handle recursive traversal if needed
            if traverse_to_lowest_level and functional_level_list:
                logging.info(f"Starting traversal to lowest levels...")
                levels_to_process = [item['level_id'] for item in functional_level_list]

                while levels_to_process:
                    current_level_id = levels_to_process.pop(0)
                    logging.info(f"Processing children of level: {current_level_id}")

                    # Get children of current level
                    child_query = """SELECT A.LEVEL_ID, A.PARENT_LEVEL_ID, A.LEVEL_DESCRIPTION,  
                        A.CREATED_DATE, A.UPDATED_DATE, B.LEVEL_DESCRIPTION 
                        FROM FUNCTIONAL_LEVELS A, FUNCTIONAL_LEVELS B 
                        WHERE A.PARENT_LEVEL_ID = B.LEVEL_ID 
                        AND A.STATUS = %s AND A.CORPORATE_ACCOUNT = %s AND A.PROJECT_ID = %s 
                        AND A.PARENT_LEVEL_ID = %s"""
                    child_params = ('Active', corporate_account, project_id, current_level_id)

                    cursor.execute(child_query, child_params)
                    child_results = cursor.fetchall()

                    for child in child_results:
                        child_id = child[0]

                        # Skip if this level has already been processed
                        if child_id in processed_level_ids:
                            continue

                        parent_id = child[1]
                        level_description = child[2]
                        parent_description = child[5]

                        # Store level descriptions for hierarchy building
                        level_descriptions[child_id] = level_description
                        level_descriptions[parent_id] = parent_description

                        child_details = {
                            'level_id': child_id,
                            'parent_level_id': parent_id,
                            'parent_level_description': parent_description,
                            'level_description': level_description,
                            'created_date': child[3],
                            'updated_date': child[4]
                        }

                        logging.info(f"Found child level: {child_details}")
                        functional_level_list.append(child_details)
                        processed_level_ids.add(child_id)

                        # Add this child to our processing queue
                        levels_to_process.append(child_id)

            # Add functional_level_hierarchy to each item in the list
            for item in functional_level_list:
                # Build the hierarchy path
                hierarchy_path = []
                current_id = item['level_id']

                # Get the functional level description
                hierarchy_path.insert(0, item['level_description'])

                # Get parent description
                current_parent_id = item['parent_level_id']
                while current_parent_id != 0 and str(current_parent_id) != '0':
                    # Get parent level description from our mapping
                    if current_parent_id in level_descriptions:
                        parent_desc = level_descriptions[current_parent_id]
                        hierarchy_path.insert(0, parent_desc)

                    # Query for parent of the current parent
                    mySql_select_query = """SELECT PARENT_LEVEL_ID FROM FUNCTIONAL_LEVELS WHERE LEVEL_ID = %s AND CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s """
                    record = (current_parent_id,corporate_account, project_id)
                    cursor.execute(mySql_select_query, record)
                    result = cursor.fetchone()

                    if result:
                        current_parent_id = result[0]

                        # If we don't have this parent in our dictionary, fetch its description
                        if current_parent_id != 0 and current_parent_id not in level_descriptions:
                            mySql_select_query = """SELECT LEVEL_DESCRIPTION FROM FUNCTIONAL_LEVELS WHERE LEVEL_ID = %s AND CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s """
                            record = (current_parent_id, corporate_account, project_id)
                            cursor.execute(mySql_select_query, record)
                            desc_result = cursor.fetchone()

                            if desc_result:
                                level_descriptions[current_parent_id] = desc_result[0]
                                hierarchy_path.insert(0, desc_result[0])
                    else:
                        break

                # Create the hierarchy string
                # Check if we need to remove the topmost level
                if len(hierarchy_path) > 1:
                    # Get the root level ID (normally parent_level_id = 0)
                    root_level_query = """SELECT LEVEL_DESCRIPTION FROM FUNCTIONAL_LEVELS 
                        WHERE PARENT_LEVEL_ID = 0 AND STATUS = 'Active' 
                        AND CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s"""
                    root_level_params = (corporate_account, project_id)
                    cursor.execute(root_level_query, root_level_params)
                    root_level_result = cursor.fetchone()

                    # If we found the root level and it's at the start of our path, remove it
                    if root_level_result and hierarchy_path[0] == root_level_result[0]:
                        hierarchy_path = hierarchy_path[1:]

                item['functional_level_hierarchy'] = " > ".join(hierarchy_path)

            mySql_select_query = """SELECT PARENT_LEVEL_ID FROM FUNCTIONAL_LEVELS WHERE LEVEL_ID = %s AND CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s """
            record = (parent_level_id, corporate_account, project_id)
            cursor.execute(mySql_select_query, record)
            result = cursor.fetchone()
            if result:
                parent_of_parent = str(result[0])

            logging.info(f"Total levels in response: {len(functional_level_list)}")
            if len(functional_level_list) == 0:
                sts = "Failed"
                sts_description = "No matching functional levels found"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the functional level details: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'functional_level_list': functional_level_list,
        'parent_of_parent': parent_of_parent,
        'parent_level_id': str(parent_level_id),
        'status': sts,
        'status_description': sts_description
    })


@initialsetup_blueprint.route('/api/get_functional_level_path', methods=['GET','POST'])
@token_required
def get_functional_level_path(current_user):

    data = request.json
    corporate_account= data.get('corporate_account')
    project_id = data.get('project_id')
    level_id = data.get('level_id')

    logging.info(f"inside get_functional_levels corporate acc: {corporate_account}")
    logging.info(f"inside get_functional_levels project id: {project_id}")
    logging.info(f"inside get_functional_levels level Id: {level_id}")


    sts = "Success"
    sts_description = "Functional level path retrieved successfully"
    hierarchy_path = []

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        current_level = get_functional_level_details(corporate_account, project_id, level_id)

        if not current_level:
            return []

        logging.info(f"current_level : {current_level}")

        # Build path from bottom to top
        while current_level and current_level['parent_level_id'] != 0:
            hierarchy_path.insert(0, {
                'level_id': str(current_level['level_id']),
                'level_description': current_level['level_description']
            })
            current_level = get_functional_level_details(corporate_account, project_id, current_level['parent_level_id'])
            logging.info(f"current_level : {current_level}")

        # Add the root level if we found one
        if current_level:
            hierarchy_path.insert(0, {
                'level_id': str(current_level['level_id']),
                'level_description': current_level['level_description']
            })

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the functional level path: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


    return jsonify({
        'status': sts,
        'status_description': sts_description,
        "level_path": hierarchy_path
    })


@initialsetup_blueprint.route('/api/create_functional_level', methods=['POST'])
@token_required
@validate_access
def create_functional_level(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    parent_level_id = data.get('parent_level_id')
    level_description = data.get('level_description')

    logging.info(f"inside create_functional_levels : {data}")

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

    if not parent_level_id or parent_level_id == '0':
        parent_level_id = 0;
    else:
        if not validate_functional_level(corporate_account, project_id, parent_level_id):
            return jsonify({
                'status': 'Failed',
                'status_description': 'Parent functional level is not valid'
            })


    if not level_description.strip():
        return jsonify({
            'status': 'Failed',
            'status_description': 'Functional level description is required'
        })

    sts = "Success"
    sts_description = "Functional level added successfully"
    level_id = None
    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        mySql_select_query = f"""SELECT COUNT(*) FROM FUNCTIONAL_LEVELS WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s """
        record = (corporate_account, project_id)
        cursor.execute(mySql_select_query, record)
        result = cursor.fetchone()

        if result:
            if result[0] == 0:
                # insert the root level record
                mySql_insert_query = """INSERT INTO FUNCTIONAL_LEVELS (CORPORATE_ACCOUNT, PROJECT_ID, LEVEL_ID, 
                    PARENT_LEVEL_ID, LEVEL_DESCRIPTION, STATUS, CREATED_DATE, UPDATED_DATE) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""

                parent_level_id, seq_status, seq_status_description = generate_next_sequence(corporate_account, project_id,
                                                                                      'FUNCTIONAL_LEVEL_ID')
                if seq_status == "Failed":
                    sts = "Failed"
                    sts_description = seq_status_description
                    return jsonify({
                        'status': sts,
                        'status_description': sts_description
                    })
                record = (corporate_account, project_id, parent_level_id, 0, 'Root Level', 'Active', datetime.now(), datetime.now())
                cursor.execute(mySql_insert_query, record)
            connection.commit()


        mySql_insert_query = """INSERT INTO FUNCTIONAL_LEVELS (CORPORATE_ACCOUNT, PROJECT_ID, LEVEL_ID,  
        PARENT_LEVEL_ID, LEVEL_DESCRIPTION, STATUS, CREATED_DATE, UPDATED_DATE)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s) """

        level_id, seq_status, seq_status_description  = generate_next_sequence(corporate_account, project_id, 'FUNCTIONAL_LEVEL_ID')

        if seq_status == "Failed":
            sts = "Failed"
            sts_description = seq_status_description
            return jsonify({
                'status': sts,
                'status_description': sts_description
            })
        record = (corporate_account, project_id, level_id,   parent_level_id, level_description, 'Active', datetime.now(), datetime.now())
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
        'level_id': level_id,
        'status': sts,
        'status_description': sts_description
    })





@initialsetup_blueprint.route('/api/update_functional_level', methods=['POST'])
@token_required
@validate_access
def update_functional_level(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    level_id = data.get('level_id')
    level_description = data.get('level_description')

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

    if not level_id:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Functional level Id is required'
        })

    if not level_description.strip():
        return jsonify({
            'status': 'Failed',
            'status_description': 'Function level description is required'
        })

    sts = "Success"
    sts_description = "Functional level updated successfully"
    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_insert_query = """UPDATE FUNCTIONAL_LEVELS SET LEVEL_DESCRIPTION = %s, UPDATED_DATE = %s WHERE 
        CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND LEVEL_ID = %s """


        record = (level_description, datetime.now(), corporate_account, project_id, level_id)
        cursor.execute(mySql_insert_query, record)
        connection.commit()


        rows_impacted = cursor.rowcount
        if rows_impacted == 0:
            sts = "Failed"
            sts_description = "No matching functional level found to update"


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




@initialsetup_blueprint.route('/api/delete_functional_level', methods=['POST'])
@token_required
@validate_access
def delete_functional_level(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    level_ids = data.get('level_ids', [])  # Expect an array of IDs
    override_dependencies = data.get('override_dependencies', False)  # Expect a boolean value


    logging.info(f"input json: {data}")
    logging.info(f"input json: {level_ids}")


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

    if not level_ids or not isinstance(level_ids, list):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Level IDs must be provided as an array'
        })

    logging.info(f"override delete: {override_dependencies}")

    sts = "Success"
    sts_description = "Functional levels deleted successfully"

    for level_id in level_ids:
        sts, sts_description, sub_level_count, dependency_count , dependency_list  = get_functional_level_dependency_details(corporate_account, project_id, level_id)
        logging.info(f"NEW NEW level Id: {level_id} dependency count: {dependency_count}")

        if sts == 'Failed':
            return jsonify({
                'status': 'Failed',
                'status_description': sts_description
            })

        if dependency_count > 0 and not override_dependencies:
            return jsonify({
                'status': 'Failed',
                'status_description': f'One of the level you are trying to delete has {dependency_count} dependencies. Please remove the dependencies before deleting the level or select "Override Delete"'
            })


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
        placeholders = ','.join(['%s'] * len(level_ids))
        mySql_delete_query = f"""DELETE FROM FUNCTIONAL_LEVELS
            WHERE CORPORATE_ACCOUNT = %s 
            AND PROJECT_ID = %s 
            AND LEVEL_ID IN ({placeholders})"""

        # Prepare parameters: corporate_account, project_id, followed by all level_ids
        record = (corporate_account, project_id, *level_ids)

        cursor.execute(mySql_delete_query, record)
        deleted_count = cursor.rowcount

        if deleted_count == 0:
            sts = "Failed"
            sts_description = "No matching functional levels found to delete"
        else:
            sts_description = f"Successfully deleted {deleted_count} functional level(s)"

        # Commit the transaction
        connection.commit()

    except mysql.connector.Error as error:
        if connection:
            connection.rollback()  # Rollback in case of error
        sts = "Failed"
        sts_description = f"Failed to delete the functional levels: {error}"
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



@initialsetup_blueprint.route('/api/get_functional_level_dependencies', methods=['GET', 'POST'])
@token_required
def get_functional_level_dependencies(current_user):

    data = request.json
    corporate_account= data.get('corporate_account')
    project_id = data.get('project_id')
    level_id = data.get('level_id')


    if level_id == '0' or level_id == None or not level_id:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Level is required and has to be a non-zero value'
        })

    if not validate_level_id(corporate_account, project_id, level_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Level is invalid'
        })

    sts, sts_description, sub_level_count , dependency_count, dependency_list = get_functional_level_dependency_details(corporate_account, project_id, level_id)


    if sts == 'Failed':
        return jsonify({
            'status': 'Failed',
            'status_description': sts_description
        })


    return jsonify({
        'Total Dependencies': dependency_count,
        'Functional Levels Under This Levels': sub_level_count,
        'Other Dependencies': dependency_list,
        'status': sts,
        'status_description': sts_description
    })











@initialsetup_blueprint.route('/api/get_key_functional_attribute_categories', methods=['GET', 'POST'])
@token_required
def get_key_functional_attribute_categories(current_user):

    data = request.json
    corporate_account= data.get('corporate_account')
    project_id = data.get('project_id')
    search_query = data.get('search_query')

    logging.info(f"inside get_business_team_list corporate acc: {corporate_account}")
    logging.info(f"inside get_business_team_list project id: {project_id}")
    logging.info(f"inside get_business_team_list search text: {search_query}")


    sts = "Success"
    sts_description = "Key functional attribute categories retrieved successfully"
    attribute_category_details = {}
    attribute_category_list = []

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_select_query = """SELECT ATTRIBUTE_CATEGORY, CATEGORY_DESCRIPTION,
        CREATED_DATE, UPDATED_DATE FROM KEY_ATTRIBUTES_HEADER
        WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s """

        if search_query:


            mySql_select_query += " AND (ATTRIBUTE_CATEGORY LIKE %s OR CATEGORY_DESCRIPTION LIKE %s)"
            record = (corporate_account, project_id, f"%{search_query}%", f"%{search_query}%")

        else:
            record = (corporate_account, project_id)


        cursor.execute(mySql_select_query, record)
        for result in cursor.fetchall():

            attribute_category_details = {
                'attribute_category': result[0],
                'category_description': result[1],
                'created_date': result[2],
                'updated_date': result[3]
            }
            attribute_category_list.append(attribute_category_details)

        if len(attribute_category_list) == 0:
            sts = "Failed"
            sts_description = "No matching exception  categories found"



    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the functional attribute category details: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'attribute_category_list': attribute_category_list,
        'status': sts,
        'status_description': sts_description
    })






@initialsetup_blueprint.route('/api/get_key_functional_attributes', methods=['GET', 'POST'])
@token_required
def get_key_functional_attributes(current_user):

    data = request.json
    corporate_account= data.get('corporate_account')
    project_id = data.get('project_id')
    attribute_category = data.get('attribute_category')
    search_query = data.get('search_query')


    logging.info(f"inside get_attributes : {data}")



    sts = "Success"
    sts_description = "Key functional attribute(s) retrieved successfully"
    attribute_details = {}
    attribute_list = []

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_select_query = """SELECT ATTRIBUTE_CATEGORY, ATTRIBUTE_NAME, ATTRIBUTE_DESCRIPTION,
        CREATED_DATE, UPDATED_DATE, KEY_ATTRIBUTE_LIST_ID FROM KEY_ATTRIBUTES_LIST
        WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s """

        if search_query:
            if attribute_category:
                mySql_select_query += " AND ( ATTRIBUTE_NAME LIKE %s OR ATTRIBUTE_DESCRIPTION LIKE %s ) AND ATTRIBUTE_CATEGORY = %s"
                record = (corporate_account, project_id, f"%{search_query}%", f"%{search_query}%", attribute_category)
            else:
                mySql_select_query += " AND (ATTRIBUTE_NAME LIKE %s OR ATTRIBUTE_DESCRIPTION LIKE %s)"
                record = (corporate_account, project_id, f"%{search_query}%" , f"%{search_query}%")

        else:
            if attribute_category:
                mySql_select_query += " AND ATTRIBUTE_CATEGORY = %s"
                record = (corporate_account, project_id, attribute_category)
            else:
                record = (corporate_account, project_id)


        cursor.execute(mySql_select_query, record)
        for result in cursor.fetchall():

            attribute_details = {
                'attribute_category': result[0],
                'attribute_name': result[1],
                'category_description': result[2],
                'created_date': result[3],
                'updated_date': result[4],
                'key_attribute_list_id': result[5]

            }
            attribute_list.append(attribute_details)

        if len(attribute_list) == 0:
            sts = "Failed"
            sts_description = "No matching exception values found"


        logging.info('success all the way')

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the functional attribute category details: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'attribute_list': attribute_list,
        'status': sts,
        'status_description': sts_description
    })



@initialsetup_blueprint.route('/api/create_key_functional_attribute_categories', methods=['POST'])
@token_required
@validate_access
def create_key_functional_attribute_categories(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    attribute_category = data.get('attribute_category')
    category_description = data.get('category_description')



    logging.info(f"inside create_attribute_category : {data}")

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

    if not attribute_category.strip():
        return jsonify({
            'status': 'Failed',
            'status_description': 'Exception category is required'
        })


    sts = "Success"
    #sts = "Failed"
    sts_description = "Exception category added successfully"

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_insert_query = """INSERT INTO KEY_ATTRIBUTES_HEADER (CORPORATE_ACCOUNT, PROJECT_ID, ATTRIBUTE_CATEGORY, CATEGORY_DESCRIPTION, CREATED_DATE, UPDATED_DATE)
        VALUES (%s, %s, %s, %s, %s, %s) """

        record = (corporate_account, project_id, attribute_category, category_description,datetime.now(), datetime.now())
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
        'attribute_category': attribute_category,
        'status': sts,
        'status_description': sts_description
    })


@initialsetup_blueprint.route('/api/create_key_functional_attribute', methods=['POST'])
@token_required
@validate_access
def create_key_functional_attribute(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    attribute_category = data.get('attribute_category')
    attribute_name = data.get('attribute_name')
    attribute_description = data.get('attribute_description', '')  # Default to empty string if not provided

    logging.info(f"inside create_attribute_category : {data}")

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

    if not attribute_category:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Exception category is required'
        })

    if not validate_functional_attribute_category(corporate_account, project_id, attribute_category):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Exception category is not valid'
        })

    if not attribute_name:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Exception value is required'
        })

    # Fixed indentation on the line below
    key_attribute_list_id, seq_status, seq_status_description = generate_next_sequence(corporate_account, project_id,
                                                                                   'KEY_ATTRIBUTE_LIST')

    if seq_status == "Failed":
        return jsonify({
            'status': 'Failed',
            'status_description': seq_status_description
        })

    connection = None
    sts = "Success"
    # sts = "Failed"
    sts_description = "Exception value added successfully"

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        # Using parameterized query to prevent SQL injection
        mySql_insert_query = """
        INSERT INTO KEY_ATTRIBUTES_LIST 
        (CORPORATE_ACCOUNT, PROJECT_ID, KEY_ATTRIBUTE_LIST_ID, ATTRIBUTE_CATEGORY, ATTRIBUTE_NAME, 
        ATTRIBUTE_DESCRIPTION, CREATED_DATE, UPDATED_DATE)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        current_time = datetime.now()
        record = (
            corporate_account,
            project_id,
            key_attribute_list_id,
            attribute_category,
            attribute_name,
            attribute_description,
            current_time,
            current_time
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
        logging.error(f"Database error in create_key_functional_attribute: {error}")

    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'key_attribute_list_id': key_attribute_list_id,
        'status': sts,
        'status_description': sts_description
    })




@initialsetup_blueprint.route('/api/update_key_functional_attribute_categories', methods=['POST'])
@token_required
@validate_access
def update_key_functional_attribute_categories(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    attribute_category = data.get('attribute_category')
    category_description = data.get('category_description')

    logging.info(f"inside update key functional attribute category : {data}")

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


    if not attribute_category.strip():
        return jsonify({
            'status': 'Failed',
            'status_description': 'Exception Category is required'
        })


    if not validate_functional_attribute_category(corporate_account, project_id, attribute_category):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Exception category is not valid'
        })


    sts = "Success"
    #sts = "Failed"
    sts_description = "Exception category updated successfully"


    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        mySql_insert_query = """UPDATE KEY_ATTRIBUTES_HEADER SET CATEGORY_DESCRIPTION = %s, UPDATED_DATE = %s WHERE 
        CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND ATTRIBUTE_CATEGORY = %s """


        record = (category_description, datetime.now(), corporate_account, project_id, attribute_category)
        cursor.execute(mySql_insert_query, record)
        connection.commit()


        rows_impacted = cursor.rowcount
        if rows_impacted == 0:
            logging.info('hello 1')
            sts = "Failed"
            sts_description = "No matching exception category found to update"

            logging.info('hello 2')

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


@initialsetup_blueprint.route('/api/update_key_functional_attribute', methods=['POST'])
@token_required
@validate_access
def update_key_functional_attribute(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    key_attribute_list_id = data.get('key_attribute_list_id')
    attribute_category = data.get('attribute_category')
    attribute_name = data.get('attribute_name')
    attribute_description = data.get('attribute_description')

    logging.info(f"inside update key functional attribute : {data}")

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
    if not key_attribute_list_id:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Exception ID is required'
        })
    if not validate_project_id(corporate_account, project_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Project Id is not valid'
        })

    if not attribute_category.strip():
        return jsonify({
            'status': 'Failed',
            'status_description': 'Exception category is required'
        })

    if not validate_functional_attribute_category(corporate_account, project_id, attribute_category):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Exception category is not valid'
        })

    if not attribute_name.strip():
        return jsonify({
            'status': 'Failed',
            'status_description': 'Exception value is required'
        })

    if not attribute_description.strip():
        return jsonify({
            'status': 'Failed',
            'status_description': 'Description is required'
        })

    sts = "Success"
    #sts = "Failed"
    sts_description = "Exception value updated successfully"
    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_insert_query = """UPDATE KEY_ATTRIBUTES_LIST SET ATTRIBUTE_DESCRIPTION = %s,  ATTRIBUTE_CATEGORY = %s, ATTRIBUTE_NAME = %s, UPDATED_DATE = %s WHERE 
CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND KEY_ATTRIBUTE_LIST_ID = %s  """

        record = (attribute_description, attribute_category, attribute_name, datetime.now(), corporate_account, project_id, key_attribute_list_id)
        cursor.execute(mySql_insert_query, record)
        connection.commit()

        rows_impacted = cursor.rowcount
        if rows_impacted == 0:
            sts = "Failed"
            sts_description = "No matching exception found to update"


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




@initialsetup_blueprint.route('/api/delete_key_functional_attribute_categories', methods=['POST'])
@token_required
@validate_access
def delete_key_functional_attribute_categories(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    attribute_categories = data.get('attribute_categories', [])


    logging.info(f"input json: {data}")
    logging.info(f"input json: {attribute_categories}")


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

    if not attribute_categories or not isinstance(attribute_categories, list):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Exception categories must be provided as an array'
        })



    sts = "Success"
    sts_description = "Exception category deleted successfully"
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
        placeholders = ','.join(['%s'] * len(attribute_categories))
        mySql_delete_query = f"""DELETE FROM KEY_ATTRIBUTES_HEADER
            WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND ATTRIBUTE_CATEGORY IN ({placeholders})"""

        record = (corporate_account, project_id, *attribute_categories)

        cursor.execute(mySql_delete_query, record)
        deleted_count = cursor.rowcount

        if deleted_count == 0:
            sts = "Failed"
            sts_description = "No matching exception categories found to delete"
        else:
            sts_description = f"Successfully deleted {deleted_count} exception categories"

        # Commit the transaction
        connection.commit()

    except mysql.connector.Error as error:
        if connection:
            connection.rollback()  # Rollback in case of error
        sts = "Failed"
        sts_description = f"Failed to delete the exception categories: {error}"
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




@initialsetup_blueprint.route('/api/delete_key_functional_attribute', methods=['POST'])
@token_required
@validate_access
def delete_key_functional_attribute(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    attribute_category = data.get('attribute_category')
    attribute_names = data.get('attribute_names', [])


    logging.info(f"input json: {data}")
    logging.info(f"input json: {attribute_names}")


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

    if not attribute_category:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Exception category is required'
        })

    if not validate_functional_attribute_category(corporate_account, project_id, attribute_category):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Exception category is not valid'
        })

    if not attribute_names or not isinstance(attribute_names, list):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Exception value must be provided as an array'
        })



    sts = "Success"
    sts_description = "Exception value deleted successfully"
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
        placeholders = ','.join(['%s'] * len(attribute_names))
        mySql_delete_query = f"""DELETE FROM KEY_ATTRIBUTES_LIST
         WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND ATTRIBUTE_CATEGORY = %s AND ATTRIBUTE_NAME IN ({placeholders})"""

        record = (corporate_account, project_id, attribute_category, *attribute_names)

        cursor.execute(mySql_delete_query, record)
        deleted_count = cursor.rowcount

        if deleted_count == 0:
            sts = "Failed"
            sts_description = "No matching exception values found to delete"
        else:
            sts_description = f"Successfully deleted {deleted_count} exception value(s)"

        # Commit the transaction
        connection.commit()

    except mysql.connector.Error as error:
        if connection:
            connection.rollback()  # Rollback in case of error
        sts = "Failed"
        sts_description = f"Failed to delete exception values : {error}"
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


@initialsetup_blueprint.route('/api/create_status', methods=['POST'])
@token_required
@validate_access
def create_status(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    entity = data.get('entity')
    status = data.get('status')


    logging.info(f"inside create_status : {data}")

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
    if not entity:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Entity is required'
        })

    if not status:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Status is required'
        })


    sts = "Success"
    sts_description = "Status added successfully"

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_insert_query = """insert into ACCOUNT_STATUSES (CORPORATE_ACCOUNT, PROJECT_ID, ENTITY, STATUS, CREATED_DATE, UPDATED_DATE)
        values (%s, %s, %s, %s, %s , %s ) """

        record = (corporate_account, project_id,  entity, status,datetime.now(), datetime.now())
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


@initialsetup_blueprint.route('/api/delete_status', methods=['POST'])
@token_required
@validate_access
def delete_status(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    entity = data.get('entity')
    status_ids = data.get('status', [])  # Expect an array of IDs

    logging.info(f"input json: {data}")
    logging.info(f"status_ids: {status_ids}")

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

    if not entity:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Entity is required'
        })

    if not status_ids or not isinstance(status_ids, list):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Status must be provided as an array'
        })

    sts = "Success"
    sts_description = "Statuses deleted successfully"
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
        placeholders = ','.join(['%s'] * len(status_ids))




        mySql_select_query = f"""SELECT COUNT(*) FROM ACCOUNT_STATUSES 
            WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND ENTITY = %s """

        # Prepare parameters: corporate_account, project_id, entity, followed by all status_ids
        record = (corporate_account, project_id, entity)

        cursor.execute(mySql_select_query, record)
        result = cursor.fetchone()
        if result:
            if result[0] <= 1 or len(status_ids) >= result[0]:
                sts = "Failed"
                sts_description = "At least one status must be present for the status category"
                return jsonify({
                    'status': sts,
                    'status_description': sts_description
                })

        mySql_delete_query = f"""DELETE FROM ACCOUNT_STATUSES 
          WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND ENTITY = %s AND STATUS IN ({placeholders})"""

        # Prepare parameters: corporate_account, project_id, entity, followed by all status_ids
        record = (corporate_account, project_id, entity, *status_ids)

        cursor.execute(mySql_delete_query, record)
        deleted_count = cursor.rowcount

        if deleted_count == 0:
            sts = "Failed"
            sts_description = "No matching statuses found to delete"
        else:
            sts_description = f"Successfully deleted {deleted_count} status(es)"

        # Commit the transaction
        connection.commit()

    except mysql.connector.Error as error:
        if connection:
            connection.rollback()  # Rollback in case of error
        sts = "Failed"
        sts_description = f"Failed to delete the status details: {error}"
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

@initialsetup_blueprint.route('/api/delete_requirement_status', methods=['POST'])
@token_required
@validate_access
def delete_requirement_status(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    statuses = data.get('statuses', [])


    logging.info(f"input json: {data}")
    logging.info(f"input json: {statuses}")


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



    if not statuses or not isinstance(statuses, list):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Statuses must be provided as an array'
        })



    sts = "Success"
    sts_description = "Statuses deleted successfully"
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
        placeholders = ','.join(['%s'] * len(statuses))
        mySql_delete_query = f"""DELETE FROM ACCOUNT_STATUSES
           WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND STATUS IN ({placeholders})"""

        record = (corporate_account, project_id,   *statuses)

        cursor.execute(mySql_delete_query, record)
        deleted_count = cursor.rowcount

        if deleted_count == 0:
            sts = "Failed"
            sts_description = "No matching statuses found to delete"
        else:
            sts_description = f"Successfully deleted {deleted_count} statuses"

        # Commit the transaction
        connection.commit()

    except mysql.connector.Error as error:
        if connection:
            connection.rollback()  # Rollback in case of error
        sts = "Failed"
        sts_description = f"Failed to delete the statuses : {error}"
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



@initialsetup_blueprint.route('/api/get_statuses', methods=['GET', 'POST'])
@token_required
def get_statuses(current_user):

    data = request.json
    corporate_account= data.get('corporate_account')
    project_id = data.get('project_id')
    entity = data.get('entity')
    search_query = data.get('search_query')


    logging.info(f"inside get_statuses : {data}")



    sts = "Success"
    sts_description = "Requirement statuses retrieved successfully"
    status_details = {}
    status_list = []

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_select_query = """SELECT  STATUS, CREATED_DATE, UPDATED_DATE FROM ACCOUNT_STATUSES WHERE ENTITY = %s AND CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s  """

        if search_query:
                mySql_select_query += " AND STATUS LIKE %s"
                record = (entity, corporate_account, project_id, f"%{search_query}%")

        else:
                record = (entity, corporate_account, project_id)


        cursor.execute(mySql_select_query, record)

        logging.info(f" executed SQL is: {cursor._executed}")


        for result in cursor.fetchall():

            status_details = {
                'status': result[0],
                'created_date': result[1],
                'updated_date': result[2]
            }
            status_list.append(status_details)

        if len(status_list) == 0:
            sts = "Failed"
            sts_description = "No matching statuses found"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the status details: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'entity_statuses': status_list,
        'status': sts,
        'status_description': sts_description
    })



@initialsetup_blueprint.route('/api/get_status_headers', methods=['GET', 'POST'])
@token_required
def get_status_headers(current_user):

    data = request.json
    corporate_account= data.get('corporate_account')
    project_id = data.get('project_id')

    sts = "Success"
    sts_description = "Requirement status headers retrieved successfully"
    status_details = {}
    status_list = []

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_select_query = """SELECT  DISTINCT ENTITY FROM ACCOUNT_STATUSES WHERE  CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s """
        record = (corporate_account, project_id)


        cursor.execute(mySql_select_query, record)

        logging.info(f" executed SQL is: {cursor._executed}")


        for result in cursor.fetchall():

            status_details = {
                'entity': result[0]
            }
            status_list.append(status_details)

        if len(status_list) == 0:
            sts = "Failed"
            sts_description = "No matching statuses found"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the status details: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'entity_headers': status_list,
        'status': sts,
        'status_description': sts_description
    })


if __name__ == '__main__':
    app.run(debug=True)


