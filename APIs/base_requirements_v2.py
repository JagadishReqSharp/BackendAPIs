from flask import Flask, request, jsonify, Blueprint
import mysql.connector
from datetime import datetime
import config
import logging
from utils import token_required
from access_validation_at_api_level import validate_access
from foundational_v2 import generate_next_sequence, validate_functional_domain, validate_level_id, validate_req_id, validate_status, validate_user_id, is_user_authorized_to_approve, validate_project_id, validate_product_id,get_functional_level_children
from foundational_v2 import validate_corporate_account, validate_usecase_id


# Create a blueprint
base_requirements_blueprint = Blueprint('base_requirements', __name__)


app = Flask(__name__)

logging.basicConfig(filename='debugging.log', level=logging.DEBUG)



@base_requirements_blueprint.route('/api/create_product', methods=['POST'])
@token_required
@validate_access
def create_product(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    product_name = data.get('product_name')
    product_description = data.get('product_description')
    product_company = data.get('product_company')
    product_version = data.get('product_version')
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


    if not product_name:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Product name is required'
        })

    if not product_company:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Product company is required'
        })

    if status not in ['Active', 'Inactive']:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Status can be Active or Inactive'
        })

    # Generate the next sequence for product ID
    product_id, seq_status, seq_status_description = generate_next_sequence(9999,'', 'PRODUCT')

    if seq_status == "Failed":
        return jsonify({
            'status': 'Failed',
            'status_description': seq_status_description
        })

    sts = "Success"
    sts_description = "Product added successfully"

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        mySql_insert_query = """INSERT INTO PRODUCTS_BY_PROJECT 
        (CORPORATE_ACCOUNT, PROJECT_ID, PRODUCT_ID, PRODUCT_NAME, PRODUCT_DESCRIPTION, PRODUCT_COMPANY, 
        PRODUCT_VERSION, STATUS, CREATED_DATE, UPDATED_DATE)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """

        record = (corporate_account, project_id, product_id, product_name, product_description,
                  product_company, product_version, status, datetime.now(), datetime.now())
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
        'status_description': sts_description,
        'product_id': product_id if sts == "Success" else None
    })


@base_requirements_blueprint.route('/api/update_product', methods=['POST'])
@token_required
@validate_access
def update_product(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    product_id = data.get('product_id')
    product_name = data.get('product_name')
    product_description = data.get('product_description')
    product_company = data.get('product_company')
    product_version = data.get('product_version')
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
    if not product_id:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Product ID is required'
        })


    if not product_name:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Product name is required'
        })

    if not product_company:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Product company is required'
        })

    if  status not in ['Active', 'Inactive']:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Status can be Active or Inactive'
        })

    sts = "Success"
    sts_description = "Product updated successfully"
    connection = None

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        # Start transaction
        connection.start_transaction()

        # First check if the product exists
        check_query = "SELECT PRODUCT_ID FROM PRODUCTS_BY_PROJECT WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND PRODUCT_ID = %s"
        cursor.execute(check_query, (corporate_account,project_id, product_id))
        if not cursor.fetchone():
            return jsonify({
                'status': 'Failed',
                'status_description': 'Product not found'
            })

        # Build dynamic update query based on provided fields
        update_parts = []
        update_values = []

        update_parts.append("PRODUCT_NAME = %s")
        update_values.append(product_name)

        update_parts.append("PRODUCT_DESCRIPTION = %s")
        update_values.append(product_description)

        update_parts.append("PRODUCT_COMPANY = %s")
        update_values.append(product_company)

        update_parts.append("PRODUCT_VERSION = %s")
        update_values.append(product_version)

        update_parts.append("STATUS = %s")
        update_values.append(status)

        # Always update the updated_date
        update_parts.append("UPDATED_DATE = %s")
        update_values.append(datetime.now())

        # Add functional_domain and product_id at the end for WHERE clause
        update_values.append(corporate_account)
        update_values.append(project_id)
        update_values.append(product_id)


        update_query = f"""UPDATE PRODUCTS_BY_PROJECT 
                        SET {', '.join(update_parts)} 
                        WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND PRODUCT_ID = %s"""

        cursor.execute(update_query, tuple(update_values))

        if cursor.rowcount == 0:
            sts = "Failed"
            sts_description = "No changes made to the product"

        # Commit the transaction
        connection.commit()

    except mysql.connector.Error as error:
        if connection:
            connection.rollback()  # Rollback in case of error
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
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'status': sts,
        'status_description': sts_description
    })


@base_requirements_blueprint.route('/api/delete_product', methods=['POST'])
@token_required
@validate_access
def delete_product(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    product_ids = data.get('product_ids', [])  # Expect an array of IDs

    logging.info(f"input json: {data}")
    logging.info(f"input json: {product_ids}")

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

    if not product_ids or not isinstance(product_ids, list):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Product IDs must be provided as an array'
        })

    sts = "Success"
    sts_description = "Products deleted successfully"
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
        placeholders = ','.join(['%s'] * len(product_ids))
        mySql_delete_query = f"""DELETE FROM PRODUCTS_BY_PROJECT 
            WHERE CORPORATE_ACCOUNT = %s 
            AND PROJECT_ID = %s 
            AND PRODUCT_ID IN ({placeholders})"""

        # Prepare parameters: corporate_account, project_id, followed by all product_ids
        record = (corporate_account, project_id, *product_ids)

        cursor.execute(mySql_delete_query, record)
        deleted_count = cursor.rowcount

        if deleted_count == 0:
            sts = "Failed"
            sts_description = "No matching products found to delete"
        else:
            sts_description = f"Successfully deleted {deleted_count} product(s)"

        # Commit the transaction
        connection.commit()

    except mysql.connector.Error as error:
        if connection:
            connection.rollback()  # Rollback in case of error
        sts = "Failed"
        sts_description = f"Failed to delete the product details: {error}"
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

@base_requirements_blueprint.route('/api/get_product_list', methods=['GET', 'POST'])
@token_required
def get_product_list(current_user):
    data = request.json if request.is_json else {}
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    status_filter = data.get('status')

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
    sts_description = "Products retrieved successfully"
    product_list = []

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        # Base query
        query = """SELECT PROJECT_ID, PRODUCT_ID, PRODUCT_NAME, PRODUCT_DESCRIPTION, 
                  PRODUCT_COMPANY, PRODUCT_VERSION, STATUS, CREATED_DATE, UPDATED_DATE, CORPORATE_ACCOUNT
                  FROM PRODUCTS_BY_PROJECT"""

        conditions = []
        params = []

        # Add filters if provided
        if corporate_account:
            conditions.append("CORPORATE_ACCOUNT = %s")
            params.append(corporate_account)

        if project_id:
            conditions.append("PROJECT_ID = %s")
            params.append(project_id)

        if status_filter:
            conditions.append("STATUS = %s")
            params.append(status_filter)

        # Add WHERE clause if conditions exist
        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        # Add order by clause
        query += " ORDER BY PRODUCT_NAME"

        cursor.execute(query, tuple(params))

        for result in cursor.fetchall():
            product_details = {
                'project_id': result[0],
                'product_id': result[1],
                'product_name': result[2],
                'product_description': result[3],
                'product_company': result[4],
                'product_version': result[5],
                'status': result[6],
                'created_date': result[7],
                'updated_date': result[8],
                'corporate_account': result[9]

            }
            product_list.append(product_details)

        if len(product_list) == 0:
            sts = "Success"  # Still success but with empty list
            sts_description = "No products found matching the criteria"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the product list: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'product_list': product_list,
        'status': sts,
        'status_description': sts_description
    })


@base_requirements_blueprint.route('/api/get_product_details', methods=['GET', 'POST'])
@token_required
def get_product_details(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    product_id = data.get('product_id')

    if not corporate_account or not project_id  or not product_id:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Corporate Account, Project Id and Product ID are required'
        })

    sts = "Success"
    sts_description = "Product details retrieved successfully"
    product_details = {}

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        query = """SELECT PROJECT_ID, PRODUCT_ID, PRODUCT_NAME, PRODUCT_DESCRIPTION, 
                  PRODUCT_COMPANY, PRODUCT_VERSION, STATUS, CREATED_DATE, UPDATED_DATE, CORPORATE_ACCOUNT
                  FROM PRODUCTS_BY_PROJECT 
                  WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND PRODUCT_ID = %s"""

        cursor.execute(query, (corporate_account, project_id, product_id))

        result = cursor.fetchone()

        if result:
            product_details = {
                'project_id': result[0],
                'product_id': result[1],
                'product_name': result[2],
                'product_description': result[3],
                'product_company': result[4],
                'product_version': result[5],
                'status': result[6],
                'created_date': result[7],
                'updated_date': result[8],
                'corporate_account': result[9]
            }
        else:
            sts = "Failed"
            sts_description = "No product found with the provided domain and ID"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the product details: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'product_details': product_details,
        'status': sts,
        'status_description': sts_description
    })


@base_requirements_blueprint.route('/api/update_requirement_classification', methods=['POST'])
@token_required
@validate_access
def update_requirement_classification(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    req_ids = data.get('req_ids')  # Changed from req_id to req_ids (array)
    product_id = data.get('product_id')
    req_classification = data.get('req_classification')

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

    if not product_id:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Product ID is required'
        })

    if not req_ids or not isinstance(req_ids, list) or len(req_ids) == 0:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Req IDs array is required and cannot be empty'
        })

    # Validate all req_ids
    for req_id in req_ids:
        if not validate_req_id(corporate_account, project_id, req_id):
            return jsonify({
                'status': 'Failed',
                'status_description': f'Requirement Id {req_id} is not valid'
            })

    if not validate_product_id(corporate_account, project_id, product_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Product Id is not valid'
        })

    if not validate_status(corporate_account, project_id, 'REQUIREMENT_CLASSIFICATION', req_classification):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Invalid requirement classification'
        })

    sts = "Success"
    sts_description = "Requirement classification updated successfully"
    failed_req_ids = []
    connection = None

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        # Start transaction
        connection.start_transaction()

        # Process each req_id in the array
        for req_id in req_ids:
            try:
                # First check if the product exists
                mySql_select_query = "SELECT PRODUCT_ID FROM REQUIREMENT_CLASSIFICATION WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND PRODUCT_ID = %s AND REQ_ID = %s"
                cursor.execute(mySql_select_query, (corporate_account, project_id, product_id, req_id))

                if not cursor.fetchone():
                    logging.info(f"Requirement classification not found for req_id {req_id}. Inserting new record.")
                    logging.info(f"Corporate Account: {corporate_account}, Project ID: {project_id}, Product ID: {product_id}, Req ID: {req_id}, Req Classification: {req_classification}")
                    mySql_insert_query = """INSERT INTO REQUIREMENT_CLASSIFICATION (CORPORATE_ACCOUNT, PROJECT_ID, REQ_ID, PRODUCT_ID, REQ_CLASSIFICATION, CREATED_DATE, UPDATED_DATE)
                                        VALUES (%s, %s, %s, %s, %s, %s, %s) """
                    record = (
                        corporate_account, project_id, req_id, product_id, req_classification, datetime.now(),
                        datetime.now())
                    cursor.execute(mySql_insert_query, record)
                    logging.info(f" executed INSERT SQL is: {cursor._executed}")

                else:
                    logging.info(f"Requirement classification found for req_id {req_id}. Updating existing record.")
                    # Build dynamic update query based on provided fields
                    update_parts = []
                    update_values = []

                    update_parts.append("REQ_CLASSIFICATION = %s")
                    update_values.append(req_classification)

                    # Always update the updated_date
                    update_parts.append("UPDATED_DATE = %s")
                    update_values.append(datetime.now())

                    # Add corporate_account, project_id, product_id, and req_id at the end for WHERE clause
                    update_values.append(corporate_account)
                    update_values.append(project_id)
                    update_values.append(product_id)
                    update_values.append(req_id)

                    update_query = f"""UPDATE REQUIREMENT_CLASSIFICATION 
                                    SET {', '.join(update_parts)} 
                                    WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND PRODUCT_ID = %s AND REQ_ID = %s"""

                    cursor.execute(update_query, tuple(update_values))
                    logging.info(f" executed UPDATE SQL is: {cursor._executed}")
            except Exception as e:
                # Track which req_ids failed
                failed_req_ids.append(req_id)
                logging.info(f"Error processing req_id {req_id}: {str(e)}")

        # Commit the transaction if any req_ids were successful
        if len(failed_req_ids) < len(req_ids):
            connection.commit()

            # Update status message if some but not all req_ids failed
            if failed_req_ids:
                sts = "Partial Success"
                sts_description = f"Some requirement classifications updated successfully. Failed for req_ids: {', '.join(map(str, failed_req_ids))}"

    except mysql.connector.Error as error:
        if connection:
            connection.rollback()  # Rollback in case of error
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
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'status': sts,
        'status_description': sts_description,
        'failed_req_ids': failed_req_ids
    })

@base_requirements_blueprint.route('/api/get_requirements_list', methods=['GET', 'POST'])
@token_required
def get_requirements_list(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    level_id = data.get('level_id')
    product_ids = data.get('product_ids', [])
    filter_by_status = data.get('filter_by_status', [])
    search_query = data.get('search_query')
    sort_criteria = data.get('sort_criteria')
    # Add new filter parameters
    requirement_criticality = data.get('requirement_criticality', [])
    requirement_priority = data.get('requirement_priority', [])
    created_date_start = data.get('created_date_start')
    created_date_end = data.get('created_date_end')
    updated_date_start = data.get('updated_date_start')
    updated_date_end = data.get('updated_date_end')
    include_child_levels_flag = data.get('include_child_levels_flag', False)



    logging.info(f"data : {data}")

    # if not filter_by_status or not isinstance(filter_by_status, list):
    #     return jsonify({
    #         'status': 'Failed',
    #         'status_description': 'Filter by status must be provided as an array'
    #     })

    sts = "Success"
    sts_description = "Requirements retrieved successfully"
    requirement_details = {}
    requirement_list = []

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

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


        # Base query without ORDER BY

        product_query = """ (SELECT A.REQ_ID, X.PRODUCT_ID, Y.PRODUCT_NAME, X.REQ_CLASSIFICATION, X.CREATED_DATE, X.UPDATED_DATE
         FROM REQ A LEFT OUTER JOIN REQUIREMENT_CLASSIFICATION X ON
         A.CORPORATE_ACCOUNT = X.CORPORATE_ACCOUNT AND A.PROJECT_ID = X.PROJECT_ID AND A.REQ_ID = X.REQ_ID
         LEFT OUTER JOIN PRODUCTS_BY_PROJECT Y ON X.PRODUCT_ID = Y.PRODUCT_ID AND X.CORPORATE_ACCOUNT = Y.CORPORATE_ACCOUNT AND X.PROJECT_ID = Y.PROJECT_ID
         , FUNCTIONAL_LEVELS B WHERE
         A.CORPORATE_ACCOUNT = B.CORPORATE_ACCOUNT AND A.PROJECT_ID = B.PROJECT_ID AND A.LEVEL_ID = B.LEVEL_ID AND
          X.PRODUCT_ID = """


        mySql_select_query = f"""SELECT A.REQ_ID, A.CORPORATE_ACCOUNT, A.PROJECT_ID, A.REQ_ID_WITH_PREFIX, A.LEVEL_ID, B.LEVEL_DESCRIPTION, A.REQ_DESCRIPTION, A.STATUS,
            A.REF_FIELD_1, A.REF_FIELD_2, A.REF_FIELD_3, A.REF_FIELD_4, A.REQ_PRIORITY, A.REQ_CRITICALITY, A.CREATED_DATE, A.UPDATED_DATE
            , (SELECT COUNT(*) FROM KEY_ATTRIBUTES_LIST_REQUIREMENTS C WHERE A.CORPORATE_ACCOUNT = C.CORPORATE_ACCOUNT AND A.PROJECT_ID = C.PROJECT_ID AND C.REQ_ID = A.REQ_ID) NUMBER_OF_EXCEPTIONS
            , (SELECT COUNT(*) FROM REQUIREMENTS_APPROVERS D WHERE A.CORPORATE_ACCOUNT = D.CORPORATE_ACCOUNT AND A.PROJECT_ID = D.PROJECT_ID AND A.REQ_ID = D.REQ_ID) NUMBER_OF_APPROVERS
            FROM REQUIREMENTS A, FUNCTIONAL_LEVELS B WHERE
            A.CORPORATE_ACCOUNT = B.CORPORATE_ACCOUNT AND A.PROJECT_ID = B.PROJECT_ID AND A.LEVEL_ID = B.LEVEL_ID AND
            A.CORPORATE_ACCOUNT = %s AND A.PROJECT_ID = %s """


        params = [corporate_account, project_id]

        if filter_by_status and isinstance(filter_by_status, list) and len(
                    filter_by_status) > 0:
                placeholders = ','.join(['%s'] * len(filter_by_status))
                mySql_select_query += f" AND A.STATUS IN({placeholders}) "
                params.extend(filter_by_status)

        if requirement_criticality and isinstance(requirement_criticality, list) and len(requirement_criticality) > 0:
            criticality_placeholders = ','.join(['%s'] * len(requirement_criticality))
            mySql_select_query += f" AND A.REQ_CRITICALITY IN({criticality_placeholders}) "
            params.extend(requirement_criticality)

        # Add priority filter if provided
        if requirement_priority and isinstance(requirement_priority, list) and len(requirement_priority) > 0:
            priority_placeholders = ','.join(['%s'] * len(requirement_priority))
            mySql_select_query += f" AND A.REQ_PRIORITY IN({priority_placeholders}) "
            params.extend(requirement_priority)

        # Add created_date range filter if provided
        if created_date_start:
            mySql_select_query += " AND A.CREATED_DATE >= %s "
            params.append(created_date_start)

        if created_date_end:
            mySql_select_query += " AND DATE(A.CREATED_DATE) <= DATE(%s) "
            params.append(created_date_end)

        # Add updated_date range filter if provided
        if updated_date_start:
            mySql_select_query += " AND A.UPDATED_DATE >= %s "
            params.append(updated_date_start)

        if updated_date_end:
            mySql_select_query += " AND DATE(A.UPDATED_DATE) <= DATE(%s) "
            params.append(updated_date_end)


        if search_query:
            if level_id:
                mySql_select_query += level_condition
                params.extend(child_levels_list)
                mySql_select_query += " AND ( A.REQ_ID_WITH_PREFIX = %s OR A.REQ_DESCRIPTION LIKE %s) "
                params.extend([search_query, f"%{search_query}%"])
            else:
                mySql_select_query += " AND ( A.REQ_ID_WITH_PREFIX = %s OR A.REQ_DESCRIPTION LIKE %s) "
                params.extend([search_query, f"%{search_query}%"])
        else:
            if level_id:
                mySql_select_query += level_condition
                params.extend(child_levels_list)


        if not product_ids or len(product_ids) == 0:
            # If no product_ids are provided, just select the requirements
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
                final_sql_query = "WITH REQ AS ( " + mySql_select_query + " ), "
                final_sql_query += "PRODUCT_1 AS " + product_query + product_ids[0] + " )"
                final_sql_query += """SELECT P.*,
                Q.PRODUCT_ID PRODUCT_ID_1, Q.PRODUCT_NAME PRODUCT_NAME_1, Q.REQ_CLASSIFICATION REQ_CLASSIFICATION_1, Q.CREATED_DATE CREATED_DATE_1, Q.UPDATED_DATE UPDATED_DATE_1
                 FROM REQ P
                LEFT OUTER JOIN PRODUCT_1 Q ON P.REQ_ID = Q.REQ_ID"""
            elif len(product_ids) == 2:
                final_sql_query = "WITH REQ AS ( " + mySql_select_query + " ), "
                final_sql_query += "PRODUCT_1 AS " + product_query + product_ids[0] + " ),"
                final_sql_query += "PRODUCT_2 AS " + product_query + product_ids[1] + " )"
                final_sql_query += """SELECT P.*,
                Q.PRODUCT_ID PRODUCT_ID_1, Q.PRODUCT_NAME PRODUCT_NAME_1, Q.REQ_CLASSIFICATION REQ_CLASSIFICATION_1, Q.CREATED_DATE CREATED_DATE_1, Q.UPDATED_DATE UPDATED_DATE_1,
                R.PRODUCT_ID PRODUCT_ID_2, R.PRODUCT_NAME PRODUCT_NAME_2, R.REQ_CLASSIFICATION REQ_CLASSIFICATION_2, R.CREATED_DATE CREATED_DATE_2, R.UPDATED_DATE UPDATED_DATE_2
                FROM REQ P
                   LEFT OUTER JOIN PRODUCT_1 Q ON P.REQ_ID = Q.REQ_ID
                   LEFT OUTER JOIN PRODUCT_2 R ON P.REQ_ID = R.REQ_ID """
            elif len(product_ids) == 3:
                final_sql_query = "WITH REQ AS ( " + mySql_select_query + " ), "
                final_sql_query += "PRODUCT_1 AS " + product_query + product_ids[0] + " ),"
                final_sql_query += "PRODUCT_2 AS " + product_query + product_ids[1] + " ),"
                final_sql_query += "PRODUCT_3 AS " + product_query + product_ids[2] + " )"
                final_sql_query += """SELECT P.*,
                Q.PRODUCT_ID PRODUCT_ID_1, Q.PRODUCT_NAME PRODUCT_NAME_1, Q.REQ_CLASSIFICATION REQ_CLASSIFICATION_1, Q.CREATED_DATE CREATED_DATE_1, Q.UPDATED_DATE UPDATED_DATE_1,
                R.PRODUCT_ID PRODUCT_ID_2, R.PRODUCT_NAME PRODUCT_NAME_2, R.REQ_CLASSIFICATION REQ_CLASSIFICATION_2, R.CREATED_DATE CREATED_DATE_2, R.UPDATED_DATE UPDATED_DATE_2,
                S.PRODUCT_ID PRODUCT_ID_3, S.PRODUCT_NAME PRODUCT_NAME_3, S.REQ_CLASSIFICATION REQ_CLASSIFICATION_3, S.CREATED_DATE CREATED_DATE_3, S.UPDATED_DATE UPDATED_DATE_3
                FROM REQ P
                LEFT OUTER JOIN PRODUCT_1 Q ON P.REQ_ID = Q.REQ_ID
                LEFT OUTER JOIN PRODUCT_2 R ON P.REQ_ID = R.REQ_ID
                LEFT OUTER JOIN PRODUCT_3 S ON P.REQ_ID = S.REQ_ID """
            elif len(product_ids) == 4:
                final_sql_query = "WITH REQ AS ( " + mySql_select_query + " ), "
                final_sql_query += "PRODUCT_1 AS " + product_query + product_ids[0] + " ),"
                final_sql_query += "PRODUCT_2 AS " + product_query + product_ids[1] + " ),"
                final_sql_query += "PRODUCT_3 AS " + product_query + product_ids[2] + " ),"
                final_sql_query += "PRODUCT_4 AS " + product_query + product_ids[3] + " )"
                final_sql_query += """SELECT P.*,
                Q.PRODUCT_ID PRODUCT_ID_1, Q.PRODUCT_NAME PRODUCT_NAME_1, Q.REQ_CLASSIFICATION REQ_CLASSIFICATION_1, Q.CREATED_DATE CREATED_DATE_1, Q.UPDATED_DATE UPDATED_DATE_1,
                R.PRODUCT_ID PRODUCT_ID_2, R.PRODUCT_NAME PRODUCT_NAME_2, R.REQ_CLASSIFICATION REQ_CLASSIFICATION_2, R.CREATED_DATE CREATED_DATE_2, R.UPDATED_DATE UPDATED_DATE_2,
                S.PRODUCT_ID PRODUCT_ID_3, S.PRODUCT_NAME PRODUCT_NAME_3, S.REQ_CLASSIFICATION REQ_CLASSIFICATION_3, S.CREATED_DATE CREATED_DATE_3, S.UPDATED_DATE UPDATED_DATE_3,
                T.PRODUCT_ID PRODUCT_ID_4, T.PRODUCT_NAME PRODUCT_NAME_4, T.REQ_CLASSIFICATION REQ_CLASSIFICATION_4, T.CREATED_DATE CREATED_DATE_4, T.UPDATED_DATE UPDATED_DATE_4
                FROM REQ P
                LEFT OUTER JOIN PRODUCT_1 Q ON P.REQ_ID = Q.REQ_ID
                LEFT OUTER JOIN PRODUCT_2 R ON P.REQ_ID = R.REQ_ID
                LEFT OUTER JOIN PRODUCT_3 S ON P.REQ_ID = S.REQ_ID  
                LEFT OUTER JOIN PRODUCT_4 T ON P.REQ_ID = T.REQ_ID """
            elif len(product_ids) == 5:
                final_sql_query = "WITH REQ AS ( " + mySql_select_query + " ), "
                final_sql_query += "PRODUCT_1 AS " + product_query + product_ids[0] + " ),"
                final_sql_query += "PRODUCT_2 AS " + product_query + product_ids[1] + " ),"
                final_sql_query += "PRODUCT_3 AS " + product_query + product_ids[2] + " ),"
                final_sql_query += "PRODUCT_4 AS " + product_query + product_ids[3] + " ),"
                final_sql_query += "PRODUCT_5 AS " + product_query + product_ids[4] + " )"
                final_sql_query += """SELECT P.*,
                Q.PRODUCT_ID PRODUCT_ID_1, Q.PRODUCT_NAME PRODUCT_NAME_1, Q.REQ_CLASSIFICATION REQ_CLASSIFICATION_1, Q.CREATED_DATE CREATED_DATE_1, Q.UPDATED_DATE UPDATED_DATE_1,
                R.PRODUCT_ID PRODUCT_ID_2, R.PRODUCT_NAME PRODUCT_NAME_2, R.REQ_CLASSIFICATION REQ_CLASSIFICATION_2, R.CREATED_DATE CREATED_DATE_2, R.UPDATED_DATE UPDATED_DATE_2,
                S.PRODUCT_ID PRODUCT_ID_3, S.PRODUCT_NAME PRODUCT_NAME_3, S.REQ_CLASSIFICATION REQ_CLASSIFICATION_3, S.CREATED_DATE CREATED_DATE_3, S.UPDATED_DATE UPDATED_DATE_3,
                T.PRODUCT_ID PRODUCT_ID_4, T.PRODUCT_NAME PRODUCT_NAME_4, T.REQ_CLASSIFICATION REQ_CLASSIFICATION_4, T.CREATED_DATE CREATED_DATE_4, T.UPDATED_DATE UPDATED_DATE_4,
                U.PRODUCT_ID PRODUCT_ID_5, U.PRODUCT_NAME PRODUCT_NAME_5, U.REQ_CLASSIFICATION REQ_CLASSIFICATION_5, U.CREATED_DATE CREATED_DATE_5, U.UPDATED_DATE UPDATED_DATE_5
                FROM REQ P
                LEFT OUTER JOIN PRODUCT_1 Q ON P.REQ_ID = Q.REQ_ID
                LEFT OUTER JOIN PRODUCT_2 R ON P.REQ_ID = R.REQ_ID
                LEFT OUTER JOIN PRODUCT_3 S ON P.REQ_ID = S.REQ_ID  
                LEFT OUTER JOIN PRODUCT_4 T ON P.REQ_ID = T.REQ_ID 
                LEFT OUTER JOIN PRODUCT_5 U ON P.REQ_ID = U.REQ_ID """

        if not sort_criteria:
            sort_criteria = 'REQ_ID'

        # Add ORDER BY at the end
        final_sql_query += " ORDER BY " + sort_criteria

        logging.info(f" Prepared SQL is: {final_sql_query}")

        cursor.execute(final_sql_query, tuple(params))
        column_names = [desc[0] for desc in cursor.description]  # Get column names from cursor

        logging.info(f" executed SQL is: {cursor._executed}")


        for result in cursor.fetchall():

            result_dict = dict(zip(column_names, result))

            product_1_classification = None
            product_2_classification = None
            product_3_classification = None
            product_4_classification = None
            product_5_classification = None

            if len(product_ids) >= 1:
                product_1_classification = {
                    'product_id': result_dict['PRODUCT_ID_1'],
                    'product_name': result_dict['PRODUCT_NAME_1'],
                    'product_classification':  result_dict['REQ_CLASSIFICATION_1'],
                    'created_date': result_dict['CREATED_DATE_1'],
                    'updated_date': result_dict['UPDATED_DATE_1']
                }
                logging.info(f"product_1_classification: {product_1_classification}")
            if len(product_ids) >=2:
                product_2_classification = {
                    'product_id': result_dict['PRODUCT_ID_2'],
                    'product_name': result_dict['PRODUCT_NAME_2'],
                    'product_classification':  result_dict['REQ_CLASSIFICATION_2'],
                    'created_date': result_dict['CREATED_DATE_2'],
                    'updated_date': result_dict['UPDATED_DATE_2']
                }
            if len(product_ids) >=3:
                product_3_classification = {
                    'product_id': result_dict['PRODUCT_ID_3'],
                    'product_name': result_dict['PRODUCT_NAME_3'],
                    'product_classification':  result_dict['REQ_CLASSIFICATION_3'],
                    'created_date': result_dict['CREATED_DATE_3'],
                    'updated_date': result_dict['UPDATED_DATE_3']
                }
            if len(product_ids) >=4:
                product_4_classification = {
                    'product_id': result_dict['PRODUCT_ID_4'],
                    'product_name': result_dict['PRODUCT_NAME_4'],
                    'product_classification':  result_dict['REQ_CLASSIFICATION_4'],
                    'created_date': result_dict['CREATED_DATE_4'],
                    'updated_date': result_dict['UPDATED_DATE_4']
                }
            if len(product_ids) >=5:
                product_5_classification = {
                    'product_id': result_dict['PRODUCT_ID_5'],
                    'product_name': result_dict['PRODUCT_NAME_5'],
                    'product_classification':  result_dict['REQ_CLASSIFICATION_5'],
                    'created_date': result_dict['CREATED_DATE_5'],
                    'updated_date': result_dict['UPDATED_DATE_5']
                }

            requirement_details = {
                'req_id': result_dict['REQ_ID'],
                'req_id_with_prefix': result_dict['REQ_ID_WITH_PREFIX'],
                'level_id': result_dict['LEVEL_ID'],
                'level_description': result_dict['LEVEL_DESCRIPTION'],
                'req_description': result_dict['REQ_DESCRIPTION'],
                'status': result_dict['STATUS'],
                'ref_field_1': result_dict['REF_FIELD_1'],
                'ref_field_2': result_dict['REF_FIELD_2'],
                'ref_field_3': result_dict['REF_FIELD_3'],
                'ref_field_4': result_dict['REF_FIELD_4'],
                'req_priority': result_dict['REQ_PRIORITY'],
                'req_criticality': result_dict['REQ_CRITICALITY'],
                'number_of_exceptions': result_dict['NUMBER_OF_EXCEPTIONS'],
                'number_of_approvers': result_dict['NUMBER_OF_APPROVERS'],
                'created_date': result_dict['CREATED_DATE'],
                'updated_date': result_dict['UPDATED_DATE'],
                'product_1_classification': product_1_classification,
                'product_2_classification': product_2_classification,
                'product_3_classification': product_3_classification,
                'product_4_classification': product_4_classification,
                'product_5_classification': product_5_classification
            }
            requirement_list.append(requirement_details)

        if len(requirement_list) == 0:
            sts = "Failed"
            sts_description = "No matching requirements found"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the requirement details: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'requirement_list': requirement_list,
        'status': sts,
        'status_description': sts_description
    })


@base_requirements_blueprint.route('/api/create_product_level_user_setup', methods=['POST'])
@token_required
@validate_access
def create_product_level_user_setup(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    product_id = data.get('product_id')
    user_corporate_account = data.get('user_corporate_account')
    user_id = data.get('user_id')
    access_level = data.get('access_level')

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
    if not validate_product_id(corporate_account, project_id, product_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Product Id is not valid'
        })
    if not user_corporate_account:
        return jsonify({
            'status': 'Failed',
            'status_description': 'User corporate account is required'
        })
    if not validate_user_id(user_corporate_account, user_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'User Id is not valid'
        })
    if not access_level or not isinstance(access_level, int):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Access level is required and must be an integer'
        })

    sts = "Success"
    sts_description = "Product level user setup added successfully"

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        # Check if the user setup already exists
        check_query = """SELECT 1 FROM PRODUCTS_BY_PROJECT_USER_ACCESS 
                        WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND PRODUCT_ID = %s 
                        AND USER_CORPORATE_ACCOUNT = %s AND USER_ID = %s"""
        cursor.execute(check_query, (corporate_account, project_id, product_id, user_corporate_account, user_id))
        if cursor.fetchone():
            return jsonify({
                'status': 'Failed',
                'status_description': 'Product user access already exists'
            })

        # Insert new product user access
        mySql_insert_query = """INSERT INTO PRODUCTS_BY_PROJECT_USER_ACCESS 
        (CORPORATE_ACCOUNT, PROJECT_ID, PRODUCT_ID, USER_CORPORATE_ACCOUNT, USER_ID, ACCESS_LEVEL, CREATED_DATE, UPDATED_DATE)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s) """

        record = (corporate_account, project_id, product_id, user_corporate_account, user_id, access_level,
                  datetime.now(), datetime.now())
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
@base_requirements_blueprint.route('/api/update_product_level_user_setup', methods=['POST'])
@token_required
@validate_access
def update_product_level_user_setup(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    product_id = data.get('product_id')
    user_corporate_account = data.get('user_corporate_account')
    user_id = data.get('user_id')
    access_level = data.get('access_level')

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
    if not validate_product_id(corporate_account, project_id, product_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Product Id is not valid'
        })
    if not user_corporate_account:
        return jsonify({
            'status': 'Failed',
            'status_description': 'User corporate account is required'
        })
    if not validate_user_id(user_corporate_account, user_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'User Id is not valid'
        })
    if not access_level or not isinstance(access_level, int):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Access level is required and must be an integer'
        })

    sts = "Success"
    sts_description = "Product user access updated successfully"
    connection = None

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        # Start transaction
        connection.start_transaction()

        # First check if the product user access exists
        check_query = """SELECT 1 FROM PRODUCTS_BY_PROJECT_USER_ACCESS 
                        WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND PRODUCT_ID = %s 
                        AND USER_CORPORATE_ACCOUNT = %s AND USER_ID = %s"""
        cursor.execute(check_query, (corporate_account, project_id, product_id, user_corporate_account, user_id))
        if not cursor.fetchone():
            return jsonify({
                'status': 'Failed',
                'status_description': 'Product user access not found'
            })

        # Build dynamic update query based on provided fields
        update_parts = []
        update_values = []

        update_parts.append("ACCESS_LEVEL = %s")
        update_values.append(access_level)

        # Always update the updated_date
        update_parts.append("UPDATED_DATE = %s")
        update_values.append(datetime.now())

        # Add parameters for WHERE clause
        update_values.append(corporate_account)
        update_values.append(project_id)
        update_values.append(product_id)
        update_values.append(user_corporate_account)
        update_values.append(user_id)

        update_query = f"""UPDATE PRODUCTS_BY_PROJECT_USER_ACCESS 
                        SET {', '.join(update_parts)} 
                        WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND PRODUCT_ID = %s 
                        AND USER_CORPORATE_ACCOUNT = %s AND USER_ID = %s"""

        cursor.execute(update_query, tuple(update_values))

        if cursor.rowcount == 0:
            sts = "Failed"
            sts_description = "No changes made to the product user access"

        # Commit the transaction
        connection.commit()

    except mysql.connector.Error as error:
        if connection:
            connection.rollback()  # Rollback in case of error
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
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'status': sts,
        'status_description': sts_description
    })


@base_requirements_blueprint.route('/api/delete_product_level_user_setup', methods=['POST'])
@token_required
@validate_access
def delete_product_level_user_setup(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    product_id = data.get('product_id')
    user_corporate_account = data.get('user_corporate_account')
    user_ids = data.get('user_ids', [])  # Expect an array of user IDs

    logging.info(f"input json: {data}")
    logging.info(f"input json user_ids: {user_ids}")

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
    if not validate_product_id(corporate_account, project_id, product_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Product Id is not valid'
        })
    if not user_corporate_account:
        return jsonify({
            'status': 'Failed',
            'status_description': 'User corporate account is required'
        })
    if not user_ids or not isinstance(user_ids, list):
        return jsonify({
            'status': 'Failed',
            'status_description': 'User IDs must be provided as an array'
        })

    # Validate all user_ids
    for user_id in user_ids:
        if not validate_user_id(user_corporate_account, user_id):
            return jsonify({
                'status': 'Failed',
                'status_description': f'User Id {user_id} is not valid'
            })

    sts = "Success"
    sts_description = "Product user access entries deleted successfully"
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
        placeholders = ','.join(['%s'] * len(user_ids))
        mySql_delete_query = f"""DELETE FROM PRODUCTS_BY_PROJECT_USER_ACCESS 
            WHERE CORPORATE_ACCOUNT = %s 
            AND PROJECT_ID = %s 
            AND PRODUCT_ID = %s 
            AND USER_CORPORATE_ACCOUNT = %s 
            AND USER_ID IN ({placeholders})"""

        # Prepare parameters
        record = (corporate_account, project_id, product_id, user_corporate_account, *user_ids)

        cursor.execute(mySql_delete_query, record)
        deleted_count = cursor.rowcount

        if deleted_count == 0:
            sts = "Failed"
            sts_description = "No matching product user access entries found to delete"
        else:
            sts_description = f"Successfully deleted {deleted_count} product user access entry(s)"

        # Commit the transaction
        connection.commit()

    except mysql.connector.Error as error:
        if connection:
            connection.rollback()  # Rollback in case of error
        sts = "Failed"
        sts_description = f"Failed to delete the product user access details: {error}"
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


@base_requirements_blueprint.route('/api/get_product_level_users_list', methods=['GET', 'POST'])
@token_required
def get_product_level_users_list(current_user):
    data = request.json if request.is_json else {}
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    product_id = data.get('product_id')
    user_corporate_account = data.get('user_corporate_account')

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
    sts_description = "Product users retrieved successfully"
    user_list = []

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        # Base query
        query = """SELECT PR.CORPORATE_ACCOUNT, PR.PROJECT_ID, PR.PRODUCT_ID, PR.PRODUCT_NAME,
                    P.USER_CORPORATE_ACCOUNT, P.USER_ID, P.ACCESS_LEVEL, P.CREATED_DATE, P.UPDATED_DATE, 
                   U.USER_NAME, V.ACCOUNT_DESCRIPTION
                  FROM  PRODUCTS_BY_PROJECT PR
                  LEFT JOIN PRODUCTS_BY_PROJECT_USER_ACCESS P ON P.CORPORATE_ACCOUNT = PR.CORPORATE_ACCOUNT 
                    AND P.PROJECT_ID = PR.PROJECT_ID AND P.PRODUCT_ID = PR.PRODUCT_ID
                  LEFT JOIN USER_ACCOUNTS U ON P.USER_CORPORATE_ACCOUNT = U.CORPORATE_ACCOUNT AND P.USER_ID = U.USER_ID
                  LEFT JOIN CORPORATE_ACCOUNTS V ON P.USER_CORPORATE_ACCOUNT = V.CORPORATE_ACCOUNT """

        conditions = []
        params = []

        # Add filters if provided
        if corporate_account:
            conditions.append("P.CORPORATE_ACCOUNT = %s")
            params.append(corporate_account)

        if project_id:
            conditions.append("P.PROJECT_ID = %s")
            params.append(project_id)

        if product_id:
            conditions.append("P.PRODUCT_ID = %s")
            params.append(product_id)

        if user_corporate_account:
            conditions.append("P.USER_CORPORATE_ACCOUNT = %s")
            params.append(user_corporate_account)

        # Add WHERE clause if conditions exist
        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        # Add order by clause
        query += " ORDER BY P.PRODUCT_ID, P.USER_ID"

        cursor.execute(query, tuple(params))
        logging.info(f"Executed SQL: {cursor._executed}")

        for result in cursor.fetchall():
            user_details = {
                'corporate_account': result[0],
                'project_id': result[1],
                'product_id': result[2],
                'product_name': result[3],
                'user_corporate_account': result[4],
                'user_id': result[5],
                'access_level': result[6],
                'created_date': result[7],
                'updated_date': result[8],
                'user_name': result[9],
                'user_account_description': result[10]
            }
            user_list.append(user_details)

        if len(user_list) == 0:
            sts = "Success"  # Still success but with empty list
            sts_description = "No product users found matching the criteria"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the product users list: {error}"
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


@base_requirements_blueprint.route('/api/get_product_level_user_access', methods=['GET', 'POST'])
@token_required
def get_product_level_user_details(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    product_id = data.get('product_id')
    user_corporate_account = data.get('user_corporate_account')
    user_id = data.get('user_id')

    if not corporate_account or not project_id or not product_id or not user_corporate_account or not user_id:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Corporate Account, Project Id, Product Id, User Corporate Account, and User Id are required'
        })

    sts = "Success"
    sts_description = "Product user details retrieved successfully"
    user_details = {}

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        query = """SELECT P.PROJECT_ID, P.PRODUCT_ID, P.USER_CORPORATE_ACCOUNT, P.USER_ID, P.ACCESS_LEVEL, 
                  P.CREATED_DATE, P.UPDATED_DATE, P.CORPORATE_ACCOUNT,
                  PR.PRODUCT_NAME, U.USER_NAME
                  FROM PRODUCTS_BY_PROJECT_USER_ACCESS P
                  LEFT JOIN PRODUCTS_BY_PROJECT PR ON P.CORPORATE_ACCOUNT = PR.CORPORATE_ACCOUNT 
                    AND P.PROJECT_ID = PR.PROJECT_ID AND P.PRODUCT_ID = PR.PRODUCT_ID
                  LEFT JOIN USER_ACCOUNTS U ON P.USER_CORPORATE_ACCOUNT = U.CORPORATE_ACCOUNT AND P.USER_ID = U.USER_ID
                  WHERE P.CORPORATE_ACCOUNT = %s AND P.PROJECT_ID = %s AND P.PRODUCT_ID = %s 
                  AND P.USER_CORPORATE_ACCOUNT = %s AND P.USER_ID = %s"""

        cursor.execute(query, (corporate_account, project_id, product_id, user_corporate_account, user_id))

        result = cursor.fetchone()

        if result:
            user_details = {
                'project_id': result[0],
                'product_id': result[1],
                'user_corporate_account': result[2],
                'user_id': result[3],
                'access_level': result[4],
                'created_date': result[5],
                'updated_date': result[6],
                'corporate_account': result[7],
                'product_name': result[8],
                'user_name': result[9]
            }
        else:
            sts = "Failed"
            sts_description = "No product user found with the provided criteria"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the product user details: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'user_details': user_details,
        'status': sts,
        'status_description': sts_description
    })


@base_requirements_blueprint.route('/api/get_user_level_products_list', methods=['GET', 'POST'])
@token_required
def get_user_level_products_list(current_user):
    data = request.json
    user_corporate_account = data.get('user_corporate_account')
    user_id = data.get('user_id')
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    access_level = data.get('access_level')




    if not corporate_account or not project_id or not user_corporate_account or not user_id:
        return jsonify({
            'status': 'Failed',
            'status_description': 'User Corporate Account, User Id , Corporate Account and Project Id are required'
        })

    sts = "Success"
    sts_description = "Product list retrieved successfully"
    product_list = []

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        mySql_select_query  = """SELECT A.CORPORATE_ACCOUNT, A.PROJECT_ID, A.PRODUCT_ID, A.PRODUCT_NAME, A.PRODUCT_COMPANY, B.ACCESS_LEVEL
        FROM PRODUCTS_BY_PROJECT A, PRODUCTS_BY_PROJECT_USER_ACCESS B 
        WHERE A.CORPORATE_ACCOUNT = B.CORPORATE_ACCOUNT AND A.PROJECT_ID = B.PROJECT_ID AND A.PRODUCT_ID = B.PRODUCT_ID AND
        A.STATUS = %s  AND A.CORPORATE_ACCOUNT = %s AND A.PROJECT_ID = %s AND B.CORPORATE_ACCOUNT = %s AND B.USER_ID = %s """

        record = ['Active',  corporate_account, project_id,  user_corporate_account, user_id]

        if access_level and isinstance(access_level, int):
            mySql_select_query += " AND B.ACCESS_LEVEL = %s "
            record.append(access_level)

        cursor.execute(mySql_select_query, record)
        logging.info(f"get_user_level_products_list Executed SQL: {cursor._executed}")

        for result in cursor.fetchall():
            product_details = {
                'corporate_account': result[0],
                'project_id': result[1],
                'product_id': result[2],
                'product_name': result[3],
                'product_company': result[4],
                'access_level': result[5]
            }
            product_list.append(product_details)

        if len(product_list) == 0:
            sts = "Success"  # Still success but with empty list
            sts_description = "No product list found for the user"



    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the product list for the user: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'product_list': product_list,
        'status': sts,
        'status_description': sts_description
    })

if __name__ == '__main__':
    app.run(debug=True)


