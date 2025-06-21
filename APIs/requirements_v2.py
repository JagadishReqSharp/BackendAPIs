from flask import Flask, request, jsonify, Blueprint
import mysql.connector
from datetime import datetime
import config
import logging
from foundational_v2 import generate_next_sequence, validate_project_id, validate_level_id, validate_req_id, \
    validate_status, validate_user_id, is_user_authorized_to_approve, validate_integration_id, validate_raid_log_entry, \
    get_project_prefix, get_link_details
from foundational_v2 import validate_corporate_account, validate_usecase_id, validate_testcase_id,  validate_key_attribute_list_id, validate_product_id, validate_req_classification, get_functional_level_children
from utils import token_required
from access_validation_at_api_level import validate_access
import os
import uuid

# Create a blueprint
requirements_blueprint = Blueprint('requirements', __name__)


app = Flask(__name__)

logging.basicConfig(filename='debugging.log', level=logging.DEBUG)





@requirements_blueprint.route('/api/create_requirement', methods=['POST','PUT'])
@token_required
@validate_access
def create_requirement(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    level_id = data.get('level_id')
    req_description = data.get('req_description')
    status = data.get('status')
    req_criticality = data.get('req_criticality')
    req_priority = data.get('req_priority')
    ref_field_1 = data.get('ref_field_1')
    ref_field_2 = data.get('ref_field_2')
    ref_field_3 = data.get('ref_field_3')
    ref_field_4 = data.get('ref_field_4')

    logging.info(f"Inside create_requirement data: {data}")


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
    if not validate_level_id(corporate_account, project_id, level_id):
        return jsonify({
            'req_id': None,
            'status': 'Failed',
            'status_description': 'Level Id is not valid'
        })

    if not req_description.strip():
        return jsonify({
            'req_id': None,
            'status': 'Failed',
            'status_description': 'Requirement description is required'
        })


    if not status.strip():
        return jsonify({
            'req_id': None,
            'status': 'Failed',
            'status_description': 'Requirement status is required'
        })

    if not validate_status(corporate_account, project_id, 'REQUIREMENT', status):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Invalid requirement status'
        })


    if not req_criticality.strip():
        return jsonify({
            'req_id': None,
            'status': 'Failed',
            'status_description': 'Requirement criticality is required'
        })


    if not validate_status(corporate_account, project_id, 'REQUIREMENT_CRITICALITY', req_criticality):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Invalid requirement criticality'
        })

    if req_priority and not validate_status(corporate_account, project_id, 'REQUIREMENT_PRIORITY', req_priority):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Invalid requirement priority'
        })


    sts = "Success"
    sts_description = "Requirement added successfully"
    req_id = None
    project_prefix = None

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()


        project_prefix = get_project_prefix( corporate_account, project_id)

        if project_prefix == 'Error':
            return jsonify({
                'req_id': None,
                'status': 'Failed',
                'status_description': 'Requirement prefix not defined'
            })


        mySql_insert_query = """INSERT INTO REQUIREMENTS (CORPORATE_ACCOUNT, PROJECT_ID, REQ_ID, REQ_ID_WITH_PREFIX, LEVEL_ID, REQ_DESCRIPTION, STATUS, REQ_CRITICALITY, REQ_PRIORITY, CREATED_DATE, UPDATED_DATE, REF_FIELD_1, REF_FIELD_2, REF_FIELD_3, REF_FIELD_4)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """

        req_id, seq_status, seq_status_description  = generate_next_sequence(corporate_account, project_id, 'REQUIREMENT')

        if seq_status == "Failed":
            sts = "Failed"
            sts_description = seq_status_description
            return jsonify({
                'req_id': req_id,
                'status': sts,
                'status': sts,
                'status_description': sts_description
            })

        req_id_with_prefix = f"{project_prefix.strip()}-{req_id}"

        record = (corporate_account, project_id, req_id, req_id_with_prefix, level_id, req_description, status, req_criticality, req_priority, datetime.now(), datetime.now(), ref_field_1, ref_field_2, ref_field_3, ref_field_4)
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
        'req_id': req_id_with_prefix,
        'status': sts,
        'status_description': sts_description
    })



@requirements_blueprint.route('/api/update_requirement', methods=['PUT', 'POST'])
@token_required
@validate_access
def update_requirement(current_user):
    data = request.json
    req_id = data.get('req_id')
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    level_id = data.get('level_id')
    req_description = data.get('req_description')
    status = data.get('status')
    req_criticality = data.get('req_criticality')
    req_priority = data.get('req_priority')
    ref_field_1 = data.get('ref_field_1')
    ref_field_2 = data.get('ref_field_2')
    ref_field_3 = data.get('ref_field_3')
    ref_field_4 = data.get('ref_field_4')

    sts = "Success"
    sts_description = "Requirement updated successfully"
    rows_impacted = 0
    logging.info(f"top of update_requirement {data}")

    if not validate_level_id(corporate_account, project_id, level_id):
        return jsonify({
            'req_id': None,
            'status': 'Failed',
            'status_description': 'Level Id is not valid',
            'rows_impacted': rows_impacted
        })

    if not req_description.strip():
        return jsonify({
            'req_id': None,
            'status': 'Failed',
            'status_description': 'Requirement description is required',
            'rows_impacted': rows_impacted
        })




    if not status.strip():
        return jsonify({
            'req_id': None,
            'status': 'Failed',
            'status_description': 'Requirement status is required'
        })

    if not validate_status(corporate_account, project_id, 'REQUIREMENT', status):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Invalid requirement status'
        })


    if not req_criticality.strip():
        return jsonify({
            'req_id': None,
            'status': 'Failed',
            'status_description': 'Requirement criticality is required'
        })

    if not validate_status(corporate_account, project_id, 'REQUIREMENT_CRITICALITY', req_criticality):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Invalid requirement criticality'
        })

    if req_priority and not validate_status(corporate_account, project_id, 'REQUIREMENT_PRIORITY', req_priority):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Invalid requirement priority'
        })




    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_update_query = """UPDATE REQUIREMENTS SET LEVEL_ID = %s, REQ_DESCRIPTION = %s, STATUS = %s, REQ_CRITICALITY = %s, REQ_PRIORITY = %s, 
        UPDATED_DATE = %s, REF_FIELD_1 = %s, REF_FIELD_2 = %s, REF_FIELD_3 = %s, REF_FIELD_4 = %s 
        WHERE REQ_ID = %s AND CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s"""
        record = (level_id, req_description, status, req_criticality, req_priority, datetime.now(), ref_field_1, ref_field_2, ref_field_3, ref_field_4, req_id, corporate_account, project_id )
        cursor.execute(mySql_update_query, record)
        rows_impacted = cursor.rowcount
        if rows_impacted == 0:
            sts = "Failed"
            sts_description = "No matching requirement found to update"

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
        'req_id': req_id,
        'status': sts,
        'status_description': sts_description,
        'rows_impacted': rows_impacted

    })


@requirements_blueprint.route('/api/update_requirement_status', methods=['PUT','POST'])
@token_required
@validate_access
def update_requirement_status(current_user):

    data = request.json
    req_ids = data.get('req_ids', [])
    requirement_type = data.get('requirement_type', 'REQUIREMENT')  # Default to 'REQUIREMENT' if not provided
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    status = data.get('status')


    sts = "Success"
    sts_description = "Requirement status updated successfully"
    rows_impacted = 0
    placeholders = ''


    if not req_ids or not isinstance(req_ids, list):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Req Ids must be provided as an array'
        })

    if not validate_status(corporate_account, project_id, 'REQUIREMENT', status):
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

        placeholders = ','.join(['%s'] * len(req_ids))
        mySql_update_query = f"""UPDATE REQUIREMENTS SET STATUS = %s, UPDATED_DATE = %s WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND REQ_ID IN({placeholders})"""
        record = [status,  datetime.now(), corporate_account, project_id]
        record.extend(req_ids)
        cursor.execute(mySql_update_query, record)
        logging.info(f" executed SQL-1 is: {cursor._executed}")
        rows_impacted = cursor.rowcount

        mySql_update_query = f"""UPDATE INTEGRATION_REQUIREMENTS SET STATUS = %s, UPDATED_DATE = %s WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND INTEGRATION_ID IN({placeholders})"""
        cursor.execute(mySql_update_query, record)
        logging.info(f" executed SQL-2 is: {cursor._executed}")
        rows_impacted += cursor.rowcount
        if rows_impacted == 0:
                sts = "Failed"
                sts_description = "No matching requirement found to update"
        else:
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


@requirements_blueprint.route('/api/update_requirement_criticality', methods=['PUT','POST'])
@token_required
@validate_access
def update_requirement_criticality(current_user):

    data = request.json
    req_ids = data.get('req_ids', [])
    requirement_type = data.get('requirement_type', 'REQUIREMENT')  # Default to 'REQUIREMENT' if not provided
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    req_criticality = data.get('req_criticality')


    sts = "Success"
    sts_description = "Requirement criticality updated successfully"
    rows_impacted = 0
    placeholders = ''


    if not req_ids or not isinstance(req_ids, list):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Req Ids must be provided as an array'
        })

    if not validate_status(corporate_account, project_id, 'REQUIREMENT_CRITICALITY', req_criticality):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Invalid Criticality status'
        })

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        placeholders = ','.join(['%s'] * len(req_ids))
        mySql_update_query = f"""UPDATE REQUIREMENTS SET REQ_CRITICALITY = %s, UPDATED_DATE = %s WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND REQ_ID IN({placeholders})"""
        record = [req_criticality,  datetime.now(), corporate_account, project_id]
        record.extend(req_ids)
        cursor.execute(mySql_update_query, record)
        logging.info(f" executed SQL-1 is: {cursor._executed}")
        rows_impacted = cursor.rowcount

        mySql_update_query = f"""UPDATE INTEGRATION_REQUIREMENTS SET INTEGRATION_CRITICALITY = %s, UPDATED_DATE = %s WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND INTEGRATION_ID IN({placeholders})"""
        cursor.execute(mySql_update_query, record)
        logging.info(f" executed SQL-2 is: {cursor._executed}")
        rows_impacted += cursor.rowcount

        if rows_impacted == 0:
            sts = "Failed"
            sts_description = "No matching requirement found to update"
        else:
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

@requirements_blueprint.route('/api/update_requirement_priority', methods=['PUT','POST'])
@token_required
@validate_access
def update_requirement_priority(current_user):

    data = request.json
    req_ids = data.get('req_ids', [])
    requirement_type = data.get('requirement_type', 'REQUIREMENT')  # Default to 'REQUIREMENT' if not provided
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    req_priority = data.get('req_priority')


    sts = "Success"
    sts_description = "Requirement priority updated successfully"
    rows_impacted = 0
    placeholders = ''


    if not req_ids or not isinstance(req_ids, list):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Req Ids must be provided as an array'
        })

    if not validate_status(corporate_account, project_id, 'REQUIREMENT_PRIORITY', req_priority):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Invalid priority status'
        })

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        placeholders = ','.join(['%s'] * len(req_ids))
        mySql_update_query = f"""UPDATE REQUIREMENTS SET REQ_PRIORITY = %s, UPDATED_DATE = %s WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND REQ_ID IN({placeholders})"""
        record = [req_priority,  datetime.now(), corporate_account, project_id]
        record.extend(req_ids)
        cursor.execute(mySql_update_query, record)
        logging.info(f" executed SQL-1 is: {cursor._executed}")
        rows_impacted = cursor.rowcount

        mySql_update_query = f"""UPDATE INTEGRATION_REQUIREMENTS SET INTEGRATION_PRIORITY = %s, UPDATED_DATE = %s WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND INTEGRATION_ID IN({placeholders})"""
        cursor.execute(mySql_update_query, record)
        logging.info(f" executed SQL-2 is: {cursor._executed}")
        rows_impacted += cursor.rowcount

        if rows_impacted == 0:
            sts = "Failed"
            sts_description = "No matching requirement found to update"
        else:
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


@requirements_blueprint.route('/api/copy_requirement', methods=['PUT', 'POST'])
@token_required
@validate_access
def copy_requirement(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    from_project_id = data.get('from_project_id')
    from_req_id = data.get('from_req_id')
    to_project_id = data.get('to_project_id')
    to_level_id = data.get('to_level_id')
    to_status = data.get('to_status')
    copy_attachments = data.get('copy_attachments', False)  # New parameter for attachment copying

    # Validate input parameters
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

    if not from_req_id:
        return jsonify({
            'status': 'Failed',
            'status_description': 'From-Integration requirement Id is required'
        })

    if not validate_req_id(corporate_account, from_project_id, from_req_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'From-requirement Id is not valid'
        })

    if not validate_status(corporate_account, to_project_id, 'REQUIREMENT', to_status):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Invalid status under the target project'
        })

    sts = "Success"
    sts_description = "Requirement successfully copied to the target project"
    attachment_count = 0
    to_req_id = None
    to_req_id_with_prefix = None

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor(dictionary=True)

        # Check if source requirement exists
        check_query = """SELECT REQ_ID FROM REQUIREMENTS WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s  
        AND REQ_ID = %s"""
        check_params = (corporate_account, from_project_id, from_req_id)

        cursor.execute(check_query, check_params)
        result = cursor.fetchone()

        if result:
            # Generate new requirement ID
            to_req_id, seq_status, seq_status_description = generate_next_sequence(corporate_account, to_project_id,
                                                                                   'REQUIREMENT')

            if seq_status == "Failed":
                sts = "Failed"
                sts_description = seq_status_description
                return jsonify({
                    'status': sts,
                    'status_description': sts_description
                })

            # Get project prefix
            project_prefix = get_project_prefix(corporate_account, to_project_id)

            if project_prefix == 'Error':
                return jsonify({
                    'req_id': None,
                    'status': 'Failed',
                    'status_description': 'Requirement prefix not defined'
                })

            # Create complete requirement ID with prefix
            to_req_id_with_prefix = f"{project_prefix.strip()}-{to_req_id}"

            # Insert the new requirement
            insert_query = """ INSERT INTO REQUIREMENTS (CORPORATE_ACCOUNT, PROJECT_ID, REQ_ID, REQ_ID_WITH_PREFIX, LEVEL_ID, REQ_DESCRIPTION, STATUS,
            REQ_CRITICALITY, REQ_PRIORITY, REF_FIELD_1, REF_FIELD_2, REF_FIELD_3, REF_FIELD_4, CREATED_DATE, UPDATED_DATE)
            SELECT CORPORATE_ACCOUNT, %s, %s, %s, %s, REQ_DESCRIPTION, %s, REQ_CRITICALITY, REQ_PRIORITY,
            REF_FIELD_1, REF_FIELD_2, REF_FIELD_3, REF_FIELD_4, NOW(), NOW()
            FROM REQUIREMENTS WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND REQ_ID = %s """

            insert_params = (
                to_project_id,
                to_req_id,
                to_req_id_with_prefix,
                to_level_id,
                to_status,
                corporate_account,
                from_project_id,
                from_req_id)

            cursor.execute(insert_query, insert_params)
            connection.commit()

            # Copy key attributes
            key_attributes_query = """INSERT INTO KEY_ATTRIBUTES_LIST_REQUIREMENTS(CORPORATE_ACCOUNT, PROJECT_ID, REQ_ID, LEVEL_ID, KEY_ATTRIBUTE_LIST_ID,
            INCLUDE_EXCLUDE, CREATED_DATE, UPDATED_DATE)
            SELECT CORPORATE_ACCOUNT, %s, %s, LEVEL_ID, KEY_ATTRIBUTE_LIST_ID,
            INCLUDE_EXCLUDE, NOW(), NOW() FROM KEY_ATTRIBUTES_LIST_REQUIREMENTS
            WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND REQ_ID = %s """

            key_attributes_params = (
                to_project_id,
                to_req_id,
                corporate_account,
                from_project_id,
                from_req_id)

            cursor.execute(key_attributes_query, key_attributes_params)
            connection.commit()

            # Handle attachment copying if requested
            if copy_attachments:
                logging.info(f"Copying attachments for requirement from {from_req_id} to {to_req_id}")

                # Get existing attachments
                attachments_query = """
                    SELECT * FROM REQUIREMENT_ATTACHMENTS
                    WHERE CORPORATE_ACCOUNT = %s
                    AND PROJECT_ID = %s
                    AND REQ_ID = %s
                """
                cursor.execute(attachments_query, (corporate_account, from_project_id, from_req_id))
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
                        attachment_insert_query = """
                            INSERT INTO REQUIREMENT_ATTACHMENTS 
                            (CORPORATE_ACCOUNT, PROJECT_ID, REQ_ID, FILE_NAME, FILE_PATH, FILE_SIZE, FILE_TYPE, UPLOADED_BY)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        attachment_params = (
                            corporate_account,
                            to_project_id,
                            to_req_id,
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
            sts_description = "No matching requirement found to copy from"

    except mysql.connector.Error as error:
        sts = "Failed"
        if hasattr(error, 'errno') and error.errno == 1062:  # MySQL error code for duplicate entry
            record_info = f"corporate_account: {corporate_account}, project: {to_project_id}, requirement: {to_req_id}"
            sts_description = f"Error: Attempt to create duplicate record: ({record_info})"
        else:
            sts_description = f"Failed to copy the requirement: {error}"
        logging.error(f"Database error copying requirement: {error}")

    except Exception as e:
        sts = "Failed"
        sts_description = f"Failed to copy the requirement: {str(e)}"
        logging.error(f"Error copying requirement: {str(e)}")

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'req_id': to_req_id_with_prefix,
        'status': sts,
        'status_description': sts_description,
        'attachments_copied': copy_attachments,
        'attachment_count': attachment_count
    })

@requirements_blueprint.route('/api/get_requirements_list_old', methods=['GET', 'POST'])
@token_required
def get_requirements_list_old(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    level_id = data.get('level_id')
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
        mySql_select_query = f"""SELECT A.REQ_ID, A.REQ_ID_WITH_PREFIX, A.LEVEL_ID, B.LEVEL_DESCRIPTION, A.REQ_DESCRIPTION, A.STATUS, A.REQ_CRITICALITY, A.REQ_PRIORITY, A.CREATED_DATE, A.UPDATED_DATE,
        REF_FIELD_1, REF_FIELD_2, REF_FIELD_3, REF_FIELD_4, (SELECT COUNT(*) FROM KEY_ATTRIBUTES_LIST_REQUIREMENTS C WHERE A.CORPORATE_ACCOUNT = C.CORPORATE_ACCOUNT AND A.PROJECT_ID = C.PROJECT_ID AND C.REQ_ID = A.REQ_ID) NUMBER_OF_EXCEPTIONS
            , (SELECT COUNT(*) FROM REQUIREMENTS_APPROVERS D WHERE A.CORPORATE_ACCOUNT = D.CORPORATE_ACCOUNT AND A.PROJECT_ID = D.PROJECT_ID AND A.REQ_ID = D.REQ_ID) NUMBER_OF_APPROVERS
           FROM REQUIREMENTS A, FUNCTIONAL_LEVELS B
           WHERE A.CORPORATE_ACCOUNT = B.CORPORATE_ACCOUNT AND A.PROJECT_ID = B.PROJECT_ID AND A.LEVEL_ID = B.LEVEL_ID AND A.CORPORATE_ACCOUNT = %s AND A.PROJECT_ID = %s 
             """

        params = [corporate_account, project_id]

        if filter_by_status and isinstance(filter_by_status, list) and len(
                    filter_by_status) > 0:
                placeholders = ','.join(['%s'] * len(filter_by_status))
                mySql_select_query += f" AND A.STATUS IN({placeholders}) "
                params.extend(filter_by_status)

        # Add criticality filter if provided
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

        if not sort_criteria:
            sort_criteria = 'REQ_ID'

        # Add ORDER BY at the end
        mySql_select_query += " ORDER BY " + sort_criteria

        logging.info(f" Prepared SQL is: {mySql_select_query}")

        cursor.execute(mySql_select_query, tuple(params))

        logging.info(f" executed SQL is: {cursor._executed}")

        for result in cursor.fetchall():
            requirement_details = {
                'req_id': result[0],
                'req_id_with_prefix': result[1],
                'level_id': result[2],
                'level_description': result[3],
                'req_description': result[4],
                'status': result[5],
                'req_criticality': result[6],
                'req_priority': result[7],
                'created_date': result[8],
                'updated_date': result[9],
                'ref_field_1': result[10],
                'ref_field_2': result[11],
                'ref_field_3': result[12],
                'ref_field_4': result[13],
                'number_of_exceptions': result[14],
                'number_of_approvers': result[15]
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



@requirements_blueprint.route('/api/get_requirements_by_approver', methods=['GET', 'POST'])
@token_required
def get_requirements_by_approver(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    level_id = data.get('level_id')
    filter_by_status = data.get('filter_by_status', [])
    filter_by_approvers = data.get('filter_by_approvers', [])
    filter_by_approval_status = data.get('filter_by_approval_status', [])

    search_query = data.get('search_query')
    sort_criteria = data.get('sort_criteria')
    # Add new filter parameters
    requirement_criticality = data.get('requirement_criticality', [])
    requirement_priority = data.get('requirement_priority', [])
    created_date_start = data.get('created_date_start')
    created_date_end = data.get('created_date_end')
    updated_date_start = data.get('updated_date_start')
    updated_date_end = data.get('updated_date_end')
    approval_date_start = data.get('approval_date_start')
    approval_date_end = data.get('approval_date_end')
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
                level_condition = " AND C.LEVEL_ID = %s "
                child_levels_list = [level_id]
        else:
            # If no child levels or include_child_levels_flag is False, use the provided level_id directly
            if level_id:
                level_condition = " AND C.LEVEL_ID = %s "
                child_levels_list = [level_id] if level_id else []
        logging.info(f"level_condition: {level_condition}")
        logging.info(f"child_levels_list: {child_levels_list}")


        # Base query without ORDER BY
        mySql_select_query = f"""SELECT A.REQ_ID, C.LEVEL_ID, D.LEVEL_DESCRIPTION,
        C.REQ_ID_WITH_PREFIX, C.REQ_DESCRIPTION, C.STATUS 'REQUIREMENT_STATUS', C.REQ_CRITICALITY, C.REQ_PRIORITY, C.CREATED_DATE REQ_CREATED_DATE, C.UPDATED_DATE REQ_UPDATED_DATE,
        A.APPROVAL_STATUS, A.APPROVER_COMMENTS, A.UPDATED_DATE 'APPROVAL_DATE', A.APPROVAL_USER_ID, B.USER_NAME, A.REQUIREMENT_APPROVER_RECORD_ID
        , (SELECT COUNT(*) FROM REQUIREMENTS_APPROVERS Y WHERE A.CORPORATE_ACCOUNT = Y.CORPORATE_ACCOUNT AND A.PROJECT_ID = Y.PROJECT_ID AND A.REQ_ID = Y.REQ_ID) NUMBER_OF_APPROVERS, "REQUIREMENT" REQ_TYPE
        FROM REQUIREMENTS_APPROVERS A, USER_ACCOUNTS B, REQUIREMENTS C, FUNCTIONAL_LEVELS D
        WHERE A.CORPORATE_ACCOUNT = C.CORPORATE_ACCOUNT AND A.PROJECT_ID = C.PROJECT_ID AND A.REQ_ID = C.REQ_ID
        AND A.CORPORATE_ACCOUNT = B.CORPORATE_ACCOUNT AND A.APPROVAL_USER_ID = B.USER_ID AND 
        C.CORPORATE_ACCOUNT = D.CORPORATE_ACCOUNT AND C.PROJECT_ID = D.PROJECT_ID AND C.LEVEL_ID = D.LEVEL_ID AND
        A.CORPORATE_ACCOUNT = %s AND A.PROJECT_ID = %s """

        params = [corporate_account, project_id]

        if filter_by_approvers and isinstance(filter_by_approvers, list) and len(
                    filter_by_approvers) > 0:
            placeholders = ','.join(['%s'] * len(filter_by_approvers))
            mySql_select_query += f""" AND A.APPROVAL_USER_ID IN ({placeholders}) """
            params.extend(filter_by_approvers)

        if filter_by_approval_status and isinstance(filter_by_approval_status, list) and len(
                    filter_by_approval_status) > 0 and filter_by_approval_status[0]:
            placeholders = ','.join(['%s'] * len(filter_by_approval_status))
            mySql_select_query += f""" AND A.APPROVAL_STATUS IN ({placeholders}) """
            params.extend(filter_by_approval_status)

        if filter_by_status and isinstance(filter_by_status, list) and len(
                    filter_by_status) > 0:
                placeholders = ','.join(['%s'] * len(filter_by_status))
                mySql_select_query += f" AND C.STATUS IN({placeholders}) "
                params.extend(filter_by_status)

        # Add criticality filter if provided
        if requirement_criticality and isinstance(requirement_criticality, list) and len(requirement_criticality) > 0:
            criticality_placeholders = ','.join(['%s'] * len(requirement_criticality))
            mySql_select_query += f" AND C.REQ_CRITICALITY IN({criticality_placeholders}) "
            params.extend(requirement_criticality)

        # Add priority filter if provided
        if requirement_priority and isinstance(requirement_priority, list) and len(requirement_priority) > 0:
            priority_placeholders = ','.join(['%s'] * len(requirement_priority))
            mySql_select_query += f" AND C.REQ_PRIORITY IN({priority_placeholders}) "
            params.extend(requirement_priority)

        # Add created_date range filter if provided
        if created_date_start:
            mySql_select_query += " AND C.CREATED_DATE >= %s "
            params.append(created_date_start)

        if created_date_end:
            mySql_select_query += " AND DATE(C.CREATED_DATE) <= DATE(%s) "
            params.append(created_date_end)

        # Add updated_date range filter if provided
        if updated_date_start:
            mySql_select_query += " AND C.UPDATED_DATE >= %s "
            params.append(updated_date_start)

        if updated_date_end:
            mySql_select_query += " AND DATE(C.UPDATED_DATE) <= DATE(%s) "
            params.append(updated_date_end)

        # Add approval_date range filter if provided
        if approval_date_start:
            mySql_select_query += " AND A.UPDATED_DATE >= %s "
            params.append(approval_date_start)

        if approval_date_end:
            mySql_select_query += " AND DATE(A.UPDATED_DATE) <= DATE(%s) "
            params.append(approval_date_end)


        if search_query:
            if level_id:
                mySql_select_query += level_condition
                params.extend(child_levels_list)
                mySql_select_query += " AND ( C.REQ_ID_WITH_PREFIX = %s OR C.REQ_DESCRIPTION LIKE %s) "
                params.extend([search_query, f"%{search_query}%"])
            else:
                mySql_select_query += " AND ( C.REQ_ID_WITH_PREFIX = %s OR C.REQ_DESCRIPTION LIKE %s) "
                params.extend([search_query, f"%{search_query}%"])
        else:
            if level_id:
                mySql_select_query += level_condition
                params.extend(child_levels_list)












        # integration requirements query
            # Base query without ORDER BY
            mySql_select_query += f""" UNION
        SELECT A.REQ_ID, C.LEVEL_ID, D.LEVEL_DESCRIPTION,
        C.INTEGRATION_ID_WITH_PREFIX, C.INTEGRATION_DESCRIPTION, C.STATUS 'REQUIREMENT_STATUS', C.INTEGRATION_CRITICALITY, C.INTEGRATION_PRIORITY, C.CREATED_DATE REQ_CREATED_DATE, C.UPDATED_DATE REQ_UPDATED_DATE,
        A.APPROVAL_STATUS, A.APPROVER_COMMENTS, A.UPDATED_DATE 'APPROVAL_DATE', A.APPROVAL_USER_ID, B.USER_NAME, A.REQUIREMENT_APPROVER_RECORD_ID
        , (SELECT COUNT(*) FROM REQUIREMENTS_APPROVERS Y WHERE A.CORPORATE_ACCOUNT = Y.CORPORATE_ACCOUNT AND A.PROJECT_ID = Y.PROJECT_ID AND A.REQ_ID = Y.REQ_ID) NUMBER_OF_APPROVERS, "INTEGRATION REQUIREMENT" REQ_TYPE
        FROM REQUIREMENTS_APPROVERS A, USER_ACCOUNTS B, INTEGRATION_REQUIREMENTS C, FUNCTIONAL_LEVELS D
        WHERE A.CORPORATE_ACCOUNT = C.CORPORATE_ACCOUNT AND A.PROJECT_ID = C.PROJECT_ID AND A.REQ_ID = C.INTEGRATION_ID
        AND A.CORPORATE_ACCOUNT = B.CORPORATE_ACCOUNT AND A.APPROVAL_USER_ID = B.USER_ID AND 
        C.CORPORATE_ACCOUNT = D.CORPORATE_ACCOUNT AND C.PROJECT_ID = D.PROJECT_ID AND C.LEVEL_ID = D.LEVEL_ID AND
               A.CORPORATE_ACCOUNT = %s AND A.PROJECT_ID = %s """

            params.extend([corporate_account, project_id])

            if filter_by_approvers and isinstance(filter_by_approvers, list) and len(
                    filter_by_approvers) > 0:
                placeholders = ','.join(['%s'] * len(filter_by_approvers))
                mySql_select_query += f""" AND A.APPROVAL_USER_ID IN ({placeholders}) """
                params.extend(filter_by_approvers)

            if filter_by_approval_status and isinstance(filter_by_approval_status, list) and len(
                    filter_by_approval_status) > 0 and filter_by_approval_status[0]:
                placeholders = ','.join(['%s'] * len(filter_by_approval_status))
                mySql_select_query += f""" AND A.APPROVAL_STATUS IN ({placeholders}) """
                params.extend(filter_by_approval_status)

            if filter_by_status and isinstance(filter_by_status, list) and len(
                    filter_by_status) > 0:
                placeholders = ','.join(['%s'] * len(filter_by_status))
                mySql_select_query += f" AND C.STATUS IN({placeholders}) "
                params.extend(filter_by_status)

            # Add criticality filter if provided
            if requirement_criticality and isinstance(requirement_criticality, list) and len(
                    requirement_criticality) > 0:
                criticality_placeholders = ','.join(['%s'] * len(requirement_criticality))
                mySql_select_query += f" AND C.INTEGRATION_CRITICALITY IN({criticality_placeholders}) "
                params.extend(requirement_criticality)

            # Add priority filter if provided
            if requirement_priority and isinstance(requirement_priority, list) and len(requirement_priority) > 0:
                priority_placeholders = ','.join(['%s'] * len(requirement_priority))
                mySql_select_query += f" AND C.INTEGRATION_PRIORITY IN({priority_placeholders}) "
                params.extend(requirement_priority)

            # Add created_date range filter if provided
            if created_date_start:
                mySql_select_query += " AND C.CREATED_DATE >= %s "
                params.append(created_date_start)

            if created_date_end:
                mySql_select_query += " AND DATE(C.CREATED_DATE) <= DATE(%s) "
                params.append(created_date_end)

            # Add updated_date range filter if provided
            if updated_date_start:
                mySql_select_query += " AND C.UPDATED_DATE >= %s "
                params.append(updated_date_start)

            if updated_date_end:
                mySql_select_query += " AND DATE(C.UPDATED_DATE) <= DATE(%s) "
                params.append(updated_date_end)

            # Add approval_date range filter if provided
            if approval_date_start:
                mySql_select_query += " AND A.UPDATED_DATE >= %s "
                params.append(approval_date_start)

            if approval_date_end:
                mySql_select_query += " AND DATE(A.UPDATED_DATE) <= DATE(%s) "
                params.append(approval_date_end)

            if search_query:
                if level_id:
                    mySql_select_query += level_condition
                    params.extend(child_levels_list)
                    mySql_select_query += " AND ( C.INTEGRATION_ID_WITH_PREFIX = %s OR C.INTEGRATION_DESCRIPTION LIKE %s) "
                    params.extend([search_query, f"%{search_query}%"])
                else:
                    mySql_select_query += " AND ( C.INTEGRATION_ID_WITH_PREFIX = %s OR C.INTEGRATION_DESCRIPTION LIKE %s) "
                    params.extend([search_query, f"%{search_query}%"])
            else:
                if level_id:
                    mySql_select_query += level_condition
                    params.extend(child_levels_list)


















        if not sort_criteria:
            sort_criteria = 'REQ_ID'

        # Add ORDER BY at the end
        mySql_select_query += " ORDER BY " + sort_criteria

        logging.info(f" Prepared SQL is: {mySql_select_query}")

        cursor.execute(mySql_select_query, tuple(params))

        logging.info(f" executed SQL is: {cursor._executed}")

        for result in cursor.fetchall():
            requirement_details = {
                'req_id': result[0],
                'level_id': result[1],
                'level_description': result[2],
                'req_id_with_prefix': result[3],
                'req_description': result[4],
                'req_status': result[5],
                'req_criticality': result[6],
                'req_priority': result[7],
                'req_created_date': result[8],
                'req_updated_date': result[9],
                'approval_status': result[10] ,
                'approver_comments': result[11],
                'approval_date': result[12],
                'approver_user_id': result[13],
                'approver_user_name': result[14],
                'REQUIREMENT_APPROVER_RECORD_ID': result[15],
                'number_of_approvers': result[16],
                'req_type': result[17]
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

@requirements_blueprint.route('/api/get_requirement_details', methods=['GET', 'POST'])
@token_required
def get_requirement_details(current_user):

    data = request.json
    req_id = data.get('req_id')
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')

    #req_id = request.args.get('req_id')
    #corporate_account = request.args.get('corporate_account')
    #project_id = request.args.get('project_id')


    sts = "Success"
    sts_description = "Requirement details retrieved successfully"
    requirement_details = {}

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_select_query = f"""SELECT REQ_ID, CORPORATE_ACCOUNT, PROJECT_ID, LEVEL_ID, REQ_DESCRIPTION, STATUS, REQ_CRITICALITY, REQ_PRIORITY, CREATED_DATE, UPDATED_DATE,         
        REF_FIELD_1, REF_FIELD_2, REF_FIELD_3, REF_FIELD_4
        FROM REQUIREMENTS WHERE REQ_ID = %s AND CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s"""
        record = (req_id, corporate_account, project_id)


        cursor.execute(mySql_select_query, record)
        result = cursor.fetchone()

        if result:
            requirement_details = {
                'req_id': result[0],
                'corporate_account': result[1],
                'project_id': result[2],
                'level_id': result[3],
                'req_description': result[4],
                'status': result[5],
                'req_criticality': result[6],
                'req_priority': result[7],
                'created_date': result[8],
                'updated_date': result[9],
                'ref_field_1': result[10],
                'ref_field_2': result[11],
                'ref_field_3': result[12],
                'ref_field_4': result[13]
            }
        else:
            sts = "Failed"
            sts_description = "No matching requirement found"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the requirement details: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'requirement_details': requirement_details,
        'status': sts,
        'status_description': sts_description
    })




@requirements_blueprint.route('/api/add_requirement_approver', methods=['POST'])
@token_required
@validate_access
def add_requirement_approver(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    req_id = data.get('req_id')
    level_id = data.get('level_id')
    approval_user_id = data.get('approval_user_id')



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
    if req_id and not validate_req_id(corporate_account, project_id, req_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Requirement Id is not valid'
        })
    if level_id and not validate_level_id(corporate_account, project_id, level_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Level Id is not valid'
        })
    if not validate_user_id(corporate_account, approval_user_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'User Id is not valid'
        })

    if level_id and req_id:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Both level_id and req_id cannot be provided'
        })

    sts = "Success"
    sts_description = "User added as an approver successfully"
    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        if not level_id:
            level_id = 0
        if not req_id:
            req_id = 0

        mySql_insert_query = """INSERT INTO REQUIREMENTS_APPROVERS (CORPORATE_ACCOUNT, PROJECT_ID, REQ_ID, LEVEL_ID, APPROVAL_USER_ID, CREATED_DATE, APPROVAL_STATUS)
                                VALUES (%s, %s, %s, %s, %s, %s, 'Pending') """
        record = (corporate_account, project_id, req_id, level_id, approval_user_id, datetime.now())
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



@requirements_blueprint.route('/api/delete_requirement_approver', methods=['PUT', 'POST'])
@token_required
@validate_access
def delete_requirement_approver(current_user):

    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    req_id = data.get('req_id')
    level_id = data.get('level_id')
    approval_user_id = data.get('approval_user_id')

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
    if req_id and not validate_req_id(corporate_account, project_id, req_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Requirement Id is not valid'
        })
    if level_id and not validate_level_id(corporate_account, project_id, level_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Level Id is not valid'
        })
    if not validate_user_id(corporate_account, approval_user_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'User Id is not valid'
        })

    if level_id and req_id:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Both level_id and req_id cannot be provided'
        })

    sts = "Success"
    sts_description = "Requirement approver successfully deleted"
    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        if not level_id:
            level_id = 0
        if not req_id:
            req_id = 0

        mySql_insert_query = """DELETE FROM REQUIREMENTS_APPROVERS  
        WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND REQ_ID = %s AND LEVEL_ID = %s AND APPROVAL_USER_ID = %s """

        record = (corporate_account, project_id, req_id, level_id, approval_user_id)
        cursor.execute(mySql_insert_query, record)
        connection.commit()
        rows_impacted = cursor.rowcount
        if rows_impacted == 0:
            sts = "Failed"
            sts_description = "No matching approver found to delete"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to delete the requirement approver: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'status': sts,
        'status_description': sts_description
    })



@requirements_blueprint.route('/api/get_requirements_approver_list', methods=['GET', 'POST'])
@token_required
def get_requirements_approver_list(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    level_id = data.get('level_id')
    req_id = data.get('req_id')
    search_query = data.get('search_query')
    sort_criteria = data.get('sort_criteria')

    logging.info(f"data : {data}")

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

    if req_id and not validate_req_id(corporate_account, project_id, req_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Requirement Id is not valid'
        })
    if level_id and not validate_level_id(corporate_account, project_id, level_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Level Id is not valid'
        })

    if not level_id:
        level_id = 0

    if not req_id:
        req_id = 0

    sts = "Success"
    sts_description = "Requirements retrieved successfully"
    approver_details = {}
    approver_list = []

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        # Base query without ORDER BY
        mySql_select_query = f"""SELECT A.CORPORATE_ACCOUNT, PROJECT_ID, LEVEL_ID, REQ_ID, APPROVAL_USER_ID, USER_NAME,
        A.CREATED_DATE, A.UPDATED_DATE
        FROM REQUIREMENTS_APPROVERS A, USER_ACCOUNTS B WHERE
        A.CORPORATE_ACCOUNT = B.CORPORATE_ACCOUNT AND APPROVAL_USER_ID = USER_ID 
        AND A.CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND LEVEL_ID = %s AND REQ_ID = %s """

        # Build the query parameters list
        params = [corporate_account, project_id, level_id, req_id]



        if search_query:
                mySql_select_query += " AND ( APPROVAL_USER_ID LIKE %s OR USER_NAME LIKE %s) "
                params.extend([f"%{search_query}%", f"%{search_query}%"])

        if sort_criteria:
                mySql_select_query += " ORDER BY "
                mySql_select_query += sort_criteria

        cursor.execute(mySql_select_query, params)
        logging.info(f" executed SQL is: {cursor._executed}")

        for result in cursor.fetchall():
            approver_details = {
                'corporate_account': result[0],
                'project_id': result[1],
                'level_id': result[2],
                'req_id': result[3],
                'approval_user_id': result[4],
                'user_name': result[5],
                'created_date': result[6],
                'updated_date': result[7]
            }
            approver_list.append(approver_details)

        if len(approver_list) == 0:
            sts = "Failed"
            sts_description = "No matching approvers found"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the approver details: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'approver_list': approver_list,
        'status': sts,
        'status_description': sts_description
    })



@requirements_blueprint.route('/api/get_approver_list', methods=['GET', 'POST'])
@token_required
def get_approver_list(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')


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
    sts_description = "Approver list retrieved successfully"
    approver_details = {}
    approver_list = []

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        # Base query without ORDER BY
        mySql_select_query = f"""SELECT DISTINCT APPROVAL_USER_ID, USER_NAME FROM REQUIREMENTS_APPROVERS A, USER_ACCOUNTS B WHERE  
            A.CORPORATE_ACCOUNT = B.CORPORATE_ACCOUNT AND A.APPROVAL_USER_ID = B.USER_ID AND
              A.CORPORATE_ACCOUNT = %s AND A.PROJECT_ID = %s """

        # Build the query parameters list
        params = [corporate_account, project_id]
        cursor.execute(mySql_select_query, params)
        logging.info(f" executed SQL is: {cursor._executed}")

        for result in cursor.fetchall():
            approver_details = {
                'approval_user_id': result[0],
                'user_name': result[1]
            }
            approver_list.append(approver_details)

        if len(approver_list) == 0:
            sts = "Failed"
            sts_description = "No matching approvers found"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the approver details: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'approver_list': approver_list,
        'status': sts,
        'status_description': sts_description
    })



@requirements_blueprint.route('/api/update_requirement_approval_status', methods=['PUT', 'POST'])
@token_required
@validate_access
def update_requirement_approval_status(current_user):

    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    req_ids = data.get('req_ids', [])
    level_id = data.get('level_id')
    approval_user_id = data.get('approval_user_id')
    approval_comments = data.get('approval_comments')
    approval_status = data.get('approval_status')

    logging.info(f"Update req approval status - Data Received : {data}")

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

    if req_ids and not isinstance(req_ids, list):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Req Ids must be provided as an array'
        })

    if req_ids:
        if level_id:
            return jsonify({
                'status': 'Failed',
                'status_description': 'Both level_id and req_id cannot be provided'
            })
        for req_id in req_ids:
            if not validate_req_id(corporate_account, project_id, req_id):
                return jsonify({
                    'status': 'Failed',
                    'status_description': f'Requirement Id {req_id} is not valid'
                })

    if level_id and not validate_level_id(corporate_account, project_id, level_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Level Id is not valid'
        })
    if not validate_user_id(corporate_account, approval_user_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'User Id is not valid'
        })
    if not validate_status(corporate_account, project_id, 'REQUIREMENT_APPROVAL', approval_status):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Invalid approval status'
        })

    if req_ids:
        for req_id in req_ids:
            if not is_user_authorized_to_approve(corporate_account, project_id, approval_user_id, req_id, level_id):
                return jsonify({
                    'status': 'Failed',
                    'status_description': f'User is not authorized to approve the requirement {req_id}'
                })
    else:
        req_id = 0
        if not is_user_authorized_to_approve(corporate_account, project_id, approval_user_id, req_id, level_id):
            return jsonify({
                'status': 'Failed',
                'status_description': f'User is not authorized to approve the requirement'
            })




    sts = "Success"
    sts_description = "Requirement approval status updated successfully"
    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        if not level_id:
            level_id = 0

        mySql_update_query = """UPDATE REQUIREMENTS_APPROVERS SET APPROVAL_STATUS = %s, APPROVER_COMMENTS = %s, UPDATED_DATE = %s
        WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND REQ_ID = %s AND LEVEL_ID = %s AND APPROVAL_USER_ID = %s """

        if req_ids:
            for req_id in req_ids:
                record = (approval_status, approval_comments, datetime.now(), corporate_account, project_id, req_id, level_id, approval_user_id)
                cursor.execute(mySql_update_query, record)
                connection.commit()
        else:
                req_id =0
                record = (approval_status, approval_comments, datetime.now(), corporate_account, project_id, req_id, level_id, approval_user_id)
                cursor.execute(mySql_update_query, record)
                connection.commit()

        rows_impacted = cursor.rowcount
        logging.info(f"Rows impacted by update: {rows_impacted}")
        if rows_impacted == 0:
            mySql_insert_query = """INSERT INTO REQUIREMENTS_APPROVERS (CORPORATE_ACCOUNT, PROJECT_ID, REQ_ID, LEVEL_ID, APPROVAL_USER_ID, CREATED_DATE, APPROVAL_STATUS, APPROVER_COMMENTS)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s) """
            record = (corporate_account, project_id, req_id, level_id, approval_user_id, datetime.now(), approval_status, approval_comments)
            cursor.execute(mySql_insert_query, record)
            connection.commit()



        logging.info(f"update approval status executed SQL is: {cursor._executed}")

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



@requirements_blueprint.route('/api/get_requirements_approval_list', methods=['GET', 'POST'])
@token_required
def get_requirements_approval_list(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    level_id = data.get('level_id')
    req_ids = data.get('req_ids', [])
    filter_by_user = data.get('filter_by_user')
    sort_criteria = data.get('sort_criteria')
    req_type = data.get('req_type', 'REQUIREMENT')


    logging.info(f"data : {data}")

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
    for req_id in req_ids:
        if not validate_req_id(corporate_account, project_id, req_id):
            return jsonify({
                'status': 'Failed',
                'status_description': f'Requirement Id {req_id} is not valid'
            })

    if level_id and req_ids and len(req_ids) > 0:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Both level_id and req_id cannot be provided'
        })

    if not req_type == 'INTEGRATION' and not req_type == 'REQUIREMENT':
        return jsonify({
            'status': 'Failed',
            'status_description': 'Requirement type can be either INTEGRATION or REQUIREMENT'
        })

    if not level_id:
        level_id = 0

    sts = "Success"
    sts_description = "Requirements approval list retrieved successfully"
    requirement_approval_details = {}
    requirement_approval_list = []

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        # Base query without ORDER BY

        placeholders = ','.join(['%s'] * len(req_ids))

        if req_ids and len(req_ids) > 0:
            # if req_type == 'REQUIREMENT':
            mySql_select_query = f"""SELECT A.LEVEL_ID, A.REQ_ID, C.REQ_ID_WITH_PREFIX, APPROVAL_USER_ID, USER_NAME, APPROVER_COMMENTS, APPROVAL_STATUS, A.CREATED_DATE, A.UPDATED_DATE 
            FROM REQUIREMENTS_APPROVERS A, USER_ACCOUNTS B, REQUIREMENTS C
            WHERE A.CORPORATE_ACCOUNT = C.CORPORATE_ACCOUNT AND A.PROJECT_ID = C.PROJECT_ID AND A.REQ_ID = C.REQ_ID
            AND APPROVAL_USER_ID = USER_ID AND A.CORPORATE_ACCOUNT = B.CORPORATE_ACCOUNT
            AND A.CORPORATE_ACCOUNT = %s AND A.PROJECT_ID = %s
            AND A.LEVEL_ID = %s AND A.REQ_ID IN({placeholders})
            UNION
            SELECT A.LEVEL_ID, A.REQ_ID, C.INTEGRATION_ID_WITH_PREFIX, APPROVAL_USER_ID, USER_NAME, APPROVER_COMMENTS, APPROVAL_STATUS, A.CREATED_DATE, A.UPDATED_DATE 
            FROM REQUIREMENTS_APPROVERS A, USER_ACCOUNTS B, INTEGRATION_REQUIREMENTS C
            WHERE A.CORPORATE_ACCOUNT = C.CORPORATE_ACCOUNT AND A.PROJECT_ID = C.PROJECT_ID AND A.REQ_ID = C.INTEGRATION_ID
            AND APPROVAL_USER_ID = USER_ID AND A.CORPORATE_ACCOUNT = B.CORPORATE_ACCOUNT
            AND A.CORPORATE_ACCOUNT = %s AND A.PROJECT_ID = %s
            AND A.LEVEL_ID = %s AND A.REQ_ID IN({placeholders})"""

            params = [corporate_account, project_id, level_id]
            params.extend(req_ids)
            params.extend([corporate_account, project_id, level_id])
            params.extend(req_ids)

        else:
            mySql_select_query = f"""SELECT A.LEVEL_ID, A.REQ_ID, ' ' REQ_ID_WITH_PREFIX, APPROVAL_USER_ID, USER_NAME, APPROVER_COMMENTS, APPROVAL_STATUS, A.CREATED_DATE, A.UPDATED_DATE 
            FROM REQUIREMENTS_APPROVERS A, USER_ACCOUNTS B 
            WHERE APPROVAL_USER_ID = USER_ID AND A.CORPORATE_ACCOUNT = B.CORPORATE_ACCOUNT
            AND A.CORPORATE_ACCOUNT = %s AND A.PROJECT_ID = %s
            AND A.LEVEL_ID = %s AND A.REQ_ID = %s"""
            params = [corporate_account, project_id, level_id, 0]


        if filter_by_user:
            mySql_select_query += " AND APPROVAL_USER_ID = %s"
            params.append(filter_by_user)


        if sort_criteria:
            mySql_select_query += " ORDER BY " + sort_criteria

        else:
            mySql_select_query += " ORDER BY A.LEVEL_ID, A.REQ_ID"


        cursor.execute(mySql_select_query, tuple(params))

        logging.info(f" executed SQL is: {cursor._executed}")

        for result in cursor.fetchall():
            requirement_approval_details = {
                'level_id': result[0],
                'req_id': result[1],
                'req_id_with_prefix': result[2],
                'approval_user_id': result[3],
                'user_name': result[4],
                'approver_comments': result[5],
                'approval_status': result[6],
                'created_date': result[7],
                'updated_date': result[8]
            }
            requirement_approval_list.append(requirement_approval_details)

        if len(requirement_approval_list) == 0:
            sts = "Failed"
            sts_description = "No matching approvals found"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the requirement approval details: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'requirement_approval_list': requirement_approval_list,
        'status': sts,
        'status_description': sts_description
    })

@requirements_blueprint.route('/api/create_requirement_usecase', methods=['POST'])
@token_required
@validate_access
def create_requirement_usecase(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    level_id = data.get('level_id')
    req_id = data.get('req_id')
    usecase_description = data.get('usecase_description')
    acceptance_criteria = data.get('acceptance_criteria')
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

    if level_id and req_id:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Both level_id and req_id cannot be provided'
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

    if not usecase_description.strip():
        return jsonify({
            'status': 'Failed',
            'status_description': 'Usecase description is required'
        })

    if not validate_status(corporate_account, project_id, 'REQUIREMENT_USECASE', status):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Invalid usecase status'
        })


    usecase_id, seq_status, seq_status_description = generate_next_sequence(corporate_account, project_id, 'USECASE')
    usecase_id_with_prefix = None

    sts = "Success"
    sts_description = "Usecase successfully created"

    if not level_id:
        level_id = 0
    if not req_id:
        req_id = 0
    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        project_prefix = get_project_prefix(corporate_account, project_id)

        if project_prefix == 'Error':
            return jsonify({
                'req_id': None,
                'status': 'Failed',
                'status_description': 'Requirement prefix not defined'
            })

        usecase_id_with_prefix = f"{project_prefix}-uc-{usecase_id}"




        mySql_insert_query = """INSERT INTO REQUIREMENTS_USECASES (CORPORATE_ACCOUNT, PROJECT_ID, LEVEL_ID, REQ_ID, USECASE_ID, USECASE_ID_WITH_PREFIX, USECASE_DESCRIPTION, ACCEPTANCE_CRITERIA, STATUS, CREATED_DATE, UPDATED_DATE)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
        record = (corporate_account, project_id, level_id, req_id, usecase_id, usecase_id_with_prefix, usecase_description, acceptance_criteria,  status , datetime.now(), datetime.now())
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
        'usecase_id': usecase_id,
        'usecase_id_with_prefix': usecase_id_with_prefix,
        'status': sts,
        'status_description': sts_description
    })




@requirements_blueprint.route('/api/update_requirement_usecase', methods=['PUT', 'POST'])
@token_required
@validate_access
def update_requirement_usecase(current_user):
    data = request.json
    usecase_id = data.get('usecase_id')
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    usecase_description = data.get('usecase_description')
    acceptance_criteria = data.get('acceptance_criteria')
    status = data.get('status')

    sts = "Success"
    sts_description = "Usecase updated successfully"
    rows_impacted = 0


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
    if not validate_usecase_id(corporate_account, project_id, usecase_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Usecase Id is not valid'
        })
    if not usecase_description.strip():
        return jsonify({
            'status': 'Failed',
            'status_description': 'Usecase description is required'
        })
    if not validate_status(corporate_account, project_id, 'REQUIREMENT_USECASE', status):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Invalid usecase status'
        })
    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_update_query = """UPDATE REQUIREMENTS_USECASES SET USECASE_DESCRIPTION = %s, ACCEPTANCE_CRITERIA = %s, STATUS = %s, UPDATED_DATE = %s WHERE USECASE_ID = %s AND CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s"""
        record = (usecase_description, acceptance_criteria, status, datetime.now(), usecase_id, corporate_account, project_id )
        cursor.execute(mySql_update_query, record)
        rows_impacted = cursor.rowcount
        if rows_impacted == 0:
            sts = "Failed"
            sts_description = "No matching usecase found to update"


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
        'usecase_id': usecase_id,
        'status': sts,
        'status_description': sts_description,
        'rows_impacted': rows_impacted
    })



@requirements_blueprint.route('/api/delete_requirement_usecases', methods=['PUT','POST'])
@token_required
@validate_access
def delete_requirement_usecases(current_user):


    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    usecase_ids = data.get('usecase_ids',[])

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
    if not usecase_ids or not isinstance(usecase_ids, list):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Usecase Id is required'
        })

    sts = "Success"
    sts_description = "Usecase(s) successfully deleted"
    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()


        placeholders = ','.join(['%s'] * len(usecase_ids))


        mySql_insert_query = f"""DELETE FROM REQUIREMENTS_USECASES
        WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND USECASE_ID IN ({placeholders})"""

        record = (corporate_account, project_id)  + tuple(usecase_ids)

        cursor.execute(mySql_insert_query, record)


        rows_impacted = cursor.rowcount
        if rows_impacted == 0:
            sts = "Failed"
            sts_description = "No matching usecases found to delete"


        connection.commit()



    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to delete the usecases: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'status': sts,
        'status_description': sts_description
    })


@requirements_blueprint.route('/api/create_project_link', methods=['POST'])
@token_required
@validate_access
def create_project_link(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    project_link_type = data.get('project_link_type')
    source_record_type = data.get('source_record_type')
    source_ids = data.get('source_ids')  # Changed from source_id to source_ids (array)
    target_record_type = data.get('target_record_type')
    target_id = data.get('target_id')


    logging.info(f"Create project link - Data Received : {data}")

    source_id_with_prefix = None
    target_id_with_prefix = None

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

    if not project_link_type:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Link type is required'
        })

    if not source_record_type or not target_record_type:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Source and target record types are required'
        })

    if not source_ids or not isinstance(source_ids, list) or len(source_ids) == 0:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Source ids must be a non-empty array'
        })

    if not target_id:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Target id is required'
        })

    if not validate_status(corporate_account, project_id, 'RECORD_TYPE', source_record_type):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Invalid source record type'
        })

    if not validate_status(corporate_account, project_id, 'RECORD_TYPE', target_record_type):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Invalid target record type'
        })

    if not validate_status(corporate_account, project_id, 'LINK_TYPE', project_link_type):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Invalid link type'
        })

    # Validate target ID
    if target_record_type == 'REQUIREMENT' or target_record_type == 'INTEGRATION_REQUIREMENT':
            if not validate_req_id(corporate_account, project_id, target_id):
                return jsonify({
                    'status': 'Failed',
                    'status_description': f'Target Id is not valid'
                })
            project_prefix = get_project_prefix(corporate_account, project_id)
            if project_prefix == 'Error':
                return jsonify({
                    'status': 'Failed',
                    'status_description': 'Requirement prefix not defined'
                })
            target_id_with_prefix = f"{project_prefix.strip()}-{target_id}"

    elif target_record_type == 'USECASE' :
        if not validate_usecase_id(corporate_account, project_id, target_id):
            return jsonify({
                'status': 'Failed',
                'status_description': f'Target usecase Id is not valid'
            })

        project_prefix = get_project_prefix(corporate_account, project_id)
        if project_prefix == 'Error':
            return jsonify({
                'status': 'Failed',
                'status_description': 'Requirement prefix not defined'
            })
        target_id_with_prefix = f"{project_prefix}-uc-{target_id}"


    elif (target_record_type == 'RISK' or target_record_type == 'ACTION' or target_record_type == 'ISSUE' or target_record_type == 'DECISION'
            or target_record_type == 'QUESTION' or target_record_type == 'TASK'):
        target_id_with_prefix = f"{target_record_type}-{target_id}"
        if not validate_raid_log_entry(corporate_account, project_id, target_record_type, target_id):
                return jsonify({
                    'status': 'Failed',
                    'status_description': f'Target Id is not valid {target_record_type}'
                })
        logging.info("hello 2")

    # List to store created project link IDs
    project_link_ids = []
    failed_source_ids = []
    logging.info("hello 1")

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        # Process each source ID
        for source_id in source_ids:
            # Check if source and target are the same
            if source_record_type == target_record_type and source_id == target_id:
                failed_source_ids.append({
                    'source_id': source_id,
                    'reason': 'Source and target cannot be the same'
                })
                continue

            # Validate source ID based on record type
            is_valid_source = False

            if source_record_type == 'REQUIREMENT' or source_record_type == 'INTEGRATION_REQUIREMENT':
                is_valid_source = validate_req_id(corporate_account, project_id, source_id)

                project_prefix = get_project_prefix(corporate_account, project_id)
                if project_prefix == 'Error':
                    return jsonify({
                        'status': 'Failed',
                        'status_description': 'Requirement prefix not defined'
                    })
                source_id_with_prefix = f"{project_prefix.strip()}-{source_id}"



            elif source_record_type == 'USECASE':
                is_valid_source = validate_usecase_id(corporate_account, project_id, source_id)

                project_prefix = get_project_prefix(corporate_account, project_id)
                if project_prefix == 'Error':
                    return jsonify({
                        'status': 'Failed',
                        'status_description': 'Requirement prefix not defined'
                    })
                source_id_with_prefix = f"{project_prefix}-uc-{source_id}"




            elif (source_record_type == 'RISK' or source_record_type == 'ACTION' or source_record_type == 'ISSUE'
                  or source_record_type == 'DECISION' or source_record_type == 'QUESTION' or source_record_type == 'TASK'):
                is_valid_source = validate_raid_log_entry(corporate_account, project_id, source_record_type, source_id)


                if not is_valid_source:
                    failed_source_ids.append({
                        'source_id': source_id,
                        'reason': f'Invalid source ID for {source_record_type}'
                    })
                    continue

                source_id_with_prefix = f"{source_record_type}-{source_id}"
                logging.info("hello 3")


            # Generate project link ID
            project_link_id, seq_status, seq_status_description = generate_next_sequence(
                corporate_account, project_id, 'PROJECT_LINK')

            if seq_status != "Success":
                failed_source_ids.append({
                    'source_id': source_id,
                    'reason': seq_status_description
                })
                continue
            logging.info("hello 4")

            # Insert the link into the database
            mySql_insert_query = """INSERT INTO PROJECT_LINKS 
                (CORPORATE_ACCOUNT, PROJECT_ID, PROJECT_LINK_TYPE, PROJECT_LINK_ID, 
                SOURCE_RECORD_TYPE, SOURCE_ID, SOURCE_ID_WITH_PREFIX, TARGET_RECORD_TYPE, TARGET_ID, TARGET_ID_WITH_PREFIX, CREATED_DATE, UPDATED_DATE)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """

            record = (corporate_account, project_id, project_link_type, project_link_id,
                      source_record_type, source_id, source_id_with_prefix, target_record_type, target_id, target_id_with_prefix,
                      datetime.now(), datetime.now())

            logging.info(f"Record contents: {record}")
            cursor.execute(mySql_insert_query, record)
            logging.info(f" executed SQL is: {cursor._executed}")


            project_link_ids.append({
                'source_id': source_id,
                'project_link_id': project_link_id
            })

        connection.commit()

        # Determine overall status
        if len(project_link_ids) > 0 and len(failed_source_ids) == 0:
            status = "Success"
            status_description = "All links successfully established"
        elif len(project_link_ids) > 0 and len(failed_source_ids) > 0:
            status = "Partial"
            status_description = "Some links were successfully established"
        else:
            status = "Failed"
            status_description = "Failed to establish any links"

    except mysql.connector.Error as error:
        status = "Failed"
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
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'status': status,
        'status_description': status_description,
        'successful_links': project_link_ids,
        'failed_links': failed_source_ids
    })






@requirements_blueprint.route('/api/delete_project_links', methods=['PUT','POST'])
@token_required
@validate_access
def delete_project_links(current_user):


    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    project_link_ids = data.get('project_link_ids',[])

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
    if not project_link_ids or not isinstance(project_link_ids, list):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Comment Id is required'
        })

    sts = "Success"
    sts_description = "Links successfully deleted"
    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()


        placeholders = ','.join(['%s'] * len(project_link_ids))


        mySql_insert_query = f"""DELETE FROM PROJECT_LINKS
        WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND PROJECT_LINK_ID IN ({placeholders})"""

        record = (corporate_account, project_id)  + tuple(project_link_ids)

        cursor.execute(mySql_insert_query, record)
        logging.info(f" executed SQL is: {cursor._executed}")
        connection.commit()
        rows_impacted = cursor.rowcount
        if rows_impacted == 0:
            sts = "Failed"
            sts_description = "No matching links found to delete"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to delete the links: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'status': sts,
        'status_description': sts_description
    })







@requirements_blueprint.route('/api/get_links_list', methods=['GET', 'POST'])
@token_required
def get_links_list(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    source_record_type = data.get('source_record_type')
    source_id = data.get('source_id')
    sort_criteria = data.get('sort_criteria')
    search_query = data.get('search_query')

    logging.info(f"data : {data}")

    sts = "Success"
    sts_description = "Links retrieved successfully"
    search_results_details = {}
    search_results_list = []


    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()



        if not search_query:
            search_query = ''
        if not sort_criteria:
            sort_criteria = 'TARGET_RECORD_TYPE, TARGET_ID'



        # Base query without ORDER BY
        mySql_select_query = f""" SELECT PROJECT_LINK_TYPE, PROJECT_LINK_ID, SOURCE_RECORD_TYPE, SOURCE_ID, TARGET_RECORD_TYPE, TARGET_ID, 
        CREATED_DATE, UPDATED_DATE FROM PROJECT_LINKS 
        WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND  
        ( (SOURCE_RECORD_TYPE = %s AND SOURCE_ID = %s) OR (TARGET_RECORD_TYPE = %s AND TARGET_ID = %s) ) """


        params = [corporate_account, project_id , source_record_type, source_id, source_record_type, source_id]

        logging.info(f"SQL : {mySql_select_query}")

        cursor.execute(mySql_select_query, tuple(params))

        logging.info(f" executed SQL is: {cursor._executed}")

        for result in cursor.fetchall():
            source_id_with_prefix, source_description, source_status, id_sts, id_sts_description = get_link_details(corporate_account, project_id, result[2], result[3])
            if not id_sts:
                    continue
            else:
                target_id_with_prefix, target_description, target_status, id_sts, id_sts_description = get_link_details(
                    corporate_account, project_id, result[4], result[5])
                if not id_sts:
                    continue

            search_results_details = {
                'project_link_type': result[0],
                'project_link_id': result[1],
                'source_record_type': result[2],
                'source_id': result[3],
                'source_id_with_prefix': source_id_with_prefix,
                'source_description': source_description,
                'source_status': source_status,
                'target_record_type': result[4],
                'target_id': result[5],
                'target_id_with_prefix': target_id_with_prefix,
                'target_description': target_description,
                'target_status': target_status,
                'created_date': result[6]
            }
            search_results_list.append(search_results_details)


        if len(search_results_list) == 0:
            sts = "Failed"
            sts_description = "No matching results found"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the search results: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'search_results_list': search_results_list,
        'status': sts,
        'status_description': sts_description
    })












@requirements_blueprint.route('/api/get_search_results_list', methods=['GET', 'POST'])
@token_required
def get_search_results_list(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    filter_by_status = data.get('filter_by_status', [])
    search_query = data.get('search_query')
    sort_criteria = data.get('sort_criteria')
    source_record_type = data.get('source_record_type')
    source_id = data.get('source_id')


    logging.info(f"data : {data}")

    sts = "Success"
    sts_description = "Search results retrieved successfully"
    search_results_details = {}
    search_results_list = []


    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        placeholders = ','.join(['%s'] * len(filter_by_status))


        if not search_query:
            search_query = ''
        if not sort_criteria:
            sort_criteria = 'TARGET_ID_WITH_PREFIX'



        # Base query without ORDER BY
        mySql_select_query = f""" WITH TEMP AS (
        SELECT 'REQUIREMENT' TARGET_RECORD_TYPE, REQ_ID TARGET_ID, REQ_ID_WITH_PREFIX TARGET_ID_WITH_PREFIX, REQ_DESCRIPTION TARGET_ID_DESCRIPTION, STATUS  FROM REQUIREMENTS
        WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID =  %s
        UNION
        SELECT 'REQUIREMENT' TARGET_RECORD_TYPE, INTEGRATION_ID TARGET_ID, INTEGRATION_ID_WITH_PREFIX TARGET_ID_WITH_PREFIX, INTEGRATION_NAME TARGET_ID_DESCRIPTION, STATUS  FROM INTEGRATION_REQUIREMENTS
        WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s
        UNION
        SELECT 'USECASE' TARGET_RECORD_TYPE, USECASE_ID TARGET_ID, USECASE_ID_WITH_PREFIX TARGET_ID_WITH_PREFIX, USECASE_DESCRIPTION TARGET_ID_DESCRIPTION, STATUS  FROM  REQUIREMENTS_USECASES
        WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s
        UNION
        SELECT RAID_TYPE TARGET_RECORD_TYPE,  RAID_ID TARGET_ID, RAID_ID_WITH_PREFIX TARGET_ID_WITH_PREFIX, RAID_DESCRIPTION TARGET_ID_DESCRIPTION, STATUS  FROM  RAID_LOG
        WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s
        )
        SELECT * FROM TEMP A  WHERE NOT EXISTS (SELECT * FROM PROJECT_LINKS X WHERE X.SOURCE_RECORD_TYPE = %s AND X.SOURCE_ID = %s AND A.TARGET_RECORD_TYPE = X.TARGET_RECORD_TYPE AND A.TARGET_ID = X.TARGET_ID) """


        params = [corporate_account, project_id , corporate_account, project_id, corporate_account, project_id, corporate_account, project_id, source_record_type, source_id]

        if filter_by_status and len(filter_by_status) > 0:
            mySql_select_query += f"""AND STATUS IN({placeholders}) """
            params.extend(filter_by_status)

        if search_query:
                mySql_select_query += " AND ( TARGET_RECORD_TYPE LIKE %s OR TARGET_ID_WITH_PREFIX LIKE %s OR TARGET_ID_DESCRIPTION LIKE %s) ORDER BY " + sort_criteria
                params.extend([  f"%{search_query}%" , f"%{search_query}%" , f"%{search_query}%"])

        else:
                mySql_select_query += "  ORDER BY " + sort_criteria



        logging.info(f"SQL : {mySql_select_query}")

        cursor.execute(mySql_select_query, tuple(params))

        logging.info(f" executed SQL is: {cursor._executed}")

        for result in cursor.fetchall():
            search_results_details = {
                'target_record_type': result[0],
                'target_id': result[1],
                'target_id_with_prefix': result[2],
                'target_id_description': result[3],
                'status': result[4]
            }
            search_results_list.append(search_results_details)

        if len(search_results_list) == 0:
            sts = "Failed"
            sts_description = "No matching results found"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the search results: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'search_results_list': search_results_list,
        'status': sts,
        'status_description': sts_description
    })





@requirements_blueprint.route('/api/get_usecase_list', methods=['GET', 'POST'])
@token_required
def get_usecase_list(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    level_id = data.get('level_id')
    req_id = data.get('req_id')
    filter_by_status = data.get('filter_by_status', [])
    search_query = data.get('search_query')
    sort_criteria = data.get('sort_criteria')

    logging.info(f"data : {data}")

    if not filter_by_status or not isinstance(filter_by_status, list):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Filter by status must be provided as an array'
        })

    sts = "Success"
    sts_description = "Requirements retrieved successfully"
    usecase_details = {}
    usecase_list = []


    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        placeholders = ','.join(['%s'] * len(filter_by_status))

        if not level_id:
            level_id = 0
        if not req_id:
            req_id = 0
        if not search_query:
            search_query = ''
        if not sort_criteria:
            sort_criteria = 'USECASE_ID'



        # Base query without ORDER BY
        mySql_select_query = f"""SELECT A.USECASE_ID, A.USECASE_ID_WITH_PREFIX, A.USECASE_DESCRIPTION,
        A.LEVEL_ID, B.LEVEL_DESCRIPTION, A.REQ_ID, IF(C.REQ_ID_WITH_PREFIX IS NULL,  D.INTEGRATION_ID_WITH_PREFIX, C.REQ_ID_WITH_PREFIX ), A.STATUS, A.CREATED_DATE, A.UPDATED_DATE, 
        (SELECT COUNT(*) FROM REQUIREMENTS_TESTCASES D WHERE A.CORPORATE_ACCOUNT = D.CORPORATE_ACCOUNT AND A.PROJECT_ID = D.PROJECT_ID AND A.USECASE_ID = D.USECASE_ID) NUMBER_OF_TESTCASES
           FROM REQUIREMENTS_USECASES A LEFT OUTER JOIN FUNCTIONAL_LEVELS B ON A.CORPORATE_ACCOUNT = B.CORPORATE_ACCOUNT AND A.PROJECT_ID = B.PROJECT_ID AND A.LEVEL_ID = B.LEVEL_ID 
           LEFT OUTER JOIN REQUIREMENTS C ON A.CORPORATE_ACCOUNT = C.CORPORATE_ACCOUNT AND A.PROJECT_ID = C.PROJECT_ID AND A.REQ_ID = C.REQ_ID 
           LEFT OUTER JOIN INTEGRATION_REQUIREMENTS D ON A.CORPORATE_ACCOUNT = D.CORPORATE_ACCOUNT AND A.PROJECT_ID = D.PROJECT_ID AND A.REQ_ID = D.INTEGRATION_ID 
           WHERE A.CORPORATE_ACCOUNT = %s AND A.PROJECT_ID = %s  """


        params = [corporate_account, project_id]

        if filter_by_status and len(filter_by_status) > 0:
            mySql_select_query += f"""AND A.STATUS IN({placeholders}) """
            params.extend(filter_by_status)

        if search_query:
                mySql_select_query += " AND A.LEVEL_ID = %s AND A.REQ_ID = %s  AND ( A.USECASE_ID_WITH_PREFIX LIKE %s OR A.USECASE_DESCRIPTION LIKE %s) ORDER BY " + sort_criteria
                params.append(level_id)
                params.append(req_id)
                params.extend([   f"%{search_query}%" , f"%{search_query}%"])

        else:
                mySql_select_query += " AND A.LEVEL_ID = %s AND A.REQ_ID = %s ORDER BY " + sort_criteria
                params.append(level_id)
                params.append(req_id)


        logging.info(f"SQL : {mySql_select_query}")

        cursor.execute(mySql_select_query, tuple(params))

        logging.info(f" executed SQL is: {cursor._executed}")

        for result in cursor.fetchall():
            usecase_details = {
                'usecase_id': result[0],
                'usecase_id_with_prefix': result[1],
                'usecase_description': result[2],
                'level_id': result[3],
                'level_description': result[4],
                'req_id': result[5],
                'req_id_with_prefix': result[6],
                'status': result[7],
                'created_date': result[8],
                'updated_date': result[9],
                'number_of_testcases': result[10]
            }
            usecase_list.append(usecase_details)

        if len(usecase_list) == 0:
            sts = "Failed"
            sts_description = "No matching usecases found"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the usecase details: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'usecase_list': usecase_list,
        'status': sts,
        'status_description': sts_description
    })



@requirements_blueprint.route('/api/get_usecase_details', methods=['GET', 'POST'])
@token_required
def get_usecase_details(current_user):
    data = request.json
    usecase_id = data.get('usecase_id')
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')

    sts = "Success"
    sts_description = "Usecase details retrieved successfully"
    usecase_details = {}

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_select_query = f"""SELECT CORPORATE_ACCOUNT, PROJECT_ID, LEVEL_ID, REQ_ID, USECASE_ID, USECASE_ID_WITH_PREFIX, USECASE_DESCRIPTION, ACCEPTANCE_CRITERIA,
        STATUS, CREATED_DATE, UPDATED_DATE FROM REQUIREMENTS_USECASES 
        WHERE USECASE_ID = %s AND CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s"""

        record = (usecase_id, corporate_account, project_id)


        cursor.execute(mySql_select_query, record)
        result = cursor.fetchone()

        if result:
            usecase_details = {
                'corporate_account': result[0],
                'project_id': result[1],
                'level_id': result[2],
                'req_id': result[3],
                'usecase_id': result[4],
                'usecase_id_with_prefix': result[5],
                'usecase_description': result[6],
                'usecase_acceptance_criteria': result[7],
                'status': result[8],
                'created_date': result[9],
                'updated_date': result[10]
            }
        else:
            sts = "Failed"
            sts_description = "No matching usecase found"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the usecase details: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'usecase_details': usecase_details,
        'status': sts,
        'status_description': sts_description
    })

@requirements_blueprint.route('/api/create_requirement_testcase', methods=['POST'])
@token_required
@validate_access
def create_requirement_testcase(current_user):

    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    usecase_id = data.get('usecase_id')
    testcase_description = data.get('testcase_description')
    acceptance_criteria = data.get('acceptance_criteria')


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
    if not validate_usecase_id(corporate_account, project_id, usecase_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Usecase Id is not valid'
        })
    if not testcase_description:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Testcase description is required'
        })

    testcase_id, seq_status, seq_status_description = generate_next_sequence(corporate_account, project_id, 'TESTCASE')

    sts = "Success"
    sts_description = "Testcase successfully created"
    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        mySql_insert_query = """INSERT INTO REQUIREMENTS_TESTCASES (CORPORATE_ACCOUNT, PROJECT_ID, USECASE_ID, TESTCASE_ID, 
        TESTCASE_DESCRIPTION, ACCEPTANCE_CRITERIA, STATUS, CREATED_DATE, UPDATED_DATE)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) """
        record = (corporate_account, project_id, usecase_id, testcase_id, testcase_description, acceptance_criteria, 'Created', datetime.now(), datetime.now())
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


@requirements_blueprint.route('/api/update_requirement_testcase', methods=['PUT'])
@token_required
@validate_access
def update_requirement_testcase(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    usecase_id = data.get('usecase_id')
    testcase_id = data.get('testcase_id')
    testcase_description = data.get('testcase_description')
    acceptance_criteria = data.get('acceptance_criteria')
    status = data.get('status')

    sts = "Success"
    sts_description = "Testcase updated successfully"
    rows_impacted = 0


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
    if not validate_usecase_id(corporate_account, project_id, usecase_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Usecase Id is not valid'
        })
    if not validate_testcase_id(corporate_account, project_id, testcase_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Testcase Id is not valid'
        })
    if not testcase_description.strip():
        return jsonify({
            'status': 'Failed',
            'status_description': 'Testcase description is required'
        })
    if not validate_status(corporate_account, project_id, 'REQUIREMENT_TESTCASE', status):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Invalid testcase status'
        })
    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_update_query = """UPDATE REQUIREMENTS_TESTCASES SET USECASE_ID = %s, TESTCASE_DESCRIPTION = %s, ACCEPTANCE_CRITERIA = %s, STATUS = %s, UPDATED_DATE = %s WHERE TESTCASE_ID = %s AND CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s"""
        record = (usecase_id, testcase_description, acceptance_criteria, status, datetime.now(), testcase_id, corporate_account, project_id )
        cursor.execute(mySql_update_query, record)
        connection.commit()
        rows_impacted = cursor.rowcount
        if rows_impacted == 0:
            sts = "Failed"
            sts_description = "No matching testcase found to update"

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
        'testcase_id': testcase_id,
        'status': sts,
        'status_description': sts_description,
        'rows_impacted': rows_impacted
    })


@requirements_blueprint.route('/api/get_testcase_details', methods=['GET'])
@token_required
def get_testcase_details(current_user):

    data = request.json
    testcase_id = data.get('testcase_id')
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')

    sts = "Success"
    sts_description = "Testcase details retrieved successfully"
    testcase_details = {}

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_select_query = """SELECT CORPORATE_ACCOUNT, PROJECT_ID, TESTCASE_ID, USECASE_ID, TESTCASE_DESCRIPTION, ACCEPTANCE_CRITERIA, 
        STATUS, CREATED_DATE, UPDATED_DATE FROM REQUIREMENTS_TESTCASES 
        WHERE TESTCASE_ID = %s AND CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s"""

        record = (testcase_id, corporate_account, project_id)


        cursor.execute(mySql_select_query, record)
        result = cursor.fetchone()

        if result:
            testcase_details = {
                'corporate_account': result[0],
                'project_id': result[1],
                'testcase_id': result[2],
                'usecase_id': result[3],
                'testcase_description': result[4],
                'acceptance_criteria': result[5],
                'status': result[6],
                'created_date': result[7],
                'updated_date': result[8]
            }
        else:
            sts = "Failed"
            sts_description = "No matching testcase found"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the testcase details: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'testcase_details': testcase_details,
        'status': sts,
        'status_description': sts_description
    })



@requirements_blueprint.route('/api/add_comments', methods=['POST'])
@token_required
@validate_access
def add_comments(current_user):

    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    req_ids = data.get('req_ids', [])
    req_type = data.get('req_type', 'REQUIREMENT')
    level_id = data.get('level_id')
    comments = data.get('comments')
    status = data.get('status')
    user_id = data.get('user_id')

    logging.info(f"Input data: {data}")

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
    # if req_ids and req_type not in ['REQUIREMENT', 'INTEGRATION', 'USECASE']:
    #     return jsonify({
    #         'status': 'Failed',
    #         'status_description': 'Requirement type can be either REQUIREMENT, INTEGRATION or USECASE'
    #     })

    if req_ids:
        for req_id in req_ids:
            if req_type == 'INTEGRATION':
                logging.info("req_type is integration")
                if not validate_integration_id(corporate_account, project_id, req_id):
                    return jsonify({
                        'status': 'Failed',
                        'status_description': f'Integration Id {req_id} is not valid'
                    })
            else:
                if req_type == 'REQUIREMENT':
                    logging.info("req_type is requirement")
                    if not validate_req_id(corporate_account, project_id, req_id):
                        return jsonify({
                            'status': 'Failed',
                            'status_description': f'Requirement Id {req_id} is not valid'
                        })
                else:
                    if req_type == 'USECASE' :
                        logging.info("req_type is usecase")
                        if not validate_usecase_id(corporate_account, project_id, req_id):
                            return jsonify({
                                'status': 'Failed',
                                'status_description': f'Usecase Id {req_id} is not valid'
                            })
                    else:
                        logging.info("req_type is raid log")
                        if not validate_raid_log_entry(corporate_account, project_id, req_type, req_id):
                            return jsonify({
                                'status': 'Failed',
                                'status_description': f'Invalid requirement type - {req_type}'
                        })
    if level_id and req_ids and len(req_ids) > 0:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Both level_id and req_id cannot be provided'
        })

    if level_id and not validate_level_id(corporate_account, project_id, level_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Level Id is not valid'
        })

    if not comments :
        return jsonify({
            'status': 'Failed',
            'status_description': 'Comments are required'
        })

    if not user_id :
        return jsonify({
            'status': 'Failed',
            'status_description': 'user id is required'
        })
    if not validate_user_id(corporate_account, user_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'User Id is not valid'
        })
    if not validate_status(corporate_account, project_id, 'COMMENTS', status):
        return jsonify({
            'status': 'Failed',
            'status_description': f'Invalid comment status - {status} '
        })



    sts = "Success"
    sts_description = "comments added successfully"
    try:

        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        mySql_insert_query = f"""INSERT INTO REQUIREMENTS_COMMENTS (CORPORATE_ACCOUNT, PROJECT_ID, COMMENT_ID, REQ_TYPE, REQ_ID, LEVEL_ID, COMMENTS, 
        STATUS, CREATED_DATE, UPDATED_DATE, USER_ID) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """

        if not req_ids:
            if not level_id:
                level_id = 0
            comment_id, seq_status, seq_status_description = generate_next_sequence(corporate_account, project_id,
                                                                                    'COMMENT')

            if seq_status == "Failed":
                sts = "Failed"
                sts_description = seq_status_description
                return jsonify({
                    'status': sts,
                    'status_description': sts_description
                })
            record = (corporate_account, project_id, comment_id, req_type, 0, level_id, comments, status, datetime.now(), datetime.now(), user_id)
            cursor.execute(mySql_insert_query, record)
        else:
            level_id = 0
            for req_id in req_ids:
                comment_id, seq_status, seq_status_description = generate_next_sequence(corporate_account, project_id,
                                                                                        'COMMENT')

                if seq_status == "Failed":
                    sts = "Failed"
                    sts_description = seq_status_description
                    return jsonify({
                        'status': sts,
                        'status_description': sts_description
                    })
                record = (corporate_account, project_id, comment_id, req_type, req_id, level_id, comments, status, datetime.now(), datetime.now(), user_id)
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





@requirements_blueprint.route('/api/get_comments_list', methods=['GET','POST'])
@token_required
def get_comments_list(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    level_id = data.get('level_id')
    req_type = data.get('req_type')
    req_id = data.get('req_id')
    filter_by_status = data.get('filter_by_status', [])
    search_query = data.get('search_query')
    sort_criteria = data.get('sort_criteria')

    logging.info(f"data : {data}")

    # if not filter_by_status or not isinstance(filter_by_status, list):
    #     return jsonify({
    #         'status': 'Failed',
    #         'status_description': 'Filter by status must be provided as an array'
    #     })

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

    if level_id and req_id:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Both level_id and req_id cannot be provided'
        })
    # if req_id and req_type not in ['REQUIREMENT', 'INTEGRATION', 'USECASE']:
    #     return jsonify({
    #         'status': 'Failed',
    #         'status_description': 'Requirement type can be either REQUIREMENT, INTEGRATION or USECASE'
    #     })
    if req_type == 'REQUIREMENT':
        if not validate_req_id(corporate_account, project_id, req_id):
            return jsonify({
                'status': 'Failed',
                'status_description': 'Requirement Id is not valid'
            })
    else:
        if req_type == 'INTEGRATION':
            if not validate_integration_id(corporate_account, project_id, req_id):
                return jsonify({
                    'status': 'Failed',
                    'status_description': f'Integration Id is not valid'
                })
        else:
            if req_type == 'USECASE':
                if not validate_usecase_id(corporate_account, project_id, req_id):
                    return jsonify({
                        'status': 'Failed',
                        'status_description': f'Usecase Id is not valid'
                    })
            else:
                if req_type and not validate_raid_log_entry(corporate_account, project_id, req_type, req_id):
                    return jsonify({
                        'status': 'Failed',
                        'status_description': f'Invalid requirement type - {req_type}'
                    })
    if level_id and not validate_level_id(corporate_account, project_id, level_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Level Id is not valid'
        })


    placeholders = ','.join(['%s'] * len(filter_by_status))

    if not level_id:
        level_id = 0
    if not req_id:
        req_id = 0
    if not search_query:
        search_query = ''
    if not sort_criteria:
        sort_criteria = 'UPDATED_DATE DESC'


    sts = "Success"
    sts_description = "comments retrieved successfully"
    comments_details = {}
    comments_list = []

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        mySql_select_query = f""" SELECT A.COMMENT_ID, A.COMMENTS,
        A.LEVEL_ID, IF(B.LEVEL_DESCRIPTION IS NULL, 'NA',B.LEVEL_DESCRIPTION) LEVEL_DESCRIPTION, A.REQ_ID,  
        IF( REQ_TYPE = 'REQUIREMENT', C.REQ_ID_WITH_PREFIX, IF( REQ_TYPE = 'INTEGRATION',D.INTEGRATION_ID_WITH_PREFIX,'NA' )  ) REQ_ID,
        IF( REQ_TYPE = 'USECASE', E.USECASE_ID_WITH_PREFIX ,'NA'  ) USECASE_ID,
           A.STATUS, A.CREATED_DATE, A.UPDATED_DATE, A.USER_ID, F.USER_NAME ,
           G.RAID_ID_WITH_PREFIX, G.RAID_TYPE
           FROM REQUIREMENTS_COMMENTS A LEFT OUTER JOIN FUNCTIONAL_LEVELS B ON A.CORPORATE_ACCOUNT = B.CORPORATE_ACCOUNT AND A.PROJECT_ID = B.PROJECT_ID AND A.LEVEL_ID = B.LEVEL_ID 
           LEFT OUTER JOIN REQUIREMENTS C ON A.CORPORATE_ACCOUNT = C.CORPORATE_ACCOUNT AND A.PROJECT_ID = C.PROJECT_ID AND A.REQ_ID = C.REQ_ID 
           LEFT OUTER JOIN INTEGRATION_REQUIREMENTS D ON A.CORPORATE_ACCOUNT = D.CORPORATE_ACCOUNT AND A.PROJECT_ID = D.PROJECT_ID AND A.REQ_ID = D.INTEGRATION_ID 
           LEFT OUTER JOIN REQUIREMENTS_USECASES E ON A.CORPORATE_ACCOUNT = E.CORPORATE_ACCOUNT AND A.PROJECT_ID = E.PROJECT_ID AND A.REQ_ID = E.USECASE_ID 
           LEFT OUTER JOIN RAID_LOG G ON A.CORPORATE_ACCOUNT = G.CORPORATE_ACCOUNT AND A.PROJECT_ID = G.PROJECT_ID AND A.REQ_ID = G.RAID_ID ,
        USER_ACCOUNTS F 
           WHERE  A.CORPORATE_ACCOUNT = %s AND A.PROJECT_ID = %s AND F.USER_ID = A.USER_ID """


        # Build the query parameters list
        params = [corporate_account, project_id]

        if filter_by_status and len(filter_by_status) > 0:
            mySql_select_query += f"""AND A.STATUS IN({placeholders}) """
            params.extend(filter_by_status)

        if search_query:
            mySql_select_query += " AND A.LEVEL_ID = %s AND A.REQ_ID = %s AND ( A.COMMENTS LIKE %s) ORDER BY " + sort_criteria
            params.append(level_id)
            params.append(req_id)
            params.extend([f"%{search_query}%"])

        else:
            mySql_select_query += " AND A.LEVEL_ID = %s AND A.REQ_ID = %s ORDER BY " + sort_criteria
            params.append(level_id)
            params.append(req_id)


        cursor.execute(mySql_select_query, tuple(params))

        logging.info(f" executed SQL is: {cursor._executed}")

        for result in cursor.fetchall():
            comments_details = {
                'comment_id': result[0],
                'comments': result[1],
                'level_id': result[2],
                'level_description': result[3],
                'req_id': result[4],
                'req_id_with_prefix': result[5],
                'usecase_id_with_prefix': result[6],
                'status': result[7],
                'created_date': result[8],
                'updated_date': result[9],
                'user_id': result[10],
                'user_name': result[11],
                'raid_id_with_prefix': result[12],
                'raid_type': result[13]
            }
            comments_list.append(comments_details)

        logging.info(f" array lenght is : {len(comments_list) }")

        if len(comments_list) == 0:
            sts = "Failed"
            sts_description = "No matching entries found"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the requirement details: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'comments_list': comments_list,
        'status': sts,
        'status_description': sts_description
    })


@requirements_blueprint.route('/api/update_comments', methods=['PUT', 'POST'])
@token_required
@validate_access
def update_comments(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    comment_id = data.get('comment_id')
    comments = data.get('comments')
    status = data.get('status')
    user_id = data.get('user_id')

    sts = "Success"
    sts_description = "Comment updated successfully"

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

    if not comment_id:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Comment Id is required'
        })

    if not comments.strip():
        return jsonify({
            'status': 'Failed',
            'status_description': 'Comments cannot be empty'
        })

    if not status.strip():
        return jsonify({
            'status': 'Failed',
            'status_description': 'Status is required'
        })

    if not validate_status(corporate_account, project_id, 'COMMENTS', status):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Invalid comment status'
        })

    if not validate_user_id(corporate_account, user_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'User Id is not valid'
        })

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()
        mySql_update_query = f"""UPDATE REQUIREMENTS_COMMENTS SET COMMENTS = %s , STATUS = %s , UPDATED_DATE = NOW(), USER_ID = %s 
           WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID =  %s  AND COMMENT_ID = %s """

        record = (comments, status, user_id, corporate_account, project_id , comment_id)
        cursor.execute(mySql_update_query, record)
        connection.commit()
        rows_impacted = cursor.rowcount
        if rows_impacted == 0:
            sts = "Failed"
            sts_description = "No matching comment found to update"

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




@requirements_blueprint.route('/api/delete_comments', methods=['PUT','POST'])
@token_required
@validate_access
def delete_comments(current_user):


    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    comment_ids = data.get('comment_ids',[])

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
    if not comment_ids or not isinstance(comment_ids, list):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Comment Id is required'
        })

    sts = "Success"
    sts_description = "Comments successfully deleted"
    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()


        placeholders = ','.join(['%s'] * len(comment_ids))


        mySql_insert_query = f"""DELETE FROM REQUIREMENTS_COMMENTS
        WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND COMMENT_ID IN ({placeholders})"""

        record = (corporate_account, project_id)  + tuple(comment_ids)

        cursor.execute(mySql_insert_query, record)
        connection.commit()
        rows_impacted = cursor.rowcount
        if rows_impacted == 0:
            sts = "Failed"
            sts_description = "No matching comments found to delete"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to delete the comments: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'status': sts,
        'status_description': sts_description
    })








@requirements_blueprint.route('/api/add_key_attributes_list_requirements', methods=['POST'])
@token_required
@validate_access
def add_key_attributes_list_requirements(current_user):

    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    req_ids = data.get('req_ids', [])
    requirement_type = data.get('requirement_type', 'REQUIREMENT')
    level_id = data.get('level_id')
    key_attribute_list_ids = data.get('key_attribute_list_ids', [])
    include_exclude = data.get('include_exclude')

    logging.info(f"Input data: {data}")

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

    for req_id in req_ids:
        if not validate_req_id(corporate_account, project_id, req_id):
            return jsonify({
                'status': 'Failed',
                'status_description': f'Requirement Id {req_id} is not valid'
            })

    if level_id and not validate_level_id(corporate_account, project_id, level_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Level Id is not valid'
        })
    if level_id and req_ids and len(req_ids) > 0:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Both level_id and req_id cannot be provided'
        })

    if not level_id and len(req_ids) == 0:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Either level_id or req_id must be provided'
        })

    if  len(key_attribute_list_ids) == 0:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Key attribute list ID is required'
        })


    for key_attribute_list_id in key_attribute_list_ids:
        if not validate_key_attribute_list_id(corporate_account, project_id, key_attribute_list_id):
            return jsonify({
                'status': 'Failed',
                'status_description': f'Key attribute list ID {key_attribute_list_id} is not valid'
            })

    if include_exclude != 'INCLUDE' and include_exclude != 'EXCLUDE' :
        return jsonify({
            'status': 'Failed',
            'status_description': 'Valid values for include_exclude are INCLUDE or EXCLUDE'
        })

    sts = "Success"
    sts_description = "key attribute list mapping to requirement added successfully"
    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        mySql_insert_query = f"""INSERT INTO KEY_ATTRIBUTES_LIST_REQUIREMENTS(CORPORATE_ACCOUNT, PROJECT_ID, REQ_ID, LEVEL_ID, KEY_ATTRIBUTE_LIST_ID, 
        INCLUDE_EXCLUDE, CREATED_DATE, UPDATED_DATE) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) """

        if not level_id:
            level_id = 0
            for req_id in req_ids:
                for key_attribute_list_id in key_attribute_list_ids:
                    record = (corporate_account, project_id, req_id, level_id, key_attribute_list_id, include_exclude, datetime.now(), datetime.now())
                    cursor.execute(mySql_insert_query, record)
        else:
            req_id = 0
            for key_attribute_list_id in key_attribute_list_ids:
                record = (corporate_account, project_id, req_id, level_id, key_attribute_list_id, include_exclude, datetime.now(), datetime.now())
                cursor.execute(mySql_insert_query, record)

        rows_impacted = cursor.rowcount


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




@requirements_blueprint.route('/api/delete_key_attributes_list_requirements', methods=['PUT','POST'])
@token_required
@validate_access
def delete_key_attributes_list_requirements(current_user):


    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    req_id = data.get('req_id')
    level_id = data.get('level_id')
    key_attribute_list_ids = data.get('key_attribute_list_ids',[])



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

    if req_id and not validate_req_id(corporate_account, project_id, req_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Requirement Id is not valid'
        })
    if level_id and not validate_level_id(corporate_account, project_id, level_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Level Id is not valid'
        })
    if level_id and req_id:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Both level_id and req_id cannot be provided'
        })

    if not level_id and not req_id :
        return jsonify({
            'status': 'Failed',
            'status_description': 'Either level_id or req_id must be provided'
        })

    if not key_attribute_list_ids or not isinstance(key_attribute_list_ids, list):
        return jsonify({
            'status': 'Failed',
            'status_description': 'key_attribute_list_id must be provided as an array'
        })


    sts = "Success"
    sts_description = "Key attributes list mapping to requirements is successfully deleted"
    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        if not level_id:
            level_id = 0
        if not req_id:
            req_id = 0
        placeholders = ''
        placeholders = ','.join(['%s'] * len(key_attribute_list_ids))


        mySql_insert_query = f"""DELETE FROM KEY_ATTRIBUTES_LIST_REQUIREMENTS 
        WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND REQ_ID = %s AND LEVEL_ID = %s AND KEY_ATTRIBUTE_LIST_ID IN ({placeholders})"""

        record = (corporate_account, project_id, req_id, level_id)  + tuple(key_attribute_list_ids)

        cursor.execute(mySql_insert_query, record)
        connection.commit()
        rows_impacted = cursor.rowcount
        if rows_impacted == 0:
            sts = "Failed"
            sts_description = "No matching attribute list Id found to delete"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to delete the attribute list Id: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'status': sts,
        'status_description': sts_description
    })

@requirements_blueprint.route('/api/get_key_attributes_list_requirements', methods=['GET','POST'])
@token_required
def get_key_attributes_list_requirements(current_user):
    data = request.json
    corporate_account = data.get('corporate_account')
    project_id = data.get('project_id')
    level_id = data.get('level_id')
    req_id = data.get('req_id')

    logging.info(f"data : {data}")





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
    if req_id and not validate_req_id(corporate_account, project_id, req_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Requirement Id is not valid'
        })
    if level_id and not validate_level_id(corporate_account, project_id, level_id):
        return jsonify({
            'status': 'Failed',
            'status_description': 'Level Id is not valid'
        })

    if level_id and req_id:
        return jsonify({
            'status': 'Failed',
            'status_description': 'Both level_id and req_id cannot be provided'
        })

    if not level_id:
        level_id = 0
    if not req_id:
        req_id = 0


    sts = "Success"
    sts_description = "Key functional attributes retrieved successfully"
    key_functional_attribute_details = {}
    key_functional_attributes_list = []

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()


        # Base query without ORDER BY
        mySql_select_query = f"""SELECT INCLUDE_EXCLUDE, ATTRIBUTE_CATEGORY, ATTRIBUTE_NAME,
        ATTRIBUTE_DESCRIPTION, B.CREATED_DATE, B.UPDATED_DATE, A.KEY_ATTRIBUTE_LIST_ID
        FROM KEY_ATTRIBUTES_LIST_REQUIREMENTS A, KEY_ATTRIBUTES_LIST B
        WHERE A.KEY_ATTRIBUTE_LIST_ID = B.KEY_ATTRIBUTE_LIST_ID
               AND A.CORPORATE_ACCOUNT = B.CORPORATE_ACCOUNT 
       AND A.PROJECT_ID = B.PROJECT_ID 
        AND A.CORPORATE_ACCOUNT = %s
        AND A.PROJECT_ID = %s
        AND A.REQ_ID = %s AND A.LEVEL_ID = %s  
        ORDER BY ATTRIBUTE_CATEGORY DESC """



        # Build the query parameters list
        params = [corporate_account, project_id, req_id, level_id]

        cursor.execute(mySql_select_query, tuple(params))

        logging.info(f" executed SQL is: {cursor._executed}")

        for result in cursor.fetchall():
            key_functional_attribute_details = {
                'include_exclude': result[0],
                'attribute_category': result[1],
                'attribute_name': result[2],
                'attribute_description': result[3],
                'created_date': result[4],
                'updated_date': result[5],
                'key_attribute_list_id': result[6]
            }
            key_functional_attributes_list.append(key_functional_attribute_details)

        logging.info(f" array lenght is : {len(key_functional_attributes_list) }")

        if len(key_functional_attributes_list) == 0:
            sts = "Failed"
            sts_description = "No matching entries found"

    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the requirement details: {error}"
        logging.info(error)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({
        'key_functional_attributes_list': key_functional_attributes_list,
        'status': sts,
        'status_description': sts_description
    })








if __name__ == '__main__':
    app.run(debug=True)


