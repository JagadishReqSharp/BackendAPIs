import mysql.connector
from datetime import datetime
from flask import jsonify
import config
import logging
import re

logging.basicConfig(filename='debugging.log', level=logging.DEBUG)


def generate_next_sequence(corporate_account, project_id, sequence_key):
    sts = "Success"
    sts_description = "Next sequence number generated successfully"
    rows_impacted = 0
    next_sequence_no = 1000  # Default starting sequence number
    connection2 = None

    try:
        connection2 = mysql.connector.connect(host=config.host,
                                              database=config.database,
                                              user=config.user,
                                              password=config.password)
        cursor2 = connection2.cursor()

        mySql_select_query = """SELECT NEXT_SEQUENCE_NO FROM UNIQUE_SEQUENCE_GENERATION WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND SEQUENCE_KEY = %s FOR UPDATE"""
        record = (corporate_account, project_id, sequence_key)
        cursor2.execute(mySql_select_query, record)
        result = cursor2.fetchone()

        if result:
            next_sequence_no = result[0] + 1
            mySql_update_query = """UPDATE UNIQUE_SEQUENCE_GENERATION SET NEXT_SEQUENCE_NO = NEXT_SEQUENCE_NO + 1, UPDATED_DATE = %s WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND SEQUENCE_KEY = %s"""
            record = (datetime.now(), corporate_account, project_id, sequence_key)
            cursor2.execute(mySql_update_query, record)
            connection2.commit()
            rows_impacted = cursor2.rowcount
            if rows_impacted == 0:
                sts = "Failed"
                sts_description = "Unable to update the next sequence number"

        else:
            mySql_insert_query = """INSERT INTO UNIQUE_SEQUENCE_GENERATION(CORPORATE_ACCOUNT, PROJECT_ID, SEQUENCE_KEY, NEXT_SEQUENCE_NO, CREATED_DATE, UPDATED_DATE)
                                           VALUES (%s, %s, %s, %s, %s, %s) """
            record = (corporate_account, project_id, sequence_key, next_sequence_no, datetime.now(), datetime.now())

            cursor2.execute(mySql_insert_query, record)
            connection2.commit()
            rows_impacted = cursor2.rowcount


    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to generate the new sequence number: {error}"
        logging.info(error)


    finally:
        if connection2 and connection2.is_connected():
            cursor2.close()
            connection2.close()

    return next_sequence_no, sts, sts_description


def get_link_details(corporate_account, project_id, record_type, record_id):
    sts = "Success"
    sts_description = "Link details returned successfully"
    id_with_prefix = None
    id_description = None
    id_status = None
    connection2 = None

    logging.info(f"Inside get_link_details")
    logging.info(f"corporate_account: {corporate_account}")
    logging.info(f"Project_id: {project_id}")
    logging.info(f"record_type: {record_type}")
    logging.info(f"record_id: {record_id}")

    try:
        connection2 = mysql.connector.connect(host=config.host,
                                              database=config.database,
                                              user=config.user,
                                              password=config.password)
        cursor2 = connection2.cursor()

        if record_type == "REQUIREMENT" or record_type == "INTEGRATION_REQUIREMENT":
            mySql_select_query = """SELECT REQ_ID_WITH_PREFIX, REQ_DESCRIPTION, STATUS FROM REQUIREMENTS
             WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND REQ_ID = %s
             UNION
                SELECT INTEGRATION_ID_WITH_PREFIX, INTEGRATION_DESCRIPTION, STATUS FROM INTEGRATION_REQUIREMENTS
                WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND INTEGRATION_ID = %s"""

            record = (corporate_account, project_id, record_id, corporate_account, project_id, record_id)
        elif record_type == "USECASE":
            mySql_select_query = """SELECT USECASE_ID_WITH_PREFIX, USECASE_DESCRIPTION, STATUS FROM REQUIREMENTS_USECASES
                WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND USECASE_ID = %s"""
            record = (corporate_account, project_id, record_id)
        elif record_type in ("RISK", "ISSUE", "ACTION", "DECISION", "QUESTION", "TASK"):
            mySql_select_query = """SELECT RAID_ID_WITH_PREFIX, RAID_DESCRIPTION, STATUS FROM RAID_LOG
                WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND RAID_TYPE = %s AND RAID_ID = %s"""
            record = (corporate_account, project_id, record_type, record_id)
        else:
            sts = False
            sts_description = "Invalid record type"
            return id_with_prefix, id_description, id_status, sts, sts_description

        cursor2.execute(mySql_select_query, record)
        logging.info(f" executed SQL is: {cursor2._executed}")
        result = cursor2.fetchone()

        if result:
            id_with_prefix = result[0]
            id_description = result[1]
            id_status = result[2]
        else:
            sts = False
            sts_description = "No links found for the given record type and ID"



    except mysql.connector.Error as error:
        sts = "Failed"
        sts_description = f"Failed to retrieve the link details: {error}"
        logging.info(error)


    finally:
        if connection2 and connection2.is_connected():
            cursor2.close()
            connection2.close()

    return id_with_prefix, id_description, id_status, sts, sts_description


def validate_corporate_account(corporate_account):
    connection2 = None

    try:
        connection2 = mysql.connector.connect(host=config.host,
                                              database=config.database,
                                              user=config.user,
                                              password=config.password)
        cursor2 = connection2.cursor()

        mySql_select_query = "SELECT STATUS FROM CORPORATE_ACCOUNTS WHERE CORPORATE_ACCOUNT = %s"

        record = (corporate_account,)
        cursor2.execute(mySql_select_query, record)
        result = cursor2.fetchone()

        if result:

            if result[0] == "Active":
                sts = True
            else:
                sts = False

        else:
            sts = False

    except mysql.connector.Error as error:
        sts = False
        logging.info(error)

    finally:
        if connection2 and connection2.is_connected():
            cursor2.close()
            connection2.close()

    return sts


def validate_project_id(corporate_account, project_id):
    connection2 = None

    try:
        connection2 = mysql.connector.connect(host=config.host,
                                              database=config.database,
                                              user=config.user,
                                              password=config.password)
        cursor2 = connection2.cursor()

        mySql_select_query = "SELECT STATUS FROM CORPORATE_ACCOUNT_PROJECTS WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s"

        record = (corporate_account, project_id)
        cursor2.execute(mySql_select_query, record)
        result = cursor2.fetchone()

        if result:

            if result[0] == "Active":
                sts = True
            else:
                sts = False

        else:
            sts = False

    except mysql.connector.Error as error:
        sts = False
        logging.info(error)

    finally:
        if connection2 and connection2.is_connected():
            cursor2.close()
            connection2.close()

    return sts


def validate_level_id(corporate_account, project_id, level_id):
    connection2 = None

    try:
        connection2 = mysql.connector.connect(host=config.host,
                                              database=config.database,
                                              user=config.user,
                                              password=config.password)
        cursor2 = connection2.cursor()

        logging.info(f"corporate_account: {corporate_account}")
        logging.info(f"Project_id: {project_id}")
        logging.info(f"level_id: {level_id}")

        mySql_select_query = "SELECT STATUS FROM FUNCTIONAL_LEVELS WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND LEVEL_ID = %s"

        record = (corporate_account, project_id, level_id)
        cursor2.execute(mySql_select_query, record)
        result = cursor2.fetchone()

        if result:

            if result[0] == "Active":
                sts = True
            else:
                sts = False

        else:
            sts = False

    except mysql.connector.Error as error:
        sts = False
        logging.info(error)

    finally:
        if connection2 and connection2.is_connected():
            cursor2.close()
            connection2.close()

    return sts


def validate_req_id(corporate_account, project_id, req_id):
    connection2 = None

    try:
        connection2 = mysql.connector.connect(host=config.host,
                                              database=config.database,
                                              user=config.user,
                                              password=config.password)
        cursor2 = connection2.cursor()

        mySql_select_query = "SELECT STATUS FROM REQUIREMENTS WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND REQ_ID = %s"

        record = (corporate_account, project_id, req_id)
        cursor2.execute(mySql_select_query, record)
        result = cursor2.fetchone()

        if result:

            # if result[0] != "Cancelled":
            sts = True
        # else:
        #     sts = False

        else:

            mySql_select_query = "SELECT STATUS FROM INTEGRATION_REQUIREMENTS WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND INTEGRATION_ID = %s"

            record = (corporate_account, project_id, req_id)
            cursor2.execute(mySql_select_query, record)
            result = cursor2.fetchone()

            if result:

                # if result[0] != "Cancelled":
                sts = True
            # else:
            #     sts = False

            else:

                sts = False


    except mysql.connector.Error as error:
        sts = False
        logging.info(error)

    finally:
        if connection2 and connection2.is_connected():
            cursor2.close()
            connection2.close()

    return sts


def validate_status(corporate_account, project_id, entity, status):
    connection2 = None

    try:
        connection2 = mysql.connector.connect(host=config.host,
                                              database=config.database,
                                              user=config.user,
                                              password=config.password)
        cursor2 = connection2.cursor()

        mySql_select_query = "SELECT STATUS FROM ACCOUNT_STATUSES WHERE ENTITY = %s AND STATUS = %s AND CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s"

        record = (entity, status, corporate_account, project_id)

        cursor2.execute(mySql_select_query, record)
        result = cursor2.fetchone()

        if result:

            if result[0] == status:
                sts = True
            else:
                sts = False

        else:
            sts = False

    except mysql.connector.Error as error:
        sts = False
        logging.info(error)


    finally:
        if connection2 and connection2.is_connected():
            cursor2.close()
            connection2.close()
    return sts


def validate_user_id(corporate_account, user_id):
    connection2 = None

    try:
        connection2 = mysql.connector.connect(host=config.host,
                                              database=config.database,
                                              user=config.user,
                                              password=config.password)
        cursor2 = connection2.cursor()

        mySql_select_query = "SELECT STATUS FROM USER_ACCOUNTS WHERE CORPORATE_ACCOUNT = %s AND USER_ID = %s"

        record = (corporate_account, user_id)

        cursor2.execute(mySql_select_query, record)
        result = cursor2.fetchone()

        if result:

            if result[0] == 'Active':
                sts = True
            else:
                sts = False

        else:
            sts = False

    except mysql.connector.Error as error:
        sts = False
        logging.info(error)


    finally:
        if connection2 and connection2.is_connected():
            cursor2.close()
            connection2.close()
    return sts


def validate_usecase_id(corporate_account, project_id, usecase_id):
    connection2 = None

    try:
        connection2 = mysql.connector.connect(host=config.host,
                                              database=config.database,
                                              user=config.user,
                                              password=config.password)
        cursor2 = connection2.cursor()

        mySql_select_query = "SELECT STATUS FROM REQUIREMENTS_USECASES WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND USECASE_ID = %s"

        record = (corporate_account, project_id, usecase_id)
        cursor2.execute(mySql_select_query, record)
        result = cursor2.fetchone()

        if result:

            #   if result[0] != "Cancelled":
            sts = True
        #   else:
        #       sts = False

        else:
            sts = False

    except mysql.connector.Error as error:
        sts = False
        logging.info(error)

    finally:
        if connection2 and connection2.is_connected():
            cursor2.close()
            connection2.close()

    return sts


def validate_raid_log_entry(corporate_account, project_id, raid_type, raid_id):
    connection2 = None

    try:
        connection2 = mysql.connector.connect(host=config.host,
                                              database=config.database,
                                              user=config.user,
                                              password=config.password)
        cursor2 = connection2.cursor()

        mySql_select_query = "SELECT STATUS FROM RAID_LOG WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND RAID_TYPE = %s AND RAID_ID = %s"

        record = (corporate_account, project_id, raid_type, raid_id)
        cursor2.execute(mySql_select_query, record)
        logging.info(f" executed SQL is: {cursor2._executed}")

        result = cursor2.fetchone()

        if result:

            #   if result[0] != "Cancelled":
            sts = True
        #   else:
        #       sts = False

        else:
            sts = False

    except mysql.connector.Error as error:
        sts = False
        logging.info(error)

    finally:
        if connection2 and connection2.is_connected():
            cursor2.close()
            connection2.close()

    return sts


def validate_testcase_id(corporate_account, project_id, testcase_id):
    connection2 = None

    try:
        connection2 = mysql.connector.connect(host=config.host,
                                              database=config.database,
                                              user=config.user,
                                              password=config.password)
        cursor2 = connection2.cursor()

        mySql_select_query = "SELECT STATUS FROM REQUIREMENTS_TESTCASES WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND TESTCASE_ID = %s"

        record = (corporate_account, project_id, testcase_id)
        cursor2.execute(mySql_select_query, record)
        result = cursor2.fetchone()

        if result:

            #   if result[0] != "Cancelled":
            sts = True
        #   else:
        #       sts = False

        else:
            sts = False

    except mysql.connector.Error as error:
        sts = False
        logging.info(error)

    finally:
        if connection2 and connection2.is_connected():
            cursor2.close()
            connection2.close()

    return sts


def validate_key_attribute_list_id(corporate_account, project_id, key_attribute_list_id):
    connection2 = None

    try:
        connection2 = mysql.connector.connect(host=config.host,
                                              database=config.database,
                                              user=config.user,
                                              password=config.password)
        cursor2 = connection2.cursor()

        mySql_select_query = "SELECT * FROM KEY_ATTRIBUTES_LIST WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND KEY_ATTRIBUTE_LIST_ID = %s"

        record = (corporate_account, project_id, key_attribute_list_id)
        cursor2.execute(mySql_select_query, record)
        result = cursor2.fetchone()

        if result:

            #   if result[0] != "Cancelled":
            sts = True
        #   else:
        #       sts = False

        else:
            sts = False

    except mysql.connector.Error as error:
        sts = False
        logging.info(error)

    finally:
        if connection2 and connection2.is_connected():
            cursor2.close()
            connection2.close()

    return sts


def validate_integration_system_id(corporate_account, project_id, system_id):
    connection2 = None

    try:
        connection2 = mysql.connector.connect(host=config.host,
                                              database=config.database,
                                              user=config.user,
                                              password=config.password)
        cursor2 = connection2.cursor()

        mySql_select_query = "SELECT * FROM INTEGRATION_SYSTEMS WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND SYSTEM_ID = %s"

        record = (corporate_account, project_id, system_id)
        cursor2.execute(mySql_select_query, record)
        result = cursor2.fetchone()

        if result:

            #   if result[0] != "Cancelled":
            sts = True
        #   else:
        #       sts = False

        else:
            sts = False

    except mysql.connector.Error as error:
        sts = False
        logging.info(error)

    finally:
        if connection2 and connection2.is_connected():
            cursor2.close()
            connection2.close()

    return sts


def validate_integration_id(corporate_account, project_id, integration_id):
    connection2 = None

    try:
        connection2 = mysql.connector.connect(host=config.host,
                                              database=config.database,
                                              user=config.user,
                                              password=config.password)
        cursor2 = connection2.cursor()

        mySql_select_query = "SELECT * FROM INTEGRATION_REQUIREMENTS WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND INTEGRATION_ID = %s"

        record = (corporate_account, project_id, integration_id)
        cursor2.execute(mySql_select_query, record)
        result = cursor2.fetchone()

        if result:

            #   if result[0] != "Cancelled":
            sts = True
        #   else:
        #       sts = False

        else:
            sts = False

    except mysql.connector.Error as error:
        sts = False
        logging.info(error)

    finally:
        if connection2 and connection2.is_connected():
            cursor2.close()
            connection2.close()

    return sts


def is_valid_field_name(field_name):
    # Regular expression to allow only alphanumeric characters and underscores
    pattern = r'^[a-zA-Z0-9_]+$'
    return bool(re.match(pattern, field_name))


def validate_integration_field(corporate_account, project_id, field_name, integration_id, system_id, system_type):
    connection2 = None

    try:
        connection2 = mysql.connector.connect(host=config.host,
                                              database=config.database,
                                              user=config.user,
                                              password=config.password)
        cursor2 = connection2.cursor()

        sts = True
        sts_description = "integration field name is valid"

        # if integration_id ==0 and level_id == 0 and req_id == 0:
        #     mySql_select_query = ("""SELECT * FROM INTEGRATION_REQUIREMENTS_FIELDS
        #                           WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND FIELD_NAME = %s AND INTEGRATION_ID = 0 AND LEVEL_ID = 0 AND REQ_ID = 0""")
        #     record = (corporate_account,project_id, field_name)
        #     cursor2.execute(mySql_select_query, record)
        #     result = cursor2.fetchone()
        #     rows_impacted = cursor2.rowcount
        #
        #     if result and rows_impacted > 0:
        #         sts = False
        #         sts_description = "field name is already mapped at the project level"

        # if sts and level_id == 0 and req_id == 0:
        #     mySql_select_query = ("""SELECT * FROM INTEGRATION_REQUIREMENTS_FIELDS
        #                       WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND FIELD_NAME = %s AND INTEGRATION_ID = %s AND LEVEL_ID = 0 AND REQ_ID = 0""")
        #     record = (corporate_account,project_id, field_name, integration_id)
        #     cursor2.execute(mySql_select_query, record)
        #     result = cursor2.fetchone()
        #     rows_impacted = cursor2.rowcount
        #
        #     if result and rows_impacted > 0:
        #         sts = False
        #         sts_description = "field name is already mapped at the integration level"

        # if sts and req_id == 0:
        #     mySql_select_query = ("""SELECT * FROM INTEGRATION_REQUIREMENTS_FIELDS
        #                       WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND FIELD_NAME = %s AND INTEGRATION_ID = %s AND LEVEL_ID = %s AND REQ_ID = 0""")
        #     record = (corporate_account,project_id, field_name, integration_id, level_id)
        #     cursor2.execute(mySql_select_query, record)
        #     result = cursor2.fetchone()
        #     rows_impacted = cursor2.rowcount
        #
        #     if result and rows_impacted > 0:
        #         sts = False
        #         sts_description = "field name is already mapped at the functional level"

        # if sts:
        #     mySql_select_query = ("""SELECT * FROM INTEGRATION_REQUIREMENTS_FIELDS
        #                       WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND FIELD_NAME = %s AND INTEGRATION_ID = %s AND LEVEL_ID = %s AND REQ_ID = %s""")
        #     record = (corporate_account,project_id, field_name, integration_id, level_id, req_id)
        #     cursor2.execute(mySql_select_query, record)
        #     result = cursor2.fetchone()
        #     rows_impacted = cursor2.rowcount

        mySql_select_query = ("""SELECT * FROM INTEGRATION_REQUIREMENTS_FIELDS 
                          WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND FIELD_NAME = %s AND INTEGRATION_ID = %s AND SYSTEM_ID = %s AND SYSTEM_TYPE = %s""")
        record = (corporate_account, project_id, field_name, integration_id, system_id, system_type)
        cursor2.execute(mySql_select_query, record)
        result = cursor2.fetchone()
        rows_impacted = cursor2.rowcount

        if result and rows_impacted > 0:
            sts = False
            if integration_id == 0:
                sts_description = "field name is already mapped at the project level"
            else:
                sts_description = "field name is already mapped to this integration"


    except mysql.connector.Error as error:
        sts = False
        logging.info(error)

    finally:
        if connection2 and connection2.is_connected():
            cursor2.close()
            connection2.close()

    return sts, sts_description


def validate_product_id(corporate_account, project_id, product_id):
    connection2 = None

    try:
        connection2 = mysql.connector.connect(host=config.host,
                                              database=config.database,
                                              user=config.user,
                                              password=config.password)
        cursor2 = connection2.cursor()

        mySql_select_query = "SELECT * FROM PRODUCTS_BY_PROJECT WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND PRODUCT_ID = %s"

        record = (corporate_account, project_id, product_id)
        cursor2.execute(mySql_select_query, record)
        result = cursor2.fetchone()

        if result:

            #   if result[0] != "Cancelled":
            sts = True
        #   else:
        #       sts = False

        else:
            sts = False

    except mysql.connector.Error as error:
        sts = False
        logging.info(error)

    finally:
        if connection2 and connection2.is_connected():
            cursor2.close()
            connection2.close()

    return sts


def validate_req_classification(req_classification):
    connection2 = None

    try:
        connection2 = mysql.connector.connect(host=config.host,
                                              database=config.database,
                                              user=config.user,
                                              password=config.password)
        cursor2 = connection2.cursor()
        mySql_select_query = "SELECT * FROM REQUIREMENT_CLASSIFICATION WHERE REQ_CLASSIFICATION = %s"

        record = (req_classification,)
        cursor2.execute(mySql_select_query, record)
        result = cursor2.fetchone()

        if result:

            #   if result[0] != "Cancelled":
            sts = True
        #   else:
        #       sts = False

        else:
            sts = False

    except mysql.connector.Error as error:
        sts = False
        logging.info(error)

    finally:
        if connection2 and connection2.is_connected():
            cursor2.close()
            connection2.close()

    return sts


def is_user_authorized_to_approve(
        corporate_account,
        project_id,
        approval_user_id,
        req_id,
        level_id):
    # if req_id is not None and level_id is not None:
    #     # raise ValueError("Only one of req_id or level_id should be provided, not both")
    #     return False

    # Connect to the database
    # Note: In a production environment, use connection pooling
    logging.info("Authorization validation - 0")
    conn = get_database_connection()
    logging.info("Authorization validation - 1")
    try:
        with conn.cursor() as cursor:
            # Check Project Level authorization first (most general)
            if is_authorized_at_project_level(cursor, corporate_account, project_id, approval_user_id):
                logging.info("Authorization validation - 2")
                return True

            # If both req_id and level_id are None, we've already checked project level auth and it failed
            if not req_id and not level_id:
                logging.info("Authorization validation - 3")
                return False

            if req_id:
                # Requirement level check
                logging.info(f"Authorization validation - 4   Req_id {req_id} Level_Id {level_id}")
                return is_authorized_for_requirement(cursor, corporate_account, project_id, req_id, approval_user_id)
            else:
                logging.info("Authorization validation - 5")
                # Functional level check
                return is_authorized_for_functional_level(cursor, corporate_account, project_id, level_id,
                                                          approval_user_id)
    finally:
        conn.close()


def get_database_connection():
    return mysql.connector.connect(
        host=config.host,
        user=config.user,
        password=config.password,
        database=config.database
        # charset='utf8mb4',
        # cursorclass=pymysql.cursors.DictCursor
    )


def get_user_api_access_level(user_id, corporate_account, project_id, api_name):
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

            record = (api_name,)

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

    return access_level, access_status, sts, sts_description


def is_authorized_at_project_level(cursor, corporate_account: str, project_id: str, approval_user_id: str) -> bool:
    """Checks if user is authorized at the project level"""
    logging.info("Project level access validation")

    query = """
        SELECT 1 FROM REQUIREMENTS_APPROVERS
        WHERE CORPORATE_ACCOUNT = %s
        AND PROJECT_ID = %s
        AND APPROVAL_USER_ID = %s
        AND REQ_ID = 0
        AND LEVEL_ID = 0
        LIMIT 1
        """
    cursor.execute(query, (corporate_account, project_id, approval_user_id))
    return cursor.fetchone() is not None


def is_authorized_for_requirement(cursor, corporate_account: str, project_id: str,
                                  req_id: int, approval_user_id: str) -> bool:
    logging.info("Req level access validation")

    """
    Checks if user is authorized for a specific requirement.
    This includes:
    1. Direct authorization for the requirement
    2. Authorization for the functional level that contains this requirement
    """
    # Check direct authorization for the requirement
    req_query = """
        SELECT 1 FROM REQUIREMENTS_APPROVERS
        WHERE CORPORATE_ACCOUNT = %s
        AND PROJECT_ID = %s
        AND APPROVAL_USER_ID = %s
        AND REQ_ID = %s
        AND LEVEL_ID = 0
        LIMIT 1
        """
    cursor.execute(req_query, (corporate_account, project_id, approval_user_id, req_id))
    if cursor.fetchone() is not None:
        return True
    logging.info("Req level access validation - 1")

    # Get the functional level for this requirement
    level_query = """
        SELECT LEVEL_ID FROM REQUIREMENTS
        WHERE CORPORATE_ACCOUNT = %s
        AND PROJECT_ID = %s
        AND REQ_ID = %s
        """
    cursor.execute(level_query, (corporate_account, project_id, req_id))
    result = cursor.fetchone()
    logging.info(f" executed SQL is: {cursor._executed}")

    if not result:
        level_query = """
            SELECT LEVEL_ID FROM INTEGRATION_REQUIREMENTS
            WHERE CORPORATE_ACCOUNT = %s
            AND PROJECT_ID = %s
            AND INTEGRATION_ID = %s
            """
        cursor.execute(level_query, (corporate_account, project_id, req_id))
        result = cursor.fetchone()
        logging.info(f" executed SQL is: {cursor._executed}")

        if not result:
            return False  # Requirement not found

    logging.info("Req level access validation - 2")

    level_id = result[0]
    logging.info("Req level access validation - 3")

    # Check if authorized for the functional level
    return is_authorized_for_functional_level(cursor, corporate_account, project_id, level_id, approval_user_id)


def is_authorized_for_functional_level(cursor, corporate_account: str, project_id: str,
                                       level_id: int, approval_user_id: str) -> bool:
    logging.info("Functional level access validation")

    """
    Checks if user is authorized for a functional level.
    This includes:
    1. Direct authorization for this level
    2. Authorization for any parent level in the hierarchy
    """
    # Build the hierarchy path from this level up to the root
    hierarchy_path = get_level_hierarchy_path(cursor, corporate_account, project_id, level_id)

    if not hierarchy_path:
        return False  # Level not found or invalid

    # Check if user is authorized for any level in the hierarchy path
    placeholders = ', '.join(['%s'] * len(hierarchy_path))
    level_auth_query = f"""
        SELECT 1 FROM REQUIREMENTS_APPROVERS
        WHERE CORPORATE_ACCOUNT = %s
        AND PROJECT_ID = %s
        AND APPROVAL_USER_ID = %s
        AND REQ_ID = 0
        AND LEVEL_ID IN ({placeholders})
        LIMIT 1
        """

    params = (corporate_account, project_id, approval_user_id) + tuple(hierarchy_path)
    cursor.execute(level_auth_query, params)

    return cursor.fetchone() is not None


def get_level_hierarchy_path(cursor, corporate_account, project_id, level_id):
    """
    Returns the hierarchical path from a level to its topmost parent.
    The returned list includes the starting level_id and all parent level_ids.
    """
    hierarchy_path = []
    current_level_id = level_id

    # Maximum depth to prevent infinite loops in case of circular references
    max_depth = 100
    depth = 0

    while current_level_id != 0 and depth < max_depth:
        hierarchy_path.append(current_level_id)

        # Get the parent level
        query = """
            SELECT PARENT_LEVEL_ID FROM FUNCTIONAL_LEVELS
            WHERE CORPORATE_ACCOUNT = %s
            AND PROJECT_ID = %s
            AND LEVEL_ID = %s
            """
        cursor.execute(query, (corporate_account, project_id, current_level_id))
        result = cursor.fetchone()

        if not result:
            break

        current_level_id = result[0]
        depth += 1

    return hierarchy_path


def validate_functional_domain(functional_domain):
    connection2 = None

    try:
        connection2 = mysql.connector.connect(host=config.host,
                                              database=config.database,
                                              user=config.user,
                                              password=config.password)
        cursor2 = connection2.cursor()
        mySql_select_query = "SELECT * FROM FUNCTIONAL_DOMAINS WHERE FUNCTIONAL_DOMAIN = %s"

        record = (functional_domain,)
        cursor2.execute(mySql_select_query, record)
        result = cursor2.fetchone()

        if result:
            sts = True
        else:
            sts = False


    except mysql.connector.Error as error:
        sts = False
        logging.info(error)

    finally:
        if connection2 and connection2.is_connected():
            cursor2.close()
            connection2.close()

    return sts


def validate_project_prefix(corporate_account, project_id, project_prefix):
    connection2 = None

    try:
        connection2 = mysql.connector.connect(host=config.host,
                                              database=config.database,
                                              user=config.user,
                                              password=config.password)
        cursor2 = connection2.cursor()
        mySql_select_query = "SELECT COUNT(*) FROM CORPORATE_ACCOUNT_PROJECTS WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID != %s AND PROJECT_PREFIX = %s"

        record = (corporate_account, project_id, project_prefix)
        cursor2.execute(mySql_select_query, record)
        result = cursor2.fetchone()

        if result and result[0] > 0:

            #   if result[0] != "Cancelled":
            sts = False
        #   else:
        #       sts = False

        else:
            sts = True

    except mysql.connector.Error as error:
        sts = False
        logging.info(error)

    finally:
        if connection2 and connection2.is_connected():
            cursor2.close()
            connection2.close()

    return sts


def get_project_prefix(corporate_account, project_id):
    connection2 = None

    try:
        connection2 = mysql.connector.connect(host=config.host,
                                              database=config.database,
                                              user=config.user,
                                              password=config.password)
        cursor2 = connection2.cursor()
        mySql_select_query = "SELECT PROJECT_PREFIX FROM CORPORATE_ACCOUNT_PROJECTS WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s"

        record = (corporate_account, project_id)
        cursor2.execute(mySql_select_query, record)
        result = cursor2.fetchone()

        if result:
            project_prefix = result[0]
        else:
            project_prefix = 'Error'

    except mysql.connector.Error as error:
        project_prefix = 'Error'
        logging.info(error)

    finally:
        if connection2 and connection2.is_connected():
            cursor2.close()
            connection2.close()

    return project_prefix


def validate_functional_level(corporate_account, project_id, level_id):
    connection2 = None

    try:
        connection2 = mysql.connector.connect(host=config.host,
                                              database=config.database,
                                              user=config.user,
                                              password=config.password)
        cursor2 = connection2.cursor()
        mySql_select_query = "SELECT * FROM FUNCTIONAL_LEVELS WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND LEVEL_ID = %s"

        record = (corporate_account, project_id, level_id)
        cursor2.execute(mySql_select_query, record)
        result = cursor2.fetchone()

        if result:

            #   if result[0] != "Cancelled":
            sts = True
        #   else:
        #       sts = False

        else:
            sts = False

    except mysql.connector.Error as error:
        sts = False
        logging.info(error)

    finally:
        if connection2 and connection2.is_connected():
            cursor2.close()
            connection2.close()

    return sts


def get_functional_level_dependency_details(corporate_account, project_id, level_id):
    dependency_details = {}
    dependency_list = []
    total_dependency_count = 0
    sts = 'Success'
    sts_description = 'Dependency details fetched successfully'

    if level_id == '0' or level_id == None or not level_id:
        total_dependency_count = -1

    sub_level_count = 0

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        mySql_select_query = """WITH RECURSIVE SUBLEVEL_TREE AS
                  ( SELECT LEVEL_ID, PARENT_LEVEL_ID, 1 AS LEVEL_DEPTH FROM FUNCTIONAL_LEVELS 
                  WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND LEVEL_ID = %s  
                    UNION ALL
                SELECT F.LEVEL_ID, F.PARENT_LEVEL_ID, ST.LEVEL_DEPTH + 1 FROM FUNCTIONAL_LEVELS F
                INNER JOIN SUBLEVEL_TREE ST ON F.PARENT_LEVEL_ID = ST.LEVEL_ID)
                SELECT COUNT(*)-1 FROM SUBLEVEL_TREE """

        record = (corporate_account, project_id, level_id)
        cursor.execute(mySql_select_query, record)
        result = cursor.fetchone()

        if result:
            sub_level_count = result[0]
            total_dependency_count += sub_level_count

        mySql_select_query = """  WITH DEPENDENCY_LIST AS (
                SELECT CORPORATE_ACCOUNT, PROJECT_ID, LEVEL_ID, 'REQUIREMENTS' DEPENDENCY_TYPE, STATUS DEPENDENCY_STATUS, COUNT(*) DEPENDENCY_COUNT FROM REQUIREMENTS 
                WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s
                GROUP BY CORPORATE_ACCOUNT, PROJECT_ID, LEVEL_ID, DEPENDENCY_TYPE, DEPENDENCY_STATUS
                UNION
                SELECT CORPORATE_ACCOUNT, PROJECT_ID, LEVEL_ID, 'INTEGRATION REQUIREMENTS' DEPENDENCY_TYPE, STATUS DEPENDENCY_STATUS, COUNT(*) DEPENDENCY_COUNT FROM INTEGRATION_REQUIREMENTS
                WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s
                GROUP BY CORPORATE_ACCOUNT, PROJECT_ID, LEVEL_ID, DEPENDENCY_TYPE, DEPENDENCY_STATUS
                        UNION
                SELECT CORPORATE_ACCOUNT, PROJECT_ID, LEVEL_ID, 'REQUIREMENT EXCEPTIONS' DEPENDENCY_TYPE, 'NA' DEPENDENCY_STATUS, COUNT(*) DEPENDENCY_COUNT FROM KEY_ATTRIBUTES_LIST_REQUIREMENTS
                WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s
                GROUP BY CORPORATE_ACCOUNT, PROJECT_ID, LEVEL_ID, DEPENDENCY_TYPE, DEPENDENCY_STATUS
                        UNION
                SELECT CORPORATE_ACCOUNT, PROJECT_ID, LEVEL_ID, 'COMMENTS' DEPENDENCY_TYPE, 'NA' DEPENDENCY_STATUS, COUNT(*) DEPENDENCY_COUNT FROM REQUIREMENTS_COMMENTS
                WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s
                GROUP BY CORPORATE_ACCOUNT, PROJECT_ID, LEVEL_ID, DEPENDENCY_TYPE, DEPENDENCY_STATUS)

                SELECT DEPENDENCY_TYPE, DEPENDENCY_STATUS, SUM(DEPENDENCY_COUNT) FROM DEPENDENCY_LIST A WHERE 
        		LEVEL_ID = %s OR LEVEL_ID IN
                (WITH RECURSIVE SUBLEVEL_TREE AS (
                SELECT LEVEL_ID, PARENT_LEVEL_ID, 1 AS LEVEL_DEPTH FROM FUNCTIONAL_LEVELS
                WHERE PARENT_LEVEL_ID = %s AND CORPORATE_ACCOUNT= A.CORPORATE_ACCOUNT AND PROJECT_ID = A.PROJECT_ID
                UNION ALL
                SELECT F.LEVEL_ID, F.PARENT_LEVEL_ID, ST.LEVEL_DEPTH + 1 FROM FUNCTIONAL_LEVELS F
                INNER JOIN SUBLEVEL_TREE ST ON F.PARENT_LEVEL_ID = ST.LEVEL_ID)
                SELECT LEVEL_ID FROM SUBLEVEL_TREE)
                GROUP BY DEPENDENCY_TYPE, DEPENDENCY_STATUS
                ORDER BY DEPENDENCY_TYPE, DEPENDENCY_STATUS"""

        record = (
            corporate_account, project_id, corporate_account, project_id, corporate_account, project_id,
            corporate_account,
            project_id, level_id, level_id)
        cursor.execute(mySql_select_query, record)

        logging.info(f"hello SQL is {cursor.statement}")

        for result in cursor.fetchall():
            total_dependency_count += result[2]
            dependency_details = {
                'Dependency_Type': result[0],
                'Dependency_Status': result[1],
                'Dependency_Count': result[2]
            }
            dependency_list.append(dependency_details)


    except mysql.connector.Error as error:
        sts = 'Failed'
        sts_description = f"Failed to fetch dependency details {error}"
        sub_level_count = 0
        total_dependency_count = 0
        logging.info(f"Error fetching dependency details: {error}")
        logging.info(error)
        return sts, sts_description, sub_level_count, total_dependency_count, dependency_list

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return sts, sts_description, sub_level_count, total_dependency_count, dependency_list


def get_functional_level_children(corporate_account, project_id, level_id):
    child_level_ids = [level_id] if level_id and level_id != '0' else []

    if level_id == '0' or level_id is None or not level_id:
        return []

    try:
        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        # Query to get all child level IDs
        mySql_select_query = """WITH RECURSIVE SUBLEVEL_TREE AS
          (SELECT LEVEL_ID, PARENT_LEVEL_ID, 1 AS LEVEL_DEPTH FROM FUNCTIONAL_LEVELS 
          WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND LEVEL_ID = %s AND STATUS = %s
            UNION ALL
          SELECT F.LEVEL_ID, F.PARENT_LEVEL_ID, ST.LEVEL_DEPTH + 1 FROM FUNCTIONAL_LEVELS F
          INNER JOIN SUBLEVEL_TREE ST ON F.PARENT_LEVEL_ID = ST.LEVEL_ID)
        SELECT LEVEL_ID FROM SUBLEVEL_TREE WHERE LEVEL_ID != %s"""

        record = (corporate_account, project_id, level_id, 'Active', level_id)
        cursor.execute(mySql_select_query, record)

        # Fetch all child level IDs and add them to the result array
        for result in cursor.fetchall():
            child_level_ids.append(result[0])

    except mysql.connector.Error as error:
        logging.info(error)
        return []

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return child_level_ids


def validate_functional_attribute_category(corporate_account, project_id, attribute_category):
    connection2 = None

    try:
        connection2 = mysql.connector.connect(host=config.host,
                                              database=config.database,
                                              user=config.user,
                                              password=config.password)
        cursor2 = connection2.cursor()
        mySql_select_query = "SELECT * FROM KEY_ATTRIBUTES_HEADER WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND ATTRIBUTE_CATEGORY = %s"

        record = (corporate_account, project_id, attribute_category)
        cursor2.execute(mySql_select_query, record)
        result = cursor2.fetchone()

        if result:

            #   if result[0] != "Cancelled":
            sts = True
        #   else:
        #       sts = False

        else:
            sts = False

    except mysql.connector.Error as error:
        sts = False
        logging.info(error)

    finally:
        if connection2 and connection2.is_connected():
            cursor2.close()
            connection2.close()

    return sts


def get_functional_level_details(corporate_account, project_id, level_id):
    connection2 = None

    try:
        connection2 = mysql.connector.connect(host=config.host,
                                              database=config.database,
                                              user=config.user,
                                              password=config.password)
        cursor2 = connection2.cursor()
        mySql_select_query = """
            SELECT LEVEL_ID, LEVEL_DESCRIPTION, PARENT_LEVEL_ID
            FROM FUNCTIONAL_LEVELS
            WHERE CORPORATE_ACCOUNT = %s
            AND PROJECT_ID = %s
            AND LEVEL_ID = %s
            AND STATUS = 'ACTIVE'
        """

        record = (corporate_account, project_id, level_id)
        cursor2.execute(mySql_select_query, record)
        result = cursor2.fetchone()

        if result:
            return {
                'level_id': result[0],
                'level_description': result[1],
                'parent_level_id': result[2]
            }

    except mysql.connector.Error as error:
        logging.info(error)

    finally:
        if connection2 and connection2.is_connected():
            cursor2.close()
            connection2.close()

    return None