# Import required modules
from flask import Flask, request, jsonify, send_file, Blueprint
import os
from werkzeug.utils import secure_filename
import uuid
import mysql.connector
import config
import logging


file_management_blueprint = Blueprint('attachments', __name__)


from datetime import datetime

# Assuming these are already defined in your existing API
from utils import token_required

# Configure upload directory
UPLOAD_FOLDER = config.ATTACHMENTS_FOLDER_PATH
ALLOWED_EXTENSIONS = {'pdf', 'doc', 'docx', 'xls', 'xlsx', 'txt', 'png', 'jpg', 'jpeg', 'gif'}

# Create directory if it doesn't exist
os.makedirs(UPLOAD_FOLDER, exist_ok=True)


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


# 1. Upload attachment(s) for a requirement
@file_management_blueprint.route('/api/upload_requirement_attachment', methods=['POST'])
@token_required
def upload_requirement_attachment(current_user):
    try:
        req_id = request.form.get('req_id')
        corporate_account = request.form.get('corporate_account')
        project_id = request.form.get('project_id')
        uploaded_by = request.form.get('user_id')

        logging.info("Level 1")

        # Validate required inputs
        if not req_id or not corporate_account or not project_id:
            return jsonify({
                'status': 'Error',
                'status_description': 'Missing required parameters'
            }), 400

        # Check if the post request has the file part
        if 'files[]' not in request.files:
            return jsonify({
                'status': 'Error',
                'status_description': 'No file uploaded'
            }), 400

        logging.info("Level 2")

        files = request.files.getlist('files[]')

        # If user did not select file
        if not files or files[0].filename == '':
            return jsonify({
                'status': 'Error',
                'status_description': 'No file selected'
            }), 400


        logging.info("Level 3")


        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        uploaded_files = []

        logging.info("Level 4")

        for file in files:
            if file and allowed_file(file.filename):
                # Create unique filename to prevent collisions
                logging.info("Level 5")

                original_filename = secure_filename(file.filename)
                file_extension = original_filename.rsplit('.', 1)[1].lower() if '.' in original_filename else ''
                unique_filename = f"{uuid.uuid4().hex}.{file_extension}" if file_extension else f"{uuid.uuid4().hex}"

                # Create path based on corporate account and project
                relative_path = f"{corporate_account}/{project_id}/{req_id}"
                full_dir = os.path.join(UPLOAD_FOLDER, relative_path)
                os.makedirs(full_dir, exist_ok=True)

                file_path = os.path.join(full_dir, unique_filename)

                # Save the file
                file.save(file_path)

                # Get file size
                file_size = os.path.getsize(file_path)

                # Store in database
                cursor.execute("""
                    INSERT INTO REQUIREMENT_ATTACHMENTS 
                    (CORPORATE_ACCOUNT, PROJECT_ID, REQ_ID, FILE_NAME, FILE_PATH, FILE_SIZE, FILE_TYPE, UPLOADED_BY)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    corporate_account, project_id, req_id,
                    original_filename,
                    os.path.join(relative_path, unique_filename),
                    file_size,
                    file_extension,
                    uploaded_by
                ))
                logging.info("Level 6")

                attachment_id = cursor.lastrowid

                uploaded_files.append({
                    'attachment_id': attachment_id,
                    'file_name': original_filename,
                    'file_size': file_size,
                    'file_type': file_extension
                })

        connection.commit()
        cursor.close()
        connection.close()

        return jsonify({
            'status': 'Success',
            'status_description': f'{len(uploaded_files)} file(s) uploaded successfully',
            'attachments': uploaded_files
        })

    except Exception as e:
        # Log the exception
        print(f"Error uploading attachment: {str(e)}")
        logging.error(f"Error uploading attachment: {str(e)}")
        return jsonify({
            'status': 'Error',
            'status_description': f'Failed to upload attachment: {str(e)}'
        }), 500


# 2. Get all attachments for a requirement
@file_management_blueprint.route('/api/get_requirement_attachments', methods=['POST'])
@token_required
def get_requirement_attachments(current_user):
    try:
        req_id = request.json.get('req_id')
        corporate_account = request.json.get('corporate_account')
        project_id = request.json.get('project_id')

        # Validate required inputs
        if not req_id or not corporate_account or not project_id:
            return jsonify({
                'status': 'Error',
                'status_description': 'Missing required parameters'
            }), 400

        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        cursor.execute("""
            SELECT 
                ATTACHMENT_ID, 
                FILE_NAME, 
                FILE_PATH, 
                FILE_SIZE, 
                FILE_TYPE, 
                UPLOADED_BY, 
                UPLOAD_DATE
            FROM REQUIREMENT_ATTACHMENTS
            WHERE CORPORATE_ACCOUNT = %s AND PROJECT_ID = %s AND REQ_ID = %s
            ORDER BY UPLOAD_DATE DESC
        """, (corporate_account, project_id, req_id,))

        attachments = []
        for row in cursor.fetchall():
            attachments.append({
                'attachment_id': row[0],
                'file_name': row[1],
                'file_path': row[2],
                'file_size': row[3],
                'file_type': row[4],
                'uploaded_by': row[5],
                'upload_date': row[6].strftime('%Y-%m-%d %H:%M:%S') if row[6] else None
            })

        cursor.close()
        connection.close()

        return jsonify({
            'status': 'Success',
            'attachments': attachments
        })

    except Exception as e:
        # Log the exception
        print(f"Error retrieving attachments: {str(e)}")
        logging.error(f"Error uploading attachment: {str(e)}")

        return jsonify({
            'status': 'Error',
            'status_description': f'Failed to retrieve attachments: {str(e)}'
        }), 500


# 3. Download attachment
@file_management_blueprint.route('/api/download_requirement_attachment/<attachment_id>', methods=['GET', 'POST'])
def download_requirement_attachment(attachment_id):
    try:
        # Get token from query params or form data
        token = request.args.get('token') or request.form.get('token')

        if not token:
            return jsonify({
                'status': 'Error',
                'message': 'Token is missing!'
            }), 401

        # Validate token (assuming validate_token function or similar logic)
        # For this example, we'll use a simple JWT check - replace with your token validation
        try:
            # Your token validation logic here
            # Example: jwt.decode(token, app.config['SECRET_KEY'], algorithms=["HS256"])
            pass
        except Exception as e:
            return jsonify({
                'status': 'Error',
                'message': 'Invalid token!'
            }), 401

        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        cursor.execute("""
            SELECT FILE_NAME, FILE_PATH
            FROM REQUIREMENT_ATTACHMENTS
            WHERE ATTACHMENT_ID = %s
        """, (attachment_id,))

        result = cursor.fetchone()
        cursor.close()
        connection.close()

        if not result:
            return jsonify({
                'status': 'Error',
                'status_description': 'Attachment not found'
            }), 404

        file_name, file_path = result
        full_path = os.path.join(UPLOAD_FOLDER, file_path)

        if not os.path.exists(full_path):
            return jsonify({
                'status': 'Error',
                'status_description': 'File not found on server'
            }), 404

        return send_file(
            full_path,
            as_attachment=True,
            download_name=file_name
        )

    except Exception as e:
        # Log the exception
        print(f"Error downloading attachment: {str(e)}")
        return jsonify({
            'status': 'Error',
            'status_description': f'Failed to download attachment: {str(e)}'
        }), 500



# 4. Delete attachment
@file_management_blueprint.route('/api/delete_requirement_attachment', methods=['POST'])
@token_required
def delete_requirement_attachment(current_user):
    try:
        attachment_id = request.json.get('attachment_id')
        corporate_account = request.json.get('corporate_account')
        project_id = request.json.get('project_id')

        # Validate required inputs
        if not attachment_id or not corporate_account or not project_id:
            return jsonify({
                'status': 'Error',
                'status_description': 'Missing required parameters'
            }), 400

        connection = mysql.connector.connect(host=config.host,
                                             database=config.database,
                                             user=config.user,
                                             password=config.password)
        cursor = connection.cursor()

        # Get the file path before deleting the record
        cursor.execute("""
            SELECT FILE_PATH
            FROM REQUIREMENT_ATTACHMENTS
            WHERE ATTACHMENT_ID = %s
        """, (attachment_id,))

        result = cursor.fetchone()

        if not result:
            cursor.close()
            connection.close()
            return jsonify({
                'status': 'Error',
                'status_description': 'Attachment not found'
            }), 404

        file_path = result[0]

        # Delete the record from database
        cursor.execute("""
            DELETE FROM REQUIREMENT_ATTACHMENTS
            WHERE ATTACHMENT_ID = %s
        """, (attachment_id,))

        req_id = cursor.lastrowid
        connection.commit()

        # Delete the file from filesystem
        full_path = os.path.join(UPLOAD_FOLDER, file_path)
        if os.path.exists(full_path):
            os.remove(full_path)

        cursor.close()
        connection.close()

        return jsonify({
            'status': 'Success',
            'status_description': 'Attachment deleted successfully',
            'req_id': req_id
        })

    except Exception as e:
        # Log the exception
        print(f"Error deleting attachment: {str(e)}")
        logging.error(f"Error uploading attachment: {str(e)}")

        return jsonify({
            'status': 'Error',
            'status_description': f'Failed to delete attachment: {str(e)}'
        }), 500