import os
import smtplib
import logging
import uuid
import mysql.connector
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from werkzeug.utils import secure_filename
from flask import Flask, request, jsonify, Blueprint
import threading
import time
import config
from access_validation_at_api_level import validate_access
from utils import token_required

# Create Blueprint for feedback management
feedback_blueprint = Blueprint('feedback', __name__)

# Configuration
UPLOAD_FOLDER = config.ATTACHMENTS_FOLDER_PATH
FEEDBACK_FOLDER = 'feedback'  # Subfolder for feedback attachments
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10MB
MAX_FILES = 5
ALLOWED_EXTENSIONS = {'pdf', 'doc', 'docx', 'xls', 'xlsx', 'txt', 'png', 'jpg', 'jpeg', 'gif'}

# Email configuration using your hosting provider's settings
SMTP_SERVER = config.SMTP_SERVER
SMTP_PORT = config.SMTP_PORT
SENDER_EMAIL = config.EMAIL_ID
EMAIL_PASSWORD = config.EMAIL_PASSWORD

# Ensure upload directory exists
feedback_upload_path = os.path.join(UPLOAD_FOLDER, FEEDBACK_FOLDER)
os.makedirs(feedback_upload_path, exist_ok=True)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def allowed_file(filename):
    """Check if file extension is allowed"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def get_file_size_mb(size_bytes):
    """Convert bytes to MB"""
    return round(size_bytes / (1024 * 1024), 2)


def cleanup_files(file_paths, delay=60):
    """Clean up uploaded files after a delay"""

    def cleanup():
        time.sleep(delay)
        for file_path in file_paths:
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    logger.info(f"Deleted file: {file_path}")
            except Exception as e:
                logger.error(f"Error deleting file {file_path}: {e}")

    cleanup_thread = threading.Thread(target=cleanup)
    cleanup_thread.daemon = True
    cleanup_thread.start()


def store_feedback_in_database(feedback_data, attachments=None):
    """Store feedback submission in database"""
    try:
        connection = mysql.connector.connect(
            host=config.host,
            database=config.database,
            user=config.user,
            password=config.password
        )
        cursor = connection.cursor()

        # Insert feedback record
        cursor.execute("""
            INSERT INTO FEEDBACK_SUBMISSIONS 
            (TYPE, SUBJECT, DESCRIPTION, PRIORITY, CATEGORY, USER_NAME, USER_EMAIL, 
             USER_PROJECT, USER_COMPANY, SUBMISSION_DATE, STATUS)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            feedback_data['type'],
            feedback_data['subject'],
            feedback_data['description'],
            feedback_data['priority'],
            feedback_data['category'],
            feedback_data['userName'],
            feedback_data['userEmail'],
            feedback_data['userProject'],
            feedback_data['userCompany'],
            datetime.now(),
            'SUBMITTED'
        ))

        feedback_id = cursor.lastrowid

        # Store attachments if any
        if attachments:
            for attachment in attachments:
                cursor.execute("""
                    INSERT INTO FEEDBACK_ATTACHMENTS 
                    (FEEDBACK_ID, FILE_NAME, FILE_PATH, FILE_SIZE, FILE_TYPE, UPLOAD_DATE)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    feedback_id,
                    attachment['original_filename'],
                    attachment['relative_path'],
                    attachment['file_size'],
                    attachment['file_type'],
                    datetime.now()
                ))

        connection.commit()
        cursor.close()
        connection.close()

        return feedback_id

    except Exception as e:
        logger.error(f"Error storing feedback in database: {e}")
        if 'connection' in locals():
            connection.rollback()
            cursor.close()
            connection.close()
        return None


def send_feedback_email(feedback_data, recipient_email, attachments=None, feedback_id=None):
    """Send feedback email using SMTP"""
    try:
        # Create message
        msg = MIMEMultipart()
        msg['From'] = SENDER_EMAIL
        msg['To'] = recipient_email
        msg['Subject'] = f"[ReqSharp {feedback_data['type'].upper()}] {feedback_data['subject']}"

        # Set reply-to if user email is provided
        if feedback_data.get('userEmail'):
            msg['Reply-To'] = feedback_data['userEmail']

        # Pre-process the description to replace newlines with <br> tags
        formatted_description = feedback_data['description'].replace('\n', '<br>')

        # Create HTML email content
        html_content = f"""
        <html>
        <body>
            <h2>New {feedback_data['type'].title()} Submission</h2>

            <h3>User Information:</h3>
            <ul>
                <li><strong>Name:</strong> {feedback_data.get('userName', 'Not provided')}</li>
                <li><strong>Email:</strong> {feedback_data.get('userEmail', 'Not provided')}</li>
                <li><strong>Project:</strong> {feedback_data.get('userProject', 'Not provided')}</li>
                <li><strong>Company:</strong> {feedback_data.get('userCompany', 'Not provided')}</li>
            </ul>

            <h3>Submission Details:</h3>
            <ul>
                <li><strong>Feedback ID:</strong> {feedback_id or 'N/A'}</li>
                <li><strong>Type:</strong> {feedback_data['type']}</li>
                <li><strong>Category:</strong> {feedback_data.get('category', 'Not specified')}</li>
                <li><strong>Priority:</strong> {feedback_data.get('priority', 'medium').upper()}</li>
                <li><strong>Subject:</strong> {feedback_data['subject']}</li>
                <li><strong>Submission Date:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</li>
            </ul>

            <h3>Description:</h3>
            <div style="background-color: #f5f5f5; padding: 15px; border-radius: 5px; margin: 10px 0;">
                {formatted_description}
            </div>
        """

        # Add attachments info to email content
        if attachments:
            html_content += "<h3>Attachments:</h3><ul>"
            for attachment in attachments:
                file_size = get_file_size_mb(attachment['file_size'])
                html_content += f"<li>{attachment['original_filename']} ({file_size} MB)</li>"
            html_content += "</ul>"
        else:
            html_content += "<p><em>No attachments</em></p>"

        html_content += """
            <hr>
            <p><small>This message was sent from the ReqSharp Feedback System.</small></p>
        </body>
        </html>
        """

        # Attach HTML content
        msg.attach(MIMEText(html_content, 'html'))

        # Add file attachments
        if attachments:
            for attachment in attachments:
                try:
                    full_path = os.path.join(UPLOAD_FOLDER, attachment['relative_path'])
                    with open(full_path, 'rb') as file:
                        part = MIMEBase('application', 'octet-stream')
                        part.set_payload(file.read())
                        encoders.encode_base64(part)
                        part.add_header(
                            'Content-Disposition',
                            f'attachment; filename= {attachment["original_filename"]}'
                        )
                        msg.attach(part)
                except Exception as e:
                    logger.error(f"Error attaching file {attachment['original_filename']}: {e}")

        # Send email using SSL
        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
            server.login(SENDER_EMAIL, EMAIL_PASSWORD)
            server.sendmail(SENDER_EMAIL, recipient_email, msg.as_string())
            logger.info(f"Feedback email sent successfully to {recipient_email}")
            return True

    except Exception as e:
        logger.error(f"Error sending email: {e}")
        return False


@feedback_blueprint.route('/api/submit_feedback', methods=['POST'])
@token_required
def submit_feedback(current_user):
    """Handle feedback submission with file uploads"""
    try:
        # Get form data
        feedback_data = {
            'type': request.form.get('type', 'feedback'),
            'subject': request.form.get('subject', ''),
            'description': request.form.get('description', ''),
            'priority': request.form.get('priority', 'medium'),
            'category': request.form.get('category', ''),
            'userName': request.form.get('userName', ''),
            'userEmail': request.form.get('userEmail', ''),
            'userProject': request.form.get('userProject', ''),
            'userCompany': request.form.get('userCompany', '')
        }

        logging.info("Level 1 - Form data received")

        logging.info(f"Feedback data: {feedback_data}")

        # Validate required fields
        if not feedback_data['subject'] or not feedback_data['description']:
            return jsonify({
                'status': 'Error',
                'status_description': 'Subject, description are required'
            }), 400

        # Handle file uploads
        uploaded_files = []

        if 'files[]' in request.files:
            logging.info("Level 2 - Files found in request")

            files = request.files.getlist('files[]')

            # Check number of files
            if len(files) > MAX_FILES:
                return jsonify({
                    'status': 'Error',
                    'status_description': f'Maximum {MAX_FILES} files allowed'
                }), 400

            # Check if files are actually selected
            if not files or files[0].filename == '':
                files = []  # No files selected

            logging.info("Level 3 - Processing files")

            for file in files:
                if file and file.filename and allowed_file(file.filename):
                    logging.info("Level 4 - Processing individual file")

                    # Create unique filename to prevent collisions
                    original_filename = secure_filename(file.filename)
                    file_extension = original_filename.rsplit('.', 1)[1].lower() if '.' in original_filename else ''
                    unique_filename = f"{uuid.uuid4().hex}.{file_extension}" if file_extension else f"{uuid.uuid4().hex}"

                    # Create timestamp-based folder structure for feedback
                    timestamp_folder = datetime.now().strftime('%Y%m%d')
                    relative_path = f"{FEEDBACK_FOLDER}/{timestamp_folder}"
                    full_dir = os.path.join(UPLOAD_FOLDER, relative_path)
                    os.makedirs(full_dir, exist_ok=True)

                    file_path = os.path.join(full_dir, unique_filename)

                    # Save the file
                    file.save(file_path)

                    # Get file size and validate
                    file_size = os.path.getsize(file_path)
                    if file_size > MAX_FILE_SIZE:
                        os.remove(file_path)  # Clean up oversized file
                        return jsonify({
                            'status': 'Error',
                            'status_description': f'File too large: {original_filename}. Maximum size is {get_file_size_mb(MAX_FILE_SIZE)}MB'
                        }), 400

                    uploaded_files.append({
                        'original_filename': original_filename,
                        'unique_filename': unique_filename,
                        'relative_path': os.path.join(relative_path, unique_filename),
                        'full_path': file_path,
                        'file_size': file_size,
                        'file_type': file_extension
                    })

                    logging.info("Level 5 - File processed successfully")

        # Store feedback in database
        feedback_id = store_feedback_in_database(feedback_data, uploaded_files)

        if not feedback_id:
            # Clean up files if database storage failed
            for file_info in uploaded_files:
                if os.path.exists(file_info['full_path']):
                    os.remove(file_info['full_path'])

            return jsonify({
                'status': 'Error',
                'status_description': 'Failed to store feedback in database'
            }), 500

        # Send email
        email_sent = send_feedback_email(feedback_data, SENDER_EMAIL , uploaded_files, feedback_id)

        if not email_sent:
            logger.warning("Email sending failed, but feedback was stored in database")

        # Log successful submission
        logger.info(
            f"Feedback submitted: ID {feedback_id} - {feedback_data['type']} - {feedback_data['subject']} from {feedback_data.get('userName', 'Anonymous')}")

        return jsonify({
            'status': 'Success',
            'status_description': f'Feedback submitted successfully. {len(uploaded_files)} file(s) uploaded.',
            'feedback_id': feedback_id,
            'attachments': [
                {
                    'file_name': f['original_filename'],
                    'file_size': f['file_size'],
                    'file_type': f['file_type']
                } for f in uploaded_files
            ],
            'email_sent': email_sent,
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        logger.error(f"Error in submit_feedback: {e}")

        # Clean up any uploaded files in case of error
        if 'uploaded_files' in locals():
            for file_info in uploaded_files:
                if os.path.exists(file_info['full_path']):
                    os.remove(file_info['full_path'])

        return jsonify({
            'status': 'Error',
            'status_description': f'Failed to submit feedback: {str(e)}'
        }), 500


@feedback_blueprint.route('/api/get_feedback_history', methods=['POST'])
def get_feedback_history():
    """Get feedback history with optional filtering"""
    try:
        # Get filter parameters
        user_email = request.json.get('user_email')
        feedback_type = request.json.get('type')
        status = request.json.get('status')
        limit = request.json.get('limit', 50)
        offset = request.json.get('offset', 0)

        connection = mysql.connector.connect(
            host=config.host,
            database=config.database,
            user=config.user,
            password=config.password
        )
        cursor = connection.cursor()

        # Build query with filters
        query = """
            SELECT 
                FEEDBACK_ID, TYPE, SUBJECT, DESCRIPTION, PRIORITY, CATEGORY,
                USER_NAME, USER_EMAIL, USER_PROJECT, USER_COMPANY,
                SUBMISSION_DATE, STATUS
            FROM FEEDBACK_SUBMISSIONS
            WHERE 1=1
        """
        params = []

        if user_email:
            query += " AND USER_EMAIL = %s"
            params.append(user_email)

        if feedback_type:
            query += " AND TYPE = %s"
            params.append(feedback_type)

        if status:
            query += " AND STATUS = %s"
            params.append(status)

        query += " ORDER BY SUBMISSION_DATE DESC LIMIT %s OFFSET %s"
        params.extend([limit, offset])

        cursor.execute(query, params)

        feedback_list = []
        for row in cursor.fetchall():
            feedback_list.append({
                'feedback_id': row[0],
                'type': row[1],
                'subject': row[2],
                'description': row[3],
                'priority': row[4],
                'category': row[5],
                'user_name': row[6],
                'user_email': row[7],
                'user_project': row[8],
                'user_company': row[9],
                'submission_date': row[10].strftime('%Y-%m-%d %H:%M:%S') if row[10] else None,
                'status': row[11]
            })

        cursor.close()
        connection.close()

        return jsonify({
            'status': 'Success',
            'feedback_history': feedback_list,
            'count': len(feedback_list)
        })

    except Exception as e:
        logger.error(f"Error retrieving feedback history: {e}")
        return jsonify({
            'status': 'Error',
            'status_description': f'Failed to retrieve feedback history: {str(e)}'
        }), 500


@feedback_blueprint.route('/api/get_feedback_attachments', methods=['POST'])
def get_feedback_attachments():
    """Get all attachments for a specific feedback"""
    try:
        feedback_id = request.json.get('feedback_id')

        if not feedback_id:
            return jsonify({
                'status': 'Error',
                'status_description': 'Feedback ID is required'
            }), 400

        connection = mysql.connector.connect(
            host=config.host,
            database=config.database,
            user=config.user,
            password=config.password
        )
        cursor = connection.cursor()

        cursor.execute("""
            SELECT 
                ATTACHMENT_ID, FILE_NAME, FILE_PATH, FILE_SIZE, FILE_TYPE, UPLOAD_DATE
            FROM FEEDBACK_ATTACHMENTS
            WHERE FEEDBACK_ID = %s
            ORDER BY UPLOAD_DATE DESC
        """, (feedback_id,))

        attachments = []
        for row in cursor.fetchall():
            attachments.append({
                'attachment_id': row[0],
                'file_name': row[1],
                'file_path': row[2],
                'file_size': row[3],
                'file_type': row[4],
                'upload_date': row[5].strftime('%Y-%m-%d %H:%M:%S') if row[5] else None
            })

        cursor.close()
        connection.close()

        return jsonify({
            'status': 'Success',
            'attachments': attachments
        })

    except Exception as e:
        logger.error(f"Error retrieving feedback attachments: {e}")
        return jsonify({
            'status': 'Error',
            'status_description': f'Failed to retrieve attachments: {str(e)}'
        }), 500


@feedback_blueprint.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'Success',
        'status_description': 'Feedback API is running',
        'timestamp': datetime.now().isoformat()
    })


if __name__ == '__main__':
    from flask import Flask

    app = Flask(__name__)
    app.register_blueprint(feedback_blueprint)
    app.run(debug=True, host='0.0.0.0', port=5000)