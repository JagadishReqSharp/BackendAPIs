from functools import wraps
from flask import request, jsonify
import requests
import logging
from foundational_v2 import  get_user_api_access_level

def validate_access(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        try:
            # Get current_user from args since it's passed as first positional argument
            current_user = args[0] if args else None

            logging.info("inside access_validation_at_api_level")
            logging.info(f"current_user: {current_user}")


            if not current_user:
                return jsonify({'message': 'User authentication required'}), 401

            api_name = request.endpoint.split('.')[-1]
            logging.info(f"current_user[user_id]: {current_user['user_id']}")
            logging.info(f"corporate_account: {request.json.get('corporate_account')}")
            logging.info(f"project_id: {request.json.get('project_id')}")
            logging.info(f"api_name: {api_name}")


            validation_data = {
                'user_id': current_user['user_id'],
                'corporate_account': request.json.get('corporate_account'),
                'project_id': request.json.get('project_id'),
                'api_name': api_name
            }
            logging.info("Helllooooooo 1")

            access_level, access_status, sts, sts_description = get_user_api_access_level(
                current_user['user_id'],
                request.json.get('corporate_account'),
                request.json.get('project_id'),
                api_name
            )

            # response = requests.post(
            #     'http://127.0.0.1:5000/api/get_user_api_access_level',
            #     json=validation_data,
            #     headers=request.headers
            # )
            #
            # access_result = response.json()

            if not access_status:
                # error_message = access_result.get('status_description', 'Access denied')
                return jsonify({
                    'message': f'Access denied: {sts_description}',
                    'status': 'Failed',
                    'status_description': sts_description,
                    'error_type': 'INSUFFICIENT_ACCESS'
                }), 403


            return f(*args, **kwargs)

        except Exception as e:
            logging.error(f"Access validation error: {str(e)}")
            return jsonify({
                'message': 'Access validation failed',
                'error': str(e)
            }), 500

    return decorated

