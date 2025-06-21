# utils.py
import jwt
import logging
from config import SECRET_KEY
from flask import request, jsonify
from functools import wraps

def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None
        logging.info("Checking for token in request headers...1")
        if 'Authorization' in request.headers:
            token = request.headers['Authorization'].split(" ")[1]
            logging.info(f"Token found: {token}")

        if not token:
            return jsonify({'message': 'Token is missing!'}), 401

        try:
            logging.info(f"Decoding the token...SECRET_KEY {SECRET_KEY}")
            data = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
            logging.info(f"Decoded token data: {data}")
            # Create a user dictionary instead of just the ID
            current_user = {'user_id': data['user_id']}
        except jwt.ExpiredSignatureError:
            logging.error("Token has expired")
            return jsonify({'message': 'Token has expired!'}), 401
        except jwt.InvalidTokenError as e:
            logging.error(f"Invalid token error: {str(e)}")
            return jsonify({'message': 'Token is invalid!'}), 401
        except jwt.DecodeError as e:
            logging.error(f"Token decode error: {str(e)}")
            return jsonify({'message': 'Token decode failed!'}), 401
        except Exception as e:
            logging.error(f"Unexpected error during token validation: {str(e)}")
            return jsonify({'message': 'Token validation failed!'}), 401

        return f(current_user, *args, **kwargs)

    return decorated