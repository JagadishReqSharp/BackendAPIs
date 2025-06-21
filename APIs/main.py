# main.py
from flask import Flask
from flask_cors import CORS
from account_and_project_v2 import account_and_project_blueprint
from initial_setup_v2 import initialsetup_blueprint
from requirements_v2 import requirements_blueprint
from integration_requirements_v2 import integration_requirements_blueprint
from upload_attachment import file_management_blueprint
from base_requirements_v2 import base_requirements_blueprint
from project_management_v2 import raid_log_blueprint
from FeedbackSubmission import feedback_blueprint
import os



app = Flask(__name__)

CORS(app)

# Register all blueprints
#app.register_blueprint(get_user_info)
#app.register_blueprint(get_business_team_list)

app.register_blueprint(initialsetup_blueprint)
app.register_blueprint(account_and_project_blueprint)
app.register_blueprint(requirements_blueprint)
app.register_blueprint(base_requirements_blueprint)
app.register_blueprint(integration_requirements_blueprint)
app.register_blueprint(file_management_blueprint)
app.register_blueprint(raid_log_blueprint)
app.register_blueprint(feedback_blueprint)


if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000)) # Default to 5000 if PORT is not set
    host = os.environ.get('HOST', '0.0.0.0')
    app.run(host=host, port=port)