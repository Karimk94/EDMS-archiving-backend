from flask import Blueprint, request, jsonify, session
from werkzeug.security import check_password_hash, generate_password_hash
import wsdl_client

auth_bp = Blueprint('auth', __name__)

# This is a placeholder for real user management.
# In a real app, you would fetch this from a database.
# For now, we only check against the DMS credentials.
users = {
    # Storing plain passwords here is not secure for a real application.
    # This is only for the DMS login demonstration.
}

@auth_bp.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')

    if not username or not password:
        return jsonify({"error": "Username and password are required"}), 400

    # Use the DMS login function as the source of truth for authentication
    dst = wsdl_client.dms_login(username, password)

    if dst:
        # If DMS login is successful, create a session for the user
        session['user'] = {'username': username}
        session['dst'] = dst # Store the DMS token in the session
        return jsonify({"message": "Login successful", "user": {"username": username}}), 200
    else:
        return jsonify({"error": "Invalid DMS credentials"}), 401

@auth_bp.route('/logout', methods=['POST'])
def logout():
    session.pop('user', None)
    session.pop('dst', None)
    return jsonify({"message": "Logout successful"}), 200

# NEW: This endpoint checks and returns the currently logged-in user
@auth_bp.route('/user', methods=['GET'])
def get_user():
    user = session.get('user')
    if user:
        return jsonify({'user': user}), 200
    else:
        # Return an error if no user is in the session
        return jsonify({'error': 'Not authenticated'}), 401