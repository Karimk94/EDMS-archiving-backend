from flask import Flask, jsonify, request, session, Response
from flask_cors import CORS
from waitress import serve
import os
import json
from auth import auth_bp
import db_connector
import wsdl_client
import mimetypes 

app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY')
CORS(app, supports_credentials=True, resources={r"/api/*": {"origins": "*"}})
app.register_blueprint(auth_bp, url_prefix='/api/auth')

@app.route('/api/dashboard_counts', methods=['GET'])
def get_dashboard_counts():
    if 'user' not in session:
        return jsonify({"error": "Unauthorized"}), 401
    counts = db_connector.get_dashboard_counts()
    return jsonify(counts)

@app.route('/api/employees', methods=['GET'])
def get_employees():
    if 'user' not in session: return jsonify({"error": "Unauthorized"}), 401
    employees, total_rows = db_connector.fetch_archived_employees(
        search_term=request.args.get('search'),
        status=request.args.get('status'),
        filter_type=request.args.get('filter_type')
    )
    return jsonify({"employees": employees, "total_employees": total_rows})

@app.route('/api/employees', methods=['POST'])
def add_employee_archive():
    if 'user' not in session or 'dst' not in session: 
        return jsonify({"error": "Unauthorized"}), 401
    
    try:
        dms_user = session['user']['username']
        employee_data = json.loads(request.form.get('employee_data'))
        
        documents = []
        i = 0
        while f'new_documents[{i}][file]' in request.files:
            documents.append({
                "file": request.files[f'new_documents[{i}][file]'],
                "doc_type_id": request.form.get(f'new_documents[{i}][doc_type_id]'),
                "doc_type_name": request.form.get(f'new_documents[{i}][doc_type_name]'),
                "expiry": request.form.get(f'new_documents[{i}][expiry]'),
                "legislation_id": request.form.get(f'new_documents[{i}][legislation_id]')
            })
            i += 1

        success, message = db_connector.add_employee_archive_with_docs(session['dst'], dms_user, employee_data, documents)
        return (jsonify({"message": message}), 201) if success else (jsonify({"error": message}), 500)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/employees/<int:archive_id>', methods=['GET'])
def get_employee_details(archive_id):
    if 'user' not in session: return jsonify({"error": "Unauthorized"}), 401
    details = db_connector.fetch_single_archived_employee(archive_id)
    return jsonify(details) if details else (jsonify({"error": "Not found"}), 404)

@app.route('/api/employees/<int:archive_id>', methods=['PUT'])
def update_employee_archive(archive_id):
    if 'user' not in session or 'dst' not in session: return jsonify({"error": "Unauthorized"}), 401
    
    dms_user = session['user']['username']
    employee_data = json.loads(request.form.get('employee_data'))
    
    new_documents = []
    i = 0
    while f'new_documents[{i}][file]' in request.files:
        new_documents.append({
            "file": request.files[f'new_documents[{i}][file]'],
            "doc_type_id": request.form.get(f'new_documents[{i}][doc_type_id]'),
            "doc_type_name": request.form.get(f'new_documents[{i}][doc_type_name]'),
            "expiry": request.form.get(f'new_documents[{i}][expiry]'),
            "legislation_id": request.form.get(f'new_documents[{i}][legislation_id]')
        })
        i += 1
        
    deleted_doc_ids = json.loads(request.form.get('deleted_documents', '[]'))
    success, message = db_connector.update_archived_employee(session['dst'], dms_user, archive_id, employee_data, new_documents, deleted_doc_ids)
    
    return (jsonify({"message": message}), 200) if success else (jsonify({"error": message}), 500)

@app.route('/api/hr_employees', methods=['GET'])
def get_hr_employees():
    if 'user' not in session: return jsonify({"error": "Unauthorized"}), 401
    search = request.args.get('search', "", type=str)
    page = request.args.get('page', 1, type=int)
    employees, total_rows = db_connector.fetch_hr_employees_paginated(search_term=search, page=page)
    has_more = (page * 10) < total_rows
    return jsonify({"employees": employees, "hasMore": has_more})

@app.route('/api/hr_employees/<int:employee_id>', methods=['GET'])
def get_hr_employee_details(employee_id):
    if 'user' not in session: return jsonify({"error": "Unauthorized"}), 401
    details = db_connector.fetch_hr_employee_details(employee_id)
    return jsonify(details) if details else (jsonify({"error": "Not found"}), 404)

@app.route('/api/statuses', methods=['GET'])
def get_statuses():
    if 'user' not in session: return jsonify({"error": "Unauthorized"}), 401
    statuses = db_connector.fetch_statuses()
    return jsonify(statuses)

@app.route('/api/document_types', methods=['GET'])
def get_document_types():
    if 'user' not in session: return jsonify({"error": "Unauthorized"}), 401
    doc_types = db_connector.fetch_document_types()
    return jsonify(doc_types)

@app.route('/api/legislations', methods=['GET'])
def get_legislations():
    if 'user' not in session: return jsonify({"error": "Unauthorized"}), 401
    legislations = db_connector.fetch_legislations()
    return jsonify(legislations)

@app.route('/api/document/<int:docnumber>', methods=['GET'])
def get_document_file(docnumber):
    """
    Securely streams a document from the DMS to the client.
    """
    if 'user' not in session or 'dst' not in session:
        return jsonify({"error": "Unauthorized"}), 401

    dst = session['dst']
    file_bytes, filename = wsdl_client.get_document_from_dms(dst, docnumber)

    if file_bytes and filename:
        mimetype = mimetypes.guess_type(filename)[0] or 'application/octet-stream'
        return Response(file_bytes, mimetype=mimetype)
    else:
        return jsonify({"error": "Document not found or could not be retrieved from DMS."}), 404

if __name__ == '__main__':
    port = os.getenv('FLASK_PORT', 8443)
    print(f"Starting server on http://localhost:{port}")
    serve(app, host='0.0.0.0', port=port, threads=4)