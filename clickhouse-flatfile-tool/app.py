import os
import json
from flask import Flask, render_template, request, jsonify, send_file
from werkzeug.utils import secure_filename
from utils.clickhouse import ClickHouseManager
from utils.flatfile import FlatFileManager

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = os.path.join(os.getcwd(), 'uploads')
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB limit for uploads

# Ensure upload directory exists
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/connect/clickhouse', methods=['POST'])
def connect_clickhouse():
    try:
        data = request.json
        host = data.get('host')
        port = int(data.get('port'))
        database = data.get('database')
        user = data.get('user')
        jwt_token = data.get('jwt_token')
        
        ch_manager = ClickHouseManager(host, port, database, user, jwt_token)
        tables = ch_manager.get_tables()
        
        return jsonify({
            'status': 'success',
            'message': 'Connected successfully',
            'tables': tables
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Connection failed: {str(e)}'
        }), 400

@app.route('/api/get/table/columns', methods=['POST'])
def get_table_columns():
    try:
        data = request.json
        host = data.get('host')
        port = int(data.get('port'))
        database = data.get('database')
        user = data.get('user')
        jwt_token = data.get('jwt_token')
        table = data.get('table')
        
        ch_manager = ClickHouseManager(host, port, database, user, jwt_token)
        columns = ch_manager.get_columns(table)
        
        return jsonify({
            'status': 'success',
            'columns': columns
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Failed to get columns: {str(e)}'
        }), 400

@app.route('/api/get/tables/join', methods=['POST'])
def get_join_tables():
    try:
        data = request.json
        host = data.get('host')
        port = int(data.get('port'))
        database = data.get('database')
        user = data.get('user')
        jwt_token = data.get('jwt_token')
        tables = data.get('tables', [])
        
        ch_manager = ClickHouseManager(host, port, database, user, jwt_token)
        result = {}
        
        for table in tables:
            result[table] = ch_manager.get_columns(table)
        
        return jsonify({
            'status': 'success',
            'tables_columns': result
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Failed to get tables columns: {str(e)}'
        }), 400

@app.route('/api/upload/flatfile', methods=['POST'])
def upload_flatfile():
    if 'file' not in request.files:
        return jsonify({
            'status': 'error',
            'message': 'No file part'
        }), 400
    
    file = request.files['file']
    delimiter = request.form.get('delimiter', ',')
    
    if file.filename == '':
        return jsonify({
            'status': 'error',
            'message': 'No selected file'
        }), 400
    
    if file:
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)
        
        try:
            ff_manager = FlatFileManager(filepath, delimiter)
            columns = ff_manager.get_columns()
            
            return jsonify({
                'status': 'success',
                'message': 'File uploaded successfully',
                'filename': filename,
                'filepath': filepath,
                'columns': columns
            })
        except Exception as e:
            return jsonify({
                'status': 'error',
                'message': f'Failed to process file: {str(e)}'
            }), 400

@app.route('/api/preview/clickhouse', methods=['POST'])
def preview_clickhouse():
    try:
        data = request.json
        host = data.get('host')
        port = int(data.get('port'))
        database = data.get('database')
        user = data.get('user')
        jwt_token = data.get('jwt_token')
        table = data.get('table')
        columns = data.get('columns', [])
        join_config = data.get('join_config', None)
        
        ch_manager = ClickHouseManager(host, port, database, user, jwt_token)
        
        if join_config:
            preview_data = ch_manager.preview_join_data(join_config, columns)
        else:
            preview_data = ch_manager.preview_data(table, columns)
        
        return jsonify({
            'status': 'success',
            'data': preview_data
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Preview failed: {str(e)}'
        }), 400

@app.route('/api/preview/flatfile', methods=['POST'])
def preview_flatfile():
    try:
        data = request.json
        filepath = data.get('filepath')
        delimiter = data.get('delimiter', ',')
        columns = data.get('columns', [])
        
        ff_manager = FlatFileManager(filepath, delimiter)
        preview_data = ff_manager.preview_data(columns)
        
        return jsonify({
            'status': 'success',
            'data': preview_data
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Preview failed: {str(e)}'
        }), 400

@app.route('/api/ingest/clickhouse-to-flatfile', methods=['POST'])
def ingest_clickhouse_to_flatfile():
    try:
        data = request.json
        # ClickHouse source config
        host = data.get('host')
        port = int(data.get('port'))
        database = data.get('database')
        user = data.get('user')
        jwt_token = data.get('jwt_token')
        
        # Table and columns selection
        table = data.get('table')
        columns = data.get('columns', [])
        join_config = data.get('join_config', None)
        
        # Output file config
        output_filename = data.get('output_filename')
        delimiter = data.get('delimiter', ',')
        
        ch_manager = ClickHouseManager(host, port, database, user, jwt_token)
        
        # Generate output path
        output_path = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(output_filename))
        
        # Execute ingestion
        if join_config:
            count = ch_manager.export_join_to_file(join_config, columns, output_path, delimiter)
        else:
            count = ch_manager.export_to_file(table, columns, output_path, delimiter)
        
        return jsonify({
            'status': 'success',
            'message': 'Data exported successfully',
            'count': count,
            'output_path': output_path,
            'output_filename': os.path.basename(output_path)
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Export failed: {str(e)}'
        }), 400

@app.route('/api/ingest/flatfile-to-clickhouse', methods=['POST'])
def ingest_flatfile_to_clickhouse():
    try:
        data = request.json
        # Flat file source config
        filepath = data.get('filepath')
        delimiter = data.get('delimiter', ',')
        columns = data.get('columns', [])
        
        # ClickHouse target config
        host = data.get('host')
        port = int(data.get('port'))
        database = data.get('database')
        user = data.get('user')
        jwt_token = data.get('jwt_token')
        target_table = data.get('target_table')
        create_table = data.get('create_table', False)
        
        # Initialize managers
        ff_manager = FlatFileManager(filepath, delimiter)
        ch_manager = ClickHouseManager(host, port, database, user, jwt_token)
        
        # Execute ingestion
        count = ch_manager.import_from_file(ff_manager, columns, target_table, create_table)
        
        return jsonify({
            'status': 'success',
            'message': 'Data imported successfully',
            'count': count,
            'table': target_table
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Import failed: {str(e)}'
        }), 400

@app.route('/api/download/<filename>')
def download_file(filename):
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    if os.path.exists(filepath):
        return send_file(filepath, as_attachment=True)
    else:
        return jsonify({
            'status': 'error',
            'message': 'File not found'
        }), 404

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)