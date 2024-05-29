import os
import io
import logging
from flask import Flask, jsonify, request, abort, send_file
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine, MetaData
import pandas as pd
import matplotlib.pyplot as plt
from werkzeug.exceptions import HTTPException

# Configurazione del logger
logging.basicConfig(filename='daty.log', level=logging.INFO, 
                    format='%(asctime)s %(message)s')

app = Flask(__name__)

# Configurazione del database tramite variabili di ambiente
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://user:password@localhost/dbname')
app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URL
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

# Configurazione SQLAlchemy per ottenere informazioni sulle tabelle
engine = create_engine(DATABASE_URL)
metadata = MetaData()
metadata.reflect(bind=engine)

@app.route('/tables', methods=['GET'])
def get_tables():
    try:
        tables = {}
        for table_name in metadata.tables.keys():
            table = metadata.tables[table_name]
            columns = [column.name for column in table.columns]
            tables[table_name] = columns
        logging.info('Tables endpoint was accessed')
        return jsonify(tables)
    except Exception as e:
        logging.error(f'Error in /tables endpoint: {str(e)}')
        abort(500, description="Internal Server Error")

@app.route('/table/<table_name>', methods=['GET'])
def get_table(table_name):
    try:
        if table_name not in metadata.tables:
            logging.error(f'Table {table_name} not found')
            abort(404, description="Table not found")
        table = metadata.tables[table_name]
        columns = [column.name for column in table.columns]
        logging.info(f'Table {table_name} was accessed')
        return jsonify({table_name: columns})
    except Exception as e:
        logging.error(f'Error in /table/{table_name} endpoint: {str(e)}')
        abort(500, description="Internal Server Error")

@app.route('/graph', methods=['GET'])
def create_graph():
    try:
        table_name = request.args.get('table')
        x_field = request.args.get('x_field')
        y_field = request.args.get('y_field')

        if not table_name or not x_field or not y_field:
            logging.error('Missing required parameters for graph')
            abort(400, description="Missing required parameters")

        if table_name not in metadata.tables:
            logging.error(f'Table {table_name} not found')
            abort(404, description="Table not found")

        table = metadata.tables[table_name]
        if x_field not in table.columns or y_field not in table.columns:
            logging.error(f'Invalid fields for table {table_name}')
            abort(400, description="Invalid fields for the specified table")

        # Query per ottenere i dati
        query = f"SELECT {x_field}, {y_field} FROM {table_name}"
        df = pd.read_sql(query, engine)

        # Riempimento dei valori nulli
        df = df.fillna(0)

        # Creazione del grafico scatter
        plt.figure()
        plt.scatter(df[x_field], df[y_field])
        plt.xlabel(x_field)
        plt.ylabel(y_field)
        plt.title(f'Scatter Plot: {x_field} vs {y_field}')

        # Salvataggio del grafico in un buffer
        buf = io.BytesIO()
        plt.savefig(buf, format='png')
        buf.seek(0)
        plt.close()

        logging.info(f'Graph created for table {table_name} with fields {x_field} and {y_field}')
        return send_file(buf, mimetype='image/png')
    except Exception as e:
        logging.error(f'Error in /graph endpoint: {str(e)}')
        abort(500, description="Internal Server Error")

@app.errorhandler(Exception)
def handle_exception(e):
    # Passa attraverso HTTPException e usa il suo codice di stato
    if isinstance(e, HTTPException):
        response = e.get_response()
        response.data = jsonify({
            "code": e.code,
            "name": e.name,
            "description": e.description,
        }).data
        response.content_type = "application/json"
    else:
        # Gestisci tutte le altre eccezioni
        logging.error(f'Unhandled exception: {str(e)}')
        response = jsonify({
            "code": 500,
            "name": "Internal Server Error",
            "description": "An unexpected error occurred. Please try again later."
        })
        response.status_code = 500
    return response

if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    logging.info(f'Starting Daty service on port {port}')
    print(f'Starting Daty service on port {port}')
    app.run(debug=True, port=port)
