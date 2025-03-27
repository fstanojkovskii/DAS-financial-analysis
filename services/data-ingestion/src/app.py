from flask import Flask, jsonify, request
from flask_cors import CORS
import os
from dotenv import load_dotenv
from alpha_vantage_data_ingestion import (
    connect_to_cassandra,
    get_symbols_from_db,
    check_symbol_data_exists,
    fetch_daily_data
)
import logging
from kafka import KafkaProducer
import json
import threading
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

load_dotenv()
api_key = os.getenv("AlphaFree")
kafka_host = os.getenv("KAFKA_HOST", "localhost:9092")

ingestion_status = {}

def process_symbol(symbol):
    try:
        cluster, session = connect_to_cassandra()
        exists = check_symbol_data_exists(session, symbol)
        cluster.shutdown()

        if exists:
            ingestion_status[symbol] = {'status': 'completed', 'message': f'Data for {symbol} already exists'}
            return

        producer = KafkaProducer(
            bootstrap_servers=[kafka_host],
            value_serializer=lambda m: json.dumps(m).encode("utf-8"),
        )

        data = fetch_daily_data(symbol, api_key)
        if not data:
            ingestion_status[symbol] = {'status': 'failed', 'message': f'Failed to fetch data for {symbol}'}
            return

        count = 0
        for date, values in data.items():
            message = {
                'symbol': symbol,
                'date': date,
                'open': float(values['1. open']),
                'high': float(values['2. high']),
                'low': float(values['3. low']),
                'close': float(values['4. close']),
                'volume': int(values['5. volume'])
            }
            producer.send('raw-data', message)
            count += 1

        producer.flush()
        producer.close()

        ingestion_status[symbol] = {'status': 'completed', 'message': f'Successfully ingested {count} records'}
        
    except Exception as e:
        logger.error(f"Error processing {symbol}: {e}")
        ingestion_status[symbol] = {'status': 'failed', 'message': str(e)}

@app.route('/api/ingestion/start', methods=['POST'])
def start_ingestion():
    try:
        data = request.get_json()
        symbol = data.get('symbol')
        
        if not symbol:
            return jsonify({'error': 'Symbol is required'}), 400

        if symbol in ingestion_status and ingestion_status[symbol]['status'] == 'running':
            return jsonify({'error': f'Ingestion already running for {symbol}'}), 400

        ingestion_status[symbol] = {'status': 'running', 'message': 'Starting data ingestion'}
        thread = threading.Thread(target=process_symbol, args=(symbol,))
        thread.start()

        return jsonify({'status': 'success', 'message': f'Started ingestion for {symbol}'})

    except Exception as e:
        logger.error(f"Failed to start ingestion: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/ingestion/status/<symbol>', methods=['GET'])
def get_ingestion_status(symbol):
    try:
        status = ingestion_status.get(symbol, {'status': 'not_found', 'message': 'No ingestion found for this symbol'})
        return jsonify(status)
    except Exception as e:
        logger.error(f"Failed to get ingestion status: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/symbols', methods=['GET'])
def get_symbols():
    try:
        cluster, session = connect_to_cassandra()
        symbols = get_symbols_from_db(session)
        cluster.shutdown()
        return jsonify({'symbols': symbols})
    except Exception as e:
        logger.error(f"Failed to fetch symbols: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/stock/<symbol>', methods=['GET'])
def get_stock_data(symbol):
    try:
        cluster, session = connect_to_cassandra()
        exists = check_symbol_data_exists(session, symbol)
        cluster.shutdown()

        if exists:
            return jsonify({'message': f'Data for {symbol} already exists in database'})

        data = fetch_daily_data(symbol, api_key)
        if not data:
            return jsonify({'error': f'Failed to fetch data for {symbol}'}), 404

        return jsonify(data)
    except Exception as e:
        logger.error(f"Failed to fetch stock data for {symbol}: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy'})

if __name__ == '__main__':
    if not api_key:
        logger.error("API key not found in environment variables")
    else:
        app.run(host='0.0.0.0', port=5001, debug=True)