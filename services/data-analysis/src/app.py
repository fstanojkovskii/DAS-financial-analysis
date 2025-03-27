from flask import Flask, jsonify, request
from flask_cors import CORS
import logging
from threading import Thread
from dataAnalysis import (
    connect_to_cassandra,
    process_record,
    check_existing_data,
    fetch_indicators,
    fetch_historical_data
)
from indicators import (
    check_existing_indicators,
    calculate_indicators,
    save_indicators_to_cassandra,
    process_record as process_indicators_record
)
import pandas as pd
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

analysis_process = None

def run_analysis(symbols=None, date=None):
    global analysis_process
    try:
        cluster, session = connect_to_cassandra()
        
        if symbols:
            if isinstance(symbols, str):
                symbols = [symbols]
            for symbol in symbols:
                process_record(symbol, date, session)
        else:
            query = "SELECT symbol, date FROM stock_prices"
            rows = session.execute(query)
            records = [(row.symbol, row.date) for row in rows]
            
            for sym, dat in records:
                process_record(sym, dat, session)
                
        cluster.shutdown()
        
    except Exception as e:
        logger.error(f"Error in analysis process: {e}")
        raise

def run_indicators_analysis(symbols=None, date=None):
    try:
        cluster, session = connect_to_cassandra()
        
        if symbols:
            if isinstance(symbols, str):
                symbols = [symbols]
            for symbol in symbols:
                process_indicators_record(session, symbol, date)
        else:
            query = "SELECT symbol, date FROM stock_prices"
            rows = session.execute(query)
            for row in rows:
                process_indicators_record(session, row.symbol, row.date)
                
        cluster.shutdown()
        
    except Exception as e:
        logger.error(f"Error in indicators process: {e}")
        raise

@app.route('/api/analysis/start', methods=['POST'])
def start_analysis():
    global analysis_process
    
    try:
        data = request.get_json()
        symbols = data.get('symbols')
        date = data.get('date')
        
        if analysis_process and analysis_process.is_alive():
            return jsonify({
                'status': 'error',
                'message': 'Analysis is already running'
            }), 400
            
        thread = Thread(target=run_analysis, args=(symbols, date))
        thread.daemon = True
        thread.start()
        analysis_process = thread
        
        return jsonify({
            'status': 'success',
            'message': 'Analysis started successfully',
            'data': {
                'symbols': symbols if symbols else 'all',
                'date': date
            }
        })
        
    except Exception as e:
        logger.error(f"Failed to start analysis: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@app.route('/api/indicators/start', methods=['POST'])
def start_indicators_analysis():
    try:
        data = request.get_json()
        symbols = data.get('symbols')
        date = data.get('date')
        
        thread = Thread(target=run_indicators_analysis, args=(symbols, date))
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'status': 'success',
            'message': 'Indicators analysis started successfully',
            'data': {
                'symbols': symbols if symbols else 'all',
                'date': date
            }
        })
        
    except Exception as e:
        logger.error(f"Failed to start indicators analysis: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@app.route('/api/analysis/status', methods=['GET'])
def get_analysis_status():
    global analysis_process
    
    if analysis_process and analysis_process.is_alive():
        return jsonify({
            'status': 'running',
            'message': 'Analysis is currently running'
        })
    else:
        return jsonify({
            'status': 'stopped',
            'message': 'Analysis is not running'
        })

@app.route('/api/analysis/data/<symbol>', methods=['GET'])
def get_analysis_data(symbol):
    try:
        date = request.args.get('date')
        if not date:
            return jsonify({
                'status': 'error',
                'message': 'Date parameter is required'
            }), 400
            
        cluster, session = connect_to_cassandra()
        
        if not check_existing_data(session, symbol, date):
            return jsonify({
                'status': 'error',
                'message': 'Analysis data not found'
            }), 404
            
        ma_5, ma_10, volatility = fetch_indicators(session, symbol, date)
        
        historical_data = fetch_historical_data(session, symbol, date)
        
        if historical_data:
            df = pd.DataFrame(list(historical_data), columns=['date', 'close'])
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
            df['ma_5'] = ma_5
            df['ma_10'] = ma_10
            df['volatility'] = volatility
            
            result = {
                'symbol': symbol,
                'date': date,
                'indicators': {
                    'ma_5': ma_5,
                    'ma_10': ma_10,
                    'volatility': volatility
                },
                'historical_data': df.to_dict(orient='records')
            }
        else:
            result = {
                'symbol': symbol,
                'date': date,
                'indicators': {
                    'ma_5': ma_5,
                    'ma_10': ma_10,
                    'volatility': volatility
                },
                'historical_data': []
            }
            
        cluster.shutdown()
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Failed to fetch analysis data: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@app.route('/api/indicators/calculate', methods=['POST'])
def calculate_indicators_endpoint():
    try:
        data = request.get_json()
        symbol = data.get('symbol')
        date = data.get('date')
        
        if not symbol or not date:
            return jsonify({
                'status': 'error',
                'message': 'Symbol and date are required'
            }), 400
            
        cluster, session = connect_to_cassandra()
        
        if check_existing_indicators(session, symbol, date):
            return jsonify({
                'status': 'error',
                'message': 'Indicators already exist for this symbol and date'
            }), 400
            
        historical_data = fetch_historical_data(session, symbol, date)
        
        indicators = calculate_indicators(historical_data)
        save_indicators_to_cassandra(session, symbol, date, indicators)
        
        cluster.shutdown()
        return jsonify({
            'status': 'success',
            'message': 'Indicators calculated and saved successfully',
            'data': indicators
        })
        
    except Exception as e:
        logger.error(f"Failed to calculate indicators: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5003, debug=True) 