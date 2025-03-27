from gevent import monkey
monkey.patch_all()

from flask import Flask, jsonify, request
from flask_cors import CORS
from visualization import (
    connect_to_cassandra,
    get_historical_data,
    get_forecast_data,
    get_symbol_price,
    get_available_symbols,
    create_line_chart,
    create_candlestick_chart,
    create_forecast_chart
)
import logging
from gevent.pywsgi import WSGIServer
import traceback

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

@app.route('/api/chart/<symbol>', methods=['GET'])
def get_chart(symbol):
    try:
        logger.info(f"Received chart request for {symbol}")
        chart_type = request.args.get('type', 'line')
        timeframe = request.args.get('timeframe', '1y')
        
        logger.info(f"Chart type: {chart_type}, Timeframe: {timeframe}")
        
        cluster, session = connect_to_cassandra()
        
        df = get_historical_data(session, symbol, timeframe)
        if df is None:
            return jsonify({'error': f'No data available for {symbol}'}), 404
            
        try:
            if chart_type == 'line':
                chart_data = create_line_chart(df, symbol)
            elif chart_type == 'candlestick':
                chart_data = create_candlestick_chart(df, symbol)
            else:
                return jsonify({'error': 'Invalid chart type'}), 400
        except Exception as e:
            logger.error(f"Failed to create chart: {e}")
            logger.error(traceback.format_exc())
            return jsonify({'error': 'Failed to create chart'}), 500
            
        cluster.shutdown()
        return jsonify({'chart': chart_data})
        
    except Exception as e:
        logger.error(f"Failed to generate chart for {symbol}: {e}")
        logger.error(traceback.format_exc())
        return jsonify({'error': str(e)}), 500

@app.route('/api/forecast_chart/<symbol>', methods=['GET'])
def get_forecast_chart(symbol):
    try:
        logger.info(f"Received forecast chart request for {symbol}")
        cluster, session = connect_to_cassandra()
        historical_df = get_historical_data(session, symbol)
        
        if historical_df is None:
            return jsonify({'error': f'No historical data available for {symbol}'}), 404
            
        forecast_df = get_forecast_data(session, symbol)
        if forecast_df is None:
            return jsonify({'error': f'No forecast data available for {symbol}'}), 404
            
        try:
            chart_data = create_forecast_chart(historical_df, forecast_df, symbol)
        except Exception as e:
            logger.error(f"Failed to create forecast chart: {e}")
            logger.error(traceback.format_exc())
            return jsonify({'error': 'Failed to create forecast chart'}), 500
            
        cluster.shutdown()
        return jsonify({'chart': chart_data})
        
    except Exception as e:
        logger.error(f"Failed to generate forecast chart for {symbol}: {e}")
        logger.error(traceback.format_exc())
        return jsonify({'error': str(e)}), 500

@app.route('/api/symbols', methods=['GET'])
def get_symbols():
    try:
        cluster, session = connect_to_cassandra()
        symbols = get_available_symbols(session)
        cluster.shutdown()
        return jsonify({'symbols': symbols})
    except Exception as e:
        logger.error(f"Failed to get symbols: {e}")
        logger.error(traceback.format_exc())
        return jsonify({'error': str(e)}), 500

@app.route('/api/price/<symbol>', methods=['GET'])
def get_price(symbol):
    try:
        cluster, session = connect_to_cassandra()
        price_data = get_symbol_price(session, symbol)
        cluster.shutdown()
        return jsonify(price_data)
    except Exception as e:
        logger.error(f"Failed to get price for {symbol}: {e}")
        logger.error(traceback.format_exc())
        return jsonify({'error': str(e)}), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy'})

if __name__ == '__main__':
    logger.info("Starting visualization service...")
    server = WSGIServer(('0.0.0.0', 5004), app)
    server.serve_forever() 