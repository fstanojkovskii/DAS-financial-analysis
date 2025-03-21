from flask import Flask, request, jsonify
from cassandra.cluster import Cluster
from datetime import datetime, timedelta
import cassandra.util
import matplotlib.pyplot as plt
import pandas as pd
from io import BytesIO
import base64
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

def get_data_from_cassandra(session, symbol, time_frame=None):
    query = "SELECT * FROM analysis_data WHERE symbol = %s"
    rows = session.execute(query, [symbol])

    forecast_data = []
    for row in rows:
        date = row.date
        if isinstance(date, cassandra.util.Date):
            date = str(date)

        year, month, day = map(int, date.split('-'))
        date = datetime(year, month, day)

        if time_frame:
            current_date = datetime.now()
            if time_frame == '1m' and current_date - date > timedelta(days=30):
                continue
            elif time_frame == '1y' and current_date - date > timedelta(days=365):
                continue
            elif time_frame == '3y' and current_date - date > timedelta(days=365 * 3):
                continue

        forecast_data.append({
            'date': date.strftime('%Y-%m-%d'),
            'linear_regression_predictions': row.linear_regression_predictions[:10],
            'rf_forecast': row.rf_forecast[:10],
            'svm_forecast': row.svm_forecast[:10],
            'dt_forecast': row.dt_forecast[:10]
        })

    return forecast_data

def get_symbol_price(session, symbol):
    query = "SELECT * FROM stock_prices WHERE symbol = %s ORDER BY date DESC LIMIT 1"
    row = session.execute(query, [symbol]).one()

    if row:
        return {
            'symbol': symbol,
            'close_price': row.close if row.close else 'N/A',
            'price_change': row.price_change if row.price_change else 'N/A',
            'open_price': row.open if row.open else 'N/A'
        }
    return {}

def create_plot(symbol, data):
    if not data:
        return None

    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['date'])


    numeric_cols = ['linear_regression_predictions', 'rf_forecast', 'svm_forecast', 'dt_forecast']
    for col in numeric_cols:
        df[col] = df[col].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else None).astype(float)

    df = df.dropna()

    if df.empty:
        return None

    plt.figure(figsize=(10, 6))
    plt.plot(df['date'], df['linear_regression_predictions'], label='Linear Regression', color='blue')
    plt.plot(df['date'], df['rf_forecast'], label='Random Forest', color='red')
    plt.plot(df['date'], df['svm_forecast'], label='SVM', color='green')
    plt.plot(df['date'], df['dt_forecast'], label='Decision Tree', color='purple')

    plt.xlabel('Date')
    plt.ylabel('Forecast Values')
    plt.title(f'Forecast for {symbol}')
    plt.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()

    img = BytesIO()
    plt.savefig(img, format='png', dpi=100)
    img.seek(0)
    img_base64 = base64.b64encode(img.getvalue()).decode('utf-8')

    return img_base64

@app.route('/api/forecast', strict_slashes=False)
def forecast_api():
    symbol = request.args.get('symbol', default='AAPL', type=str)
    time_frame = request.args.get('time_frame', default='1m', type=str)

    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('financial_data')

    forecast_data = get_data_from_cassandra(session, symbol, time_frame)
    price_data = get_symbol_price(session, symbol)

    img_base64 = create_plot(symbol, forecast_data) if forecast_data else None

    return jsonify({
        "symbol": symbol,
        "time_frame": time_frame,
        "forecast_data": forecast_data,
        "price_data": price_data,
        "img_base64": img_base64
    })

@app.route('/api/symbols', methods=['GET'])
def get_symbols():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('financial_data')

    query = "SELECT DISTINCT symbol FROM analysis_data"
    rows = session.execute(query)

    symbols = [row.symbol for row in rows]

    return jsonify({"symbols": symbols})

if __name__ == '__main__':
    app.run(debug=True)
