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

# Fetch forecast data from Cassandra
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
            elif time_frame == '3y' and current_date - date > timedelta(days=365*3):  # Handle 3 years case
                continue


        linear_regression_predictions = row.linear_regression_predictions
        rf_forecast = row.rf_forecast
        svm_forecast = row.svm_forecast
        dt_forecast = row.dt_forecast
        linear_svr_forecast = row.linear_svr_forecast

        for i in range(len(linear_regression_predictions)):
            forecast_data.append({
                'date': date,
                'linear_regression_predictions': linear_regression_predictions[i],
                'rf_forecast': rf_forecast[i],
                'svm_forecast': svm_forecast[i],
                'dt_forecast': dt_forecast[i],
                'linear_svr_forecast': linear_svr_forecast[i],
            })

    return forecast_data

def get_symbol_price(session, symbol):
    query = "SELECT * FROM stock_prices WHERE symbol = %s ORDER BY date DESC LIMIT 1"
    row = session.execute(query, [symbol]).one()

    if row:
        return {
            'symbol': symbol,
            'close_price': row.close,
            'price_change': row.price_change,
            'open_price': row.open
        }
    return {}

# Create plot
def create_plot(symbol, data):
    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['date'])

    plt.figure(figsize=(10, 6))
    plt.plot(df['date'], df['linear_regression_predictions'], label='Linear Regression', color='blue')
    plt.plot(df['date'], df['rf_forecast'], label='Random Forest', color='red')
    plt.plot(df['date'], df['svm_forecast'], label='SVM', color='green')
    plt.plot(df['date'], df['dt_forecast'], label='Decision Tree', color='purple')
    plt.plot(df['date'], df['linear_svr_forecast'], label='Linear SVR', color='orange')

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

# Forecast API with symbol details
@app.route('/api/forecast', strict_slashes=False)
def forecast_api():
    symbol = request.args.get('symbol', default='AAPL', type=str)
    time_frame = request.args.get('time_frame', default='7d', type=str)

    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('financial_data')


    forecast_data = get_data_from_cassandra(session, symbol, time_frame)


    price_data = get_symbol_price(session, symbol)

    img_base64 = create_plot(symbol, forecast_data)

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
