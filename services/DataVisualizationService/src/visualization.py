from flask import Flask, request, render_template_string
from cassandra.cluster import Cluster
from datetime import datetime, timedelta
import cassandra.util
import matplotlib.pyplot as plt
import pandas as pd
from io import BytesIO
import base64

app = Flask(__name__)

#Data од Cassandra
def get_data_from_cassandra(session, symbol, time_frame=None):
    query = "SELECT * FROM analysis_data WHERE symbol = %s"
    rows = session.execute(query, [symbol])

    data = []
    for row in rows:
        date = row.date
        #мора у стринг конверт не ги зима како објект
        if isinstance(date, cassandra.util.Date):
            date = str(date)

        year, month, day = map(int, date.split('-'))
        date = datetime(year, month, day)

        # Филтрирање по време
        if time_frame:
            current_date = datetime.now()
            if time_frame == '7d':
                # Последни 7 дена
                if current_date - date > timedelta(days=7):
                    continue
            elif time_frame == '1m':
                # Последен месец
                if current_date - date > timedelta(days=30):
                    continue
            elif time_frame == '1y':
                # Последна година
                if current_date - date > timedelta(days=365):
                    continue

        linear_regression_predictions = row.linear_regression_predictions
        rf_forecast = row.rf_forecast
        svm_forecast = row.svm_forecast

        for i in range(len(linear_regression_predictions)):
            data.append({
                'date': date,
                'linear_regression_predictions': linear_regression_predictions[i],
                'rf_forecast': rf_forecast[i],
                'svm_forecast': svm_forecast[i],
            })

    return data


def create_plot(symbol, data):
    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['date'])

    plt.figure(figsize=(10, 6))
    plt.plot(df['date'], df['linear_regression_predictions'], label='Linear Regression', color='blue')
    plt.plot(df['date'], df['rf_forecast'], label='Random Forest', color='red')
    plt.plot(df['date'], df['svm_forecast'], label='SVM', color='green')

    plt.xlabel('Date')
    plt.ylabel('Forecast Values')
    plt.title(f'Forecast for {symbol}')
    plt.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()

    img = BytesIO()
    plt.savefig(img, format='png')
    img.seek(0)
    img_base64 = base64.b64encode(img.getvalue()).decode('utf-8')

    return img_base64


@app.route('/forecast')
def forecast():
    symbol = request.args.get('symbol', default='AAPL', type=str)
    time_frame = request.args.get('time_frame', default='7d', type=str)

    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('financial_data')

    data = get_data_from_cassandra(session, symbol, time_frame)
    img_base64 = create_plot(symbol, data)

    return render_template_string("""
        <html>
            <head>
                <title>Forecast Graph</title>
            </head>
            <body>
                <h1>Forecast Graph for {{ symbol }}</h1>
                <form method="get" action="/forecast">
                    Symbol: <input type="text" name="symbol" value="{{ symbol }}">
                    Time Frame:
                    <select name="time_frame">
                        <option value="7d" {% if time_frame == '7d' %}selected{% endif %}>Last 7 Days</option>
                        <option value="1m" {% if time_frame == '1m' %}selected{% endif %}>Last 1 Month</option>
                        <option value="1y" {% if time_frame == '1y' %}selected{% endif %}>Last 1 Year</option>
                    </select>
                    <input type="submit" value="Generate Graph">
                </form>
                <h2>Graph:</h2>
                <img src="data:image/png;base64,{{ img_base64 }}" alt="Forecast Graph"/>
            </body>
        </html>
    """, symbol=symbol, time_frame=time_frame, img_base64=img_base64)

if __name__ == '__main__':
    app.run(debug=True)
