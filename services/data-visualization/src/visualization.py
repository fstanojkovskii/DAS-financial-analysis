from cassandra.cluster import Cluster
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
import mplfinance as mpf
from io import BytesIO
import base64
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def connect_to_cassandra():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('financial_data')
    return cluster, session


def get_historical_data(session, symbol, timeframe='1y'):
    try:
        query = "SELECT date, open, high, low, close, volume FROM stock_prices WHERE symbol = %s ORDER BY date ASC"
        rows = session.execute(query, [symbol])

        data = []
        for row in rows:
            try:
                date = datetime.strptime(row.date, '%Y-%m-%d')
            except:
                date = row.date

            data.append({
                'Date': date,
                'Open': float(row.open),
                'High': float(row.high),
                'Low': float(row.low),
                'Close': float(row.close),
                'Volume': float(row.volume)
            })

        df = pd.DataFrame(data)
        if df.empty:
            return None

        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)

        if timeframe == '1m':
            df = df.last('30D')
        elif timeframe == '1y':
            df = df.last('365D')
        elif timeframe == '3y':
            df = df.last('1095D')

        return df
    except Exception as e:
        logger.error(f"Error retrieving historical data: {e}")
        return None


def get_forecast_data(session, symbol):
    try:
        query = "SELECT date, linear_regression_predictions, rf_forecast, svm_forecast, dt_forecast FROM analysis_data WHERE symbol = %s"
        rows = session.execute(query, [symbol])

        data = []
        for row in rows:
            predictions = {
                'date': row.date,
                'linear_regression': row.linear_regression_predictions[
                    0] if row.linear_regression_predictions else None,
                'random_forest': row.rf_forecast[0] if row.rf_forecast else None,
                'svm': row.svm_forecast[0] if row.svm_forecast else None,
                'decision_tree': row.dt_forecast[0] if row.dt_forecast else None
            }
            data.append(predictions)

        df = pd.DataFrame(data)
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        return df.dropna()
    except Exception as e:
        logger.error(f"Error retrieving forecast data: {e}")
        return None


def create_line_chart(df, symbol):
    try:
        plt.figure(figsize=(10, 6))
        plt.plot(df.index, df['Close'], label='Close Price', color='blue')
        plt.title(f'{symbol} Closing Prices')
        plt.xlabel('Date')
        plt.ylabel('Price')
        plt.legend()
        plt.xticks(rotation=45)
        plt.tight_layout()

        img = BytesIO()
        plt.savefig(img, format='png', dpi=100)
        img.seek(0)
        plt.close()
        return base64.b64encode(img.read()).decode('utf-8')
    except Exception as e:
        logger.error(f"Error creating line chart: {e}")
        return None


def create_candlestick_chart(df, symbol):
    try:
        img = BytesIO()
        mpf.plot(df, type='candle', style='charles', title=f'{symbol} Candlestick Chart',
                 ylabel='Price', volume=True, figsize=(10, 6), savefig=dict(fname=img, format='png', dpi=100))
        img.seek(0)
        return base64.b64encode(img.read()).decode('utf-8')
    except Exception as e:
        logger.error(f"Error creating candlestick chart: {e}")
        return None


def create_forecast_chart(historical_df, forecast_df, symbol):
    try:
        plt.figure(figsize=(10, 6))
        plt.plot(historical_df.index, historical_df['Close'], label='Historical Close', color='blue')
        plt.plot(forecast_df.index, forecast_df['linear_regression'], label='Linear Regression Forecast',
                 linestyle='--')
        plt.plot(forecast_df.index, forecast_df['random_forest'], label='Random Forest Forecast', linestyle='--')
        plt.plot(forecast_df.index, forecast_df['svm'], label='SVM Forecast', linestyle='--')
        plt.plot(forecast_df.index, forecast_df['decision_tree'], label='Decision Tree Forecast', linestyle='--')

        plt.title(f'{symbol} Price Forecast')
        plt.xlabel('Date')
        plt.ylabel('Price')
        plt.legend()
        plt.xticks(rotation=45)
        plt.tight_layout()

        img = BytesIO()
        plt.savefig(img, format='png', dpi=100)
        img.seek(0)
        plt.close()
        return base64.b64encode(img.read()).decode('utf-8')
    except Exception as e:
        logger.error(f"Error creating forecast chart: {e}")
        return None


def get_available_symbols(session):
    try:
        query_analysis = "SELECT DISTINCT symbol FROM analysis_data"
        query_prices = "SELECT DISTINCT symbol FROM stock_prices"
        symbols_analysis = {row.symbol for row in session.execute(query_analysis)}
        symbols_prices = {row.symbol for row in session.execute(query_prices)}
        return sorted(list(symbols_analysis.union(symbols_prices)))
    except Exception as e:
        logger.error(f"Error retrieving symbols: {e}")
        return []


def get_symbol_price(session, symbol):
    try:
        query = "SELECT close, price_change, open FROM stock_prices WHERE symbol = %s ORDER BY date DESC LIMIT 1"
        row = session.execute(query, [symbol]).one()
        return {
            'close': row.close,
            'price_change': row.price_change,
            'open': row.open
        } if row else {}
    except Exception as e:
        logger.error(f"Error retrieving symbol price: {e}")
        return {}