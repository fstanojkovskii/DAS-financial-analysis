import React, { useState, useEffect } from 'react';
import { useParams } from 'react-router-dom';
import { fetchForecast } from '../services/api';
import { Line } from 'react-chartjs-2';
import 'chart.js/auto';
import './css/Forecast.css';

function ForecastPage() {
    const { symbol } = useParams();
    const [forecastData, setForecastData] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState('');
    const [timeFrame, setTimeFrame] = useState('1m');

    useEffect(() => {
        const fetchForecastData = async () => {
            setLoading(true);
            setError('');
            setForecastData(null);

            try {
                const data = await fetchForecast(symbol, timeFrame);
                if (!data || Object.keys(data).length === 0) {
                    setError('No forecast data available.');
                } else {
                    setForecastData(data);
                }
            } catch (err) {
                setError('Error fetching forecast data.');
                console.error(err);
            } finally {
                setLoading(false);
            }
        };

        fetchForecastData();
    }, [symbol, timeFrame]);

    const handleTimeFrameChange = (e) => {
        setTimeFrame(e.target.value);
    };

    const chartData = forecastData && {
        labels: forecastData.forecast_data.map(d => d.date),
        datasets: [
            { label: 'Linear Regression', data: forecastData.forecast_data.map(d => d.linear_regression_predictions[0]), borderColor: 'blue' },
            { label: 'Random Forest', data: forecastData.forecast_data.map(d => d.rf_forecast[0]), borderColor: 'red' },
            { label: 'SVM', data: forecastData.forecast_data.map(d => d.svm_forecast[0]), borderColor: 'green' },
            { label: 'Decision Tree', data: forecastData.forecast_data.map(d => d.dt_forecast[0]), borderColor: 'purple' },
        ]
    };

    return (
        <div className="forecast-page-container">
            <h1>{symbol} Price Forecast</h1>


            <select value={timeFrame} onChange={handleTimeFrameChange}>
                <option value="1m">Last 1 Month</option>
                <option value="1y">Last 1 Year</option>
                <option value="3y">Last 3 Years</option>
            </select>

            {loading && <p>Loading...</p>}
            {error && <p>{error}</p>}


            {forecastData && (
                <div className="stock-info">
                    <h2>Stock Information</h2>
                    <p>Close Price: ${forecastData.price_data.close_price}</p>
                    <p>Open Price: ${forecastData.price_data.open_price}</p>
                    <p>Price Change: {forecastData.price_data.price_change}%</p>
                </div>
            )}

            {forecastData && <Line data={chartData} />}
        </div>
    );
}

export default ForecastPage;