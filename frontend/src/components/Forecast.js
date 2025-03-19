import React, { useState, useEffect } from 'react';
import { useParams } from 'react-router-dom';
import { fetchForecast } from '../services/api';
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
            try {
                const data = await fetchForecast(symbol, timeFrame);
                setForecastData(data);
            } catch (err) {
                setError('Error fetching forecast data');
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

    return (
        <div className="forecast-page-container">
            <h1 className="header">{symbol} Price Forecast</h1>

            <div className="timeframe-selector">
                <label htmlFor="timeframe">Select Time Frame: </label>
                <select
                    id="timeframe"
                    value={timeFrame}
                    onChange={handleTimeFrameChange}
                >
                    <option value="1m">Last 1 Month</option>
                    <option value="1y">Last 1 Year</option>
                    <option value="3y">Last 3 Years</option>
                </select>
            </div>

            {loading && <p>Loading forecast...</p>}

            {error && <p>{error}</p>}

            {forecastData && (
                <div className="forecast-results">
                    <h2>Stock Information</h2>
                    <p>Close Price: ${forecastData.price_data.close_price}</p>
                    <p>Open Price: ${forecastData.price_data.open_price}</p>
                    <p>Price Change: {forecastData.price_data.price_change}%</p>

                    <h2>Forecast</h2>
                    <img
                        src={`data:image/png;base64,${forecastData.img_base64}`}
                        alt="Stock Forecast"
                        className="forecast-image"
                    />
                </div>
            )}
        </div>
    );
}

export default ForecastPage;
