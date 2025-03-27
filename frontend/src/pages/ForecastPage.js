import React from 'react';
import Forecast from '../components/Forecast';
import './css/ForecastPage.css';

function ForecastPage() {
    return (
        <div className="forecast-page-container">
            <h1 className="page-header">Stock Price Forecast</h1>
            <div className="forecast-page-content">
                <Forecast />
            </div>
        </div>
    );
}

export default ForecastPage;
