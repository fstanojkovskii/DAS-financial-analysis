import React from 'react';
import './css/Home.css';

export default function Home() {
    return (
        <div className="home-container">
            <h1 className="home-title">Welcome to the Stock Market Forecast</h1>
            <p className="home-description">
                Stay updated with the latest stock market trends and forecasts.
                Select a stock symbol to view its price forecast, explore trends, and make informed decisions.
            </p>
            <div className="cta-container">
                <p className="cta-text">Get started by browsing available stock symbols and forecasts.</p>
                <button className="cta-button">
                    <a href="/symbols" className="cta-link">Browse Symbols</a>
                </button>
            </div>
        </div>
    );
}
