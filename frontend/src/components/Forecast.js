import React, { useState, useEffect } from 'react';
import axios from 'axios';
import './Forecast.css';
function Forecast() {
    const [symbols, setSymbols] = useState([]);
    const [symbol, setSymbol] = useState('');
    const [timeFrame, setTimeFrame] = useState('7d');
    const [imgBase64, setImgBase64] = useState('');
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState('');

    // Fetch available stock symbols from the backend
    useEffect(() => {
        const fetchSymbols = async () => {
            try {
                const response = await axios.get('http://127.0.0.1:5000/api/symbols');
                setSymbols(response.data.symbols);
                setSymbol(response.data.symbols[0]); // Set first symbol as default
            } catch (err) {
                console.error('Error fetching symbols:', err);
                setError('Failed to load symbols.');
            }
        };

        fetchSymbols();
    }, []);

    const handleSubmit = async (e) => {
        e.preventDefault();
        setLoading(true);
        setError('');

        try {
            const response = await axios.get('http://127.0.0.1:5000/api/forecast', {
                params: { symbol, time_frame: timeFrame }
            });
            setImgBase64(response.data.img_base64);
        } catch (err) {
            setError('Error fetching forecast data.');
            console.error(err);
        } finally {
            setLoading(false);
        }
    };

    return (
        <div className="forecast-container">
            <h1 className="header">Stock Price Forecast</h1>

            <div className="form-container">
                <form onSubmit={handleSubmit}>
                    <div className="form-grid">
                        <div className="form-group">
                            <label htmlFor="symbol-select">Stock Symbol</label>
                            <select
                                id="symbol-select"
                                className="select-style"
                                value={symbol}
                                onChange={(e) => setSymbol(e.target.value)}
                            >
                                {symbols.map((sym) => (
                                    <option key={sym} value={sym}>{sym}</option>
                                ))}
                            </select>
                        </div>

                        <div className="form-group">
                            <label htmlFor="timeframe-select">Time Frame</label>
                            <select
                                id="timeframe-select"
                                className="select-style"
                                value={timeFrame}
                                onChange={(e) => setTimeFrame(e.target.value)}
                            >
                                <option value="7d">Last 7 Days</option>
                                <option value="1m">Last 1 Month</option>
                                <option value="1y">Last 1 Year</option>
                            </select>
                        </div>

                        <div className="form-group">
                            <button type="submit" className="button-primary">
                                {loading ? 'Generating...' : 'Generate Forecast'}
                            </button>
                        </div>
                    </div>
                </form>
            </div>

            {error && (
                <div className="error-message">
                    {error}
                </div>
            )}

            {loading && (
                <div className="loading-spinner">
                    <div className="spinner"></div>
                    <p>Generating forecast...</p>
                </div>
            )}

            {imgBase64 && (
                <div className="graph-container">
                    <h2 className="graph-title">{symbol} Price Forecast</h2>
                    <img
                        src={`data:image/png;base64,${imgBase64}`}
                        alt="Forecast Graph"
                        className="graph-image"
                    />
                </div>
            )}
        </div>
    );
}

export default Forecast;
