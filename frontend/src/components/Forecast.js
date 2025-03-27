import React, { useState, useEffect } from 'react';
import { useParams } from 'react-router-dom';
import { 
    fetchForecast, 
    startAnalysis, 
    checkAnalysisStatus, 
    getAnalysisData,
    startSparkStreaming,
    stopSparkStreaming,
    getSparkStatus,
    fetchChart,
    fetchForecastChart,
    fetchCurrentPrice,
    startDataIngestion,
    checkIngestionStatus
} from '../services/api';
import { Line } from 'react-chartjs-2';
import 'chart.js/auto';
import './css/Forecast.css';

function ForecastPage() {
    const { symbol } = useParams();
    const [forecastData, setForecastData] = useState(null);
    const [historicalData, setHistoricalData] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState('');
    const [timeFrame, setTimeFrame] = useState('1y');
    const [chartType, setChartType] = useState('forecast');
    const [analysisStatus, setAnalysisStatus] = useState('stopped');
    const [analysisLoading, setAnalysisLoading] = useState(false);
    const [analysisError, setAnalysisError] = useState('');
    const [sparkStatus, setSparkStatus] = useState('stopped');
    const [sparkLoading, setSparkLoading] = useState(false);
    const [sparkError, setSparkError] = useState('');
    const [priceData, setPriceData] = useState(null);
    const [ingestionStatus, setIngestionStatus] = useState('stopped');
    const [ingestionLoading, setIngestionLoading] = useState(false);
    const [ingestionError, setIngestionError] = useState('');
    const [showIngestionModal, setShowIngestionModal] = useState(false);

    useEffect(() => {
        const fetchData = async () => {
            setLoading(true);
            setError('');
            setForecastData(null);
            setHistoricalData(null);
            setPriceData(null);

            try {
                console.log('Fetching data for symbol:', symbol);
                
                const priceResponse = await fetchCurrentPrice(symbol);
                setPriceData(priceResponse);

                if (chartType === 'forecast') {
                    const forecastResponse = await fetchForecastChart(symbol);
                    setForecastData(forecastResponse);
                } else {
                    const chartResponse = await fetchChart(symbol, chartType, timeFrame);
                    setHistoricalData(chartResponse);
                }
            } catch (err) {
                console.error('Error in fetchData:', err);
                setError('Error fetching data. Please try again later.');
            } finally {
                setLoading(false);
            }
        };

        if (symbol) {
            fetchData();
        }
    }, [symbol, timeFrame, chartType]);

    useEffect(() => {
        const checkSparkStatus = async () => {
            try {
                const status = await getSparkStatus();
                setSparkStatus(status.status);
            } catch (err) {
                console.error('Error checking Spark status:', err);
            }
        };

        const statusInterval = setInterval(checkSparkStatus, 5000);
        checkSparkStatus();

        return () => clearInterval(statusInterval);
    }, []);

    useEffect(() => {
        const checkStatus = async () => {
            try {
                const status = await checkIngestionStatus(symbol);
                setIngestionStatus(status.status);
                if (status.status === 'completed') {
                    const priceResponse = await fetchCurrentPrice(symbol);
                    setPriceData(priceResponse);
                    if (chartType === 'forecast') {
                        const forecastResponse = await fetchForecastChart(symbol);
                        setForecastData(forecastResponse);
                    } else {
                        const chartResponse = await fetchChart(symbol, chartType, timeFrame);
                        setHistoricalData(chartResponse);
                    }
                }
            } catch (err) {
                console.error('Error checking ingestion status:', err);
            }
        };

        if (ingestionStatus === 'running') {
            const statusInterval = setInterval(checkStatus, 5000);
            return () => clearInterval(statusInterval);
        }
    }, [ingestionStatus, symbol, chartType, timeFrame]);

    const handleTimeFrameChange = (e) => {
        setTimeFrame(e.target.value);
    };

    const handleChartTypeChange = (type) => {
        setChartType(type);
    };

    const handleStartAnalysis = async () => {
        setAnalysisLoading(true);
        setAnalysisError('');
        try {
            const response = await startAnalysis(symbol, new Date().toISOString().split('T')[0]);
            if (response.status === 'success') {
                setAnalysisStatus('running');
                const statusInterval = setInterval(async () => {
                    const statusResponse = await checkAnalysisStatus();
                    if (statusResponse.status === 'stopped') {
                        setAnalysisStatus('stopped');
                        clearInterval(statusInterval);
                        const analysisData = await getAnalysisData(symbol, new Date().toISOString().split('T')[0]);
                        if (analysisData) {
                            setForecastData(analysisData);
                        }
                    }
                }, 5000);
            }
        } catch (err) {
            setAnalysisError('Failed to start analysis');
            console.error(err);
        } finally {
            setAnalysisLoading(false);
        }
    };

    const handleSparkControl = async (action) => {
        setSparkLoading(true);
        setSparkError('');
        try {
            const response = await (action === 'start' ? startSparkStreaming() : stopSparkStreaming());
            if (response.status === 'success') {
                setSparkStatus(action === 'start' ? 'running' : 'stopped');
            }
        } catch (err) {
            setSparkError(`Failed to ${action} Spark streaming`);
            console.error(err);
        } finally {
            setSparkLoading(false);
        }
    };

    const handleStartIngestion = async () => {
        setIngestionLoading(true);
        setIngestionError('');
        try {
            const response = await startDataIngestion(symbol);
            if (response.status === 'success') {
                setIngestionStatus('running');
                setShowIngestionModal(true);
            }
        } catch (err) {
            setIngestionError('Failed to start data ingestion');
            console.error(err);
        } finally {
            setIngestionLoading(false);
        }
    };

    const renderChart = () => {
        if (loading) {
            return (
                <div className="loading-spinner">
                    <div className="spinner"></div>
                    <p>Loading data...</p>
                </div>
            );
        }

        if (error) {
            return <div className="error-message">{error}</div>;
        }

        switch (chartType) {
            case 'forecast':
                if (!forecastData?.chart) {
                    return (
                        <div className="info-message">
                            Analysis data not available. Please run the analysis first.
                        </div>
                    );
                }
                return (
                    <div className="chart-container">
                        <img 
                            src={`data:image/png;base64,${forecastData.chart}`}
                            alt="Forecast Chart"
                            className="chart-image"
                        />
                    </div>
                );
            case 'line':
                if (!historicalData?.chart) {
                    return (
                        <div className="info-message">
                            Line chart data not available for this symbol.
                        </div>
                    );
                }
                return (
                    <div className="chart-container">
                        <img 
                            src={`data:image/png;base64,${historicalData.chart}`}
                            alt="Stock Price Line Chart"
                            className="chart-image"
                        />
                    </div>
                );
            case 'candlestick':
                if (!historicalData?.chart) {
                    return (
                        <div className="info-message">
                            Candlestick chart data not available for this symbol.
                        </div>
                    );
                }
                return (
                    <div className="chart-container">
                        <img 
                            src={`data:image/png;base64,${historicalData.chart}`}
                            alt="Stock Price Candlestick Chart"
                            className="chart-image"
                        />
                    </div>
                );
            default:
                return null;
        }
    };

    const renderIngestionModal = () => {
        if (!showIngestionModal) return null;

        return (
            <div className="modal-overlay">
                <div className="modal-content">
                    <h3>Data Ingestion in Progress</h3>
                    <div className="ingestion-progress">
                        <div className="progress-spinner"></div>
                        <p>{ingestionStatus === 'running' ? 'Fetching historical data...' : 'Processing data...'}</p>
                    </div>
                    <button 
                        className="close-modal-btn"
                        onClick={() => setShowIngestionModal(false)}
                    >
                        Close
                    </button>
                </div>
            </div>
        );
    };

    return (
        <div className="forecast-page-container">
            <div className="header-section">
                <h1>{symbol} Price Forecast</h1>
                <div className="controls-group">
                    <div className="ingestion-controls">
                        <button 
                            className={`ingestion-btn ${ingestionStatus === 'running' ? 'running' : ''}`}
                            onClick={handleStartIngestion}
                            disabled={ingestionLoading || ingestionStatus === 'running'}
                        >
                            {ingestionLoading ? 'Starting Ingestion...' : 
                             ingestionStatus === 'running' ? 'Ingestion Running...' : 
                             'Fetch Historical Data'}
                        </button>
                        {ingestionError && <p className="error-message">{ingestionError}</p>}
                    </div>
                    <div className="analysis-controls">
                        <button 
                            className={`analysis-btn ${analysisStatus === 'running' ? 'running' : ''}`}
                            onClick={handleStartAnalysis}
                            disabled={analysisLoading || analysisStatus === 'running'}
                        >
                            {analysisLoading ? 'Starting Analysis...' : 
                             analysisStatus === 'running' ? 'Analysis Running...' : 
                             'Start Analysis'}
                        </button>
                        {analysisError && <p className="error-message">{analysisError}</p>}
                    </div>
                    <div className="spark-controls">
                        <button 
                            className={`spark-btn ${sparkStatus === 'running' ? 'running' : ''}`}
                            onClick={() => handleSparkControl(sparkStatus === 'running' ? 'stop' : 'start')}
                            disabled={sparkLoading}
                        >
                            {sparkLoading ? 'Processing...' : 
                             sparkStatus === 'running' ? 'Stop Spark Streaming' : 
                             'Start Spark Streaming'}
                        </button>
                        {sparkError && <p className="error-message">{sparkError}</p>}
                    </div>
                </div>
            </div>

            <div className="controls-container">
                <div className="timeframe-selector">
                    <label>Time Frame:</label>
                    <select value={timeFrame} onChange={handleTimeFrameChange}>
                        <option value="1m">Last 1 Month</option>
                        <option value="1y">Last 1 Year</option>
                        <option value="3y">Last 3 Years</option>
                    </select>
                </div>

                <div className="chart-type-buttons">
                    <button 
                        className={`chart-type-btn ${chartType === 'forecast' ? 'active' : ''}`}
                        onClick={() => handleChartTypeChange('forecast')}
                    >
                        Forecast
                    </button>
                    <button 
                        className={`chart-type-btn ${chartType === 'line' ? 'active' : ''}`}
                        onClick={() => handleChartTypeChange('line')}
                    >
                        Price Line
                    </button>
                    <button 
                        className={`chart-type-btn ${chartType === 'candlestick' ? 'active' : ''}`}
                        onClick={() => handleChartTypeChange('candlestick')}
                    >
                        Candlestick
                    </button>
                </div>
            </div>

            {priceData && (
                <div className="stock-info">
                    <h2>Stock Information</h2>
                    <div className="stock-info-grid">
                        <div className="info-item">
                            <span className="label">Close Price:</span>
                            <span className="value">${priceData.close}</span>
                        </div>
                        <div className="info-item">
                            <span className="label">Open Price:</span>
                            <span className="value">${priceData.open}</span>
                        </div>
                        <div className="info-item">
                            <span className="label">Price Change:</span>
                            <span className={`value ${priceData.price_change >= 0 ? 'positive' : 'negative'}`}>
                                {priceData.price_change}%
                            </span>
                        </div>
                    </div>
                </div>
            )}

            <div className="chart-section">
                {renderChart()}
            </div>

            {renderIngestionModal()}
        </div>
    );
}

export default ForecastPage;