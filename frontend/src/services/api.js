import axios from "axios";

const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || 'http://localhost:5004/api';
const ANALYSIS_API_URL = process.env.REACT_APP_ANALYSIS_API_URL || 'http://localhost:5003/api';
const PROCESSING_API_URL = process.env.REACT_APP_PROCESSING_API_URL || 'http://localhost:5002/api';
const INGESTION_API_URL = process.env.REACT_APP_INGESTION_API_URL || 'http://localhost:5001/api';

export const fetchForecast = async (symbol, timeFrame, endpoint = 'analysis_data') => {
    try {
        console.log(`Fetching ${endpoint} for ${symbol} with timeframe ${timeFrame}`);
        const response = await fetch(`${ANALYSIS_API_URL}/analysis/${endpoint}?symbol=${symbol}&time_frame=${timeFrame}`);
        if (!response.ok) {
            throw new Error(`Network response was not ok: ${response.status}`);
        }
        const data = await response.json();
        console.log(`Received ${endpoint} data:`, data);
        return data;
    } catch (error) {
        console.error(`Error fetching ${endpoint} data:`, error);
        throw error;
    }
};

export const fetchSymbols = async () => {
    try {
        const response = await axios.get(`${INGESTION_API_URL}/api/symbols`);
        return response.data.symbols;
    } catch (error) {
        console.error('Error fetching symbols:', error);
        throw error;
    }
};

export async function fetchHistoricalData(symbol) {
    try {
        console.log(`Fetching historical data for ${symbol}`);
        const response = await axios.get(`${INGESTION_API_URL}/api/stock/${symbol}`);
        console.log('Received historical data:', response.data);
        return response.data;
    } catch (error) {
        console.error('Error fetching historical data:', error);
        throw error;
    }
}

// Analysis API endpoints
export const startAnalysis = async (symbol, date) => {
    try {
        const response = await fetch(`${ANALYSIS_API_URL}/analysis/start`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ symbol, date }),
        });
        if (!response.ok) {
            throw new Error('Network response was not ok');
        }
        return await response.json();
    } catch (error) {
        console.error('Error starting analysis:', error);
        throw error;
    }
};

export const checkAnalysisStatus = async () => {
    try {
        const response = await fetch(`${ANALYSIS_API_URL}/analysis/status`);
        if (!response.ok) {
            throw new Error('Network response was not ok');
        }
        return await response.json();
    } catch (error) {
        console.error('Error checking analysis status:', error);
        throw error;
    }
};

export const getAnalysisData = async (symbol, date) => {
    try {
        const response = await fetch(`${ANALYSIS_API_URL}/analysis/data/${symbol}?date=${date}`);
        if (!response.ok) {
            throw new Error('Network response was not ok');
        }
        return await response.json();
    } catch (error) {
        console.error('Error fetching analysis data:', error);
        throw error;
    }
};

// Spark Streaming API endpoints
export const startSparkStreaming = async () => {
    try {
        const response = await fetch(`${PROCESSING_API_URL}/api/spark/start`, {
            method: 'POST',
        });
        if (!response.ok) {
            throw new Error('Network response was not ok');
        }
        return await response.json();
    } catch (error) {
        console.error('Error starting Spark streaming:', error);
        throw error;
    }
};

export const stopSparkStreaming = async () => {
    try {
        const response = await fetch(`${PROCESSING_API_URL}/api/spark/stop`, {
            method: 'POST',
        });
        if (!response.ok) {
            throw new Error('Network response was not ok');
        }
        return await response.json();
    } catch (error) {
        console.error('Error stopping Spark streaming:', error);
        throw error;
    }
};

export const getSparkStatus = async () => {
    try {
        const response = await fetch(`${PROCESSING_API_URL}/api/spark/status`);
        if (!response.ok) {
            throw new Error('Network response was not ok');
        }
        return await response.json();
    } catch (error) {
        console.error('Error checking Spark status:', error);
        throw error;
    }
};

export const fetchChart = async (symbol, chartType = 'line', timeframe = '1y') => {
    try {
        console.log(`Fetching ${chartType} chart for ${symbol} with timeframe ${timeframe}`);
        const response = await fetch(`${API_BASE_URL}/api/chart/${symbol}?type=${chartType}&timeframe=${timeframe}`);
        if (!response.ok) {
            throw new Error(`Network response was not ok: ${response.status}`);
        }
        const data = await response.json();
        console.log(`Received chart data:`, data);
        return data;
    } catch (error) {
        console.error(`Error fetching chart data:`, error);
        throw error;
    }
};

export const fetchForecastChart = async (symbol) => {
    try {
        console.log(`Fetching forecast chart for ${symbol}`);
        const response = await fetch(`${API_BASE_URL}/api/forecast_chart/${symbol}`);
        if (!response.ok) {
            throw new Error(`Network response was not ok: ${response.status}`);
        }
        const data = await response.json();
        console.log(`Received forecast chart data:`, data);
        return data;
    } catch (error) {
        console.error(`Error fetching forecast chart data:`, error);
        throw error;
    }
};

export const fetchCurrentPrice = async (symbol) => {
    try {
        console.log(`Fetching current price for ${symbol}`);
        const response = await fetch(`${API_BASE_URL}/api/price/${symbol}`);
        if (!response.ok) {
            throw new Error(`Network response was not ok: ${response.status}`);
        }
        const data = await response.json();
        console.log(`Received price data:`, data);
        return data;
    } catch (error) {
        console.error(`Error fetching price data:`, error);
        throw error;
    }
};

export const startDataIngestion = async (symbol) => {
    try {
        console.log(`Starting data ingestion for ${symbol}`);
        const response = await fetch(`${INGESTION_API_URL}/api/ingestion/start`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ symbol }),
        });
        if (!response.ok) {
            throw new Error(`Network response was not ok: ${response.status}`);
        }
        return await response.json();
    } catch (error) {
        console.error('Error starting data ingestion:', error);
        throw error;
    }
};

export const checkIngestionStatus = async (symbol) => {
    try {
        console.log(`Checking ingestion status for ${symbol}`);
        const response = await fetch(`${INGESTION_API_URL}/api/ingestion/status/${symbol}`);
        if (!response.ok) {
            throw new Error(`Network response was not ok: ${response.status}`);
        }
        return await response.json();
    } catch (error) {
        console.error('Error checking ingestion status:', error);
        throw error;
    }
};