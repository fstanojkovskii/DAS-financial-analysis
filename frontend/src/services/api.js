import axios from "axios";

const API_URL = "http://127.0.0.1:5000/api";


export async function fetchSymbols() {
    const response = await axios.get(`${API_URL}/symbols`);
    return response.data.symbols;
}

export async function fetchForecast(symbol, timeFrame) {
    const response = await axios.get(`${API_URL}/forecast`, {
        params: { symbol, time_frame: timeFrame },
    });
    return response.data;
}
