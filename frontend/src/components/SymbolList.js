import { useEffect, useState } from "react";
import { fetchSymbols } from "../services/api";
import { Link } from "react-router-dom";
import './css/SymbolList.css';

export default function SymbolList() {
    const [symbols, setSymbols] = useState([]);
    const [filteredSymbols, setFilteredSymbols] = useState([]);
    const [error, setError] = useState(null);
    const [searchTerm, setSearchTerm] = useState('');
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        const loadSymbols = async () => {
            try {
                setLoading(true);
                const symbols = await fetchSymbols();
                if (Array.isArray(symbols)) {
                    setSymbols(symbols);
                    setFilteredSymbols(symbols);
                } else {
                    setError('No symbols data received');
                }
            } catch (err) {
                setError('Error loading symbols');
                console.error('Error:', err);
            } finally {
                setLoading(false);
            }
        };

        loadSymbols();
    }, []);

    useEffect(() => {
        const filtered = symbols.filter(symbol =>
            symbol.toLowerCase().includes(searchTerm.toLowerCase())
        );
        setFilteredSymbols(filtered);
    }, [searchTerm, symbols]);

    const handleSearch = (e) => {
        setSearchTerm(e.target.value);
    };

    if (loading) {
        return (
            <div className="symbol-list-container">
                <div className="loading-spinner">
                    <div className="spinner"></div>
                    <p>Loading symbols...</p>
                </div>
            </div>
        );
    }

    return (
        <div className="symbol-list-container">
            <h2 className="symbol-list-title">Stock Symbols</h2>
            <div className="search-container">
                <input
                    type="text"
                    placeholder="Search symbols..."
                    value={searchTerm}
                    onChange={handleSearch}
                    className="search-input"
                />
                <button className="search-button">
                    <i className="fas fa-search"></i>
                </button>
            </div>
            {error ? (
                <p className="error-message">{error}</p>
            ) : (
                <div className="symbol-grid">
                    {filteredSymbols.length > 0 ? (
                        filteredSymbols.map((symbol, index) => (
                            <Link 
                                key={index} 
                                to={`/forecast/${symbol}`} 
                                className="symbol-card"
                            >
                                <div className="symbol-content">
                                    <span className="symbol-text">{symbol}</span>
                                    <i className="fas fa-chart-line"></i>
                                </div>
                            </Link>
                        ))
                    ) : (
                        <div className="no-results">
                            <i className="fas fa-search"></i>
                            <p>No symbols found matching your search</p>
                        </div>
                    )}
                </div>
            )}
        </div>
    );
}
