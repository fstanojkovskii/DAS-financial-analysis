import { useEffect, useState } from "react";
import { fetchSymbols } from "../services/api";
import { Link } from "react-router-dom";
import './css/SymbolList.css';

export default function SymbolList() {
    const [symbols, setSymbols] = useState([]);

    useEffect(() => {

        fetchSymbols().then((symbolsData) => {
            setSymbols(symbolsData);
        });
    }, []);

    return (
        <div className="symbol-list-container">
            <h2 className="symbol-list-title">Stock Symbols</h2>
            <ul className="symbol-list">
                {symbols.length > 0 ? (
                    symbols.map((symbol, index) => (
                        <li key={index} className="symbol-item">
                            <Link to={`/forecast/${symbol}`} className="symbol-link">
                                {symbol}
                            </Link>
                        </li>
                    ))
                ) : (
                    <p>No symbols available</p>
                )}
            </ul>
        </div>
    );
}
