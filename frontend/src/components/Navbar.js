import { Link } from "react-router-dom";
import './css/Navbar.css';

export default function Navbar() {
    return (
        <nav className="navbar">
            <div className="navbar-container">
                <Link to="/" className="navbar-logo">Home</Link>
                <Link to="/symbols" className="navbar-link">Symbols</Link>
            </div>
        </nav>
    );
}
