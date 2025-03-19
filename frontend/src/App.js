import { Routes, Route } from "react-router-dom";
import Navbar from "./components/Navbar";
import Home from "./pages/Home";
import Symbols from "./pages/Symbols";
import Forecast from "./pages/ForecastPage";

export default function App() {
    return (
        <div>
            <Navbar />
            <Routes>
                <Route path="/" element={<Home />} />
                <Route path="/symbols" element={<Symbols />} />
                <Route path="/forecast/:symbol" element={<Forecast />} />
            </Routes>
        </div>
    );
}
