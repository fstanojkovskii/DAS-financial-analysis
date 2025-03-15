import React, { useEffect, useState } from "react";
import axios from "axios";
import Forecast from "./components/Forecast";


    function App() {
        return (
            <div className="App">
                <Forecast />
            </div>
        );
    }

export default App;
