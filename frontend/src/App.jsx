import { useEffect, useState } from "react";
import StatCard from "./components/statCard";
import PollutantGraph from "./components/pollutionGraphs";
import Alerts from "./components/AlertsPanel";
import Recommendations from "./components/RecommendationsTab";
import 'react-toastify/dist/ReactToastify.css';
import { ToastContainer } from 'react-toastify';
import React from 'react'
import { toast } from "react-toastify";
import { useRef } from "react";



const EMPTY_POLLUTION = {
  PM2_5: null,
  CO2: null,
};

export default function App() {

  const lastToastTimeCo2 = useRef(0);
  const lastToastTimePm25 = useRef(0);
  
  const [latest, setLatest] = useState(EMPTY_POLLUTION);
  const [data, setData] = useState([]);
  const [showGraphs, setShowGraphs] = useState(false);

  useEffect(() => {

    const ws_url=import.meta.env.VITE_WS_TARGET || "ws://localhost:8000/ws"
    const ws = new WebSocket(ws_url);

    ws.onmessage = (event) => {
      const parsed = JSON.parse(event.data);

      const formatted = {
        time: new Date(parsed.timestamp * 1000).toLocaleTimeString(),
        CO2: parsed.co2,
        PM2_5: parsed.pm25,
        sensorId: parsed.sensor_id,
      };

      setData((prev) => [...prev, formatted].slice(-150));
      setLatest(formatted);

      const now = Date.now();

      if (formatted.PM2_5 > 35 && now - lastToastTimePm25.current > 10000) {
        toast.warn("⚠️ High PM2.5 detected", {
          position: "top-right",
          autoClose: 5000,
        });
        lastToastTimePm25.current = now;
      }
      
      if (formatted.CO2 > 650 && now - lastToastTimeCo2.current > 10000) {
        toast.error("🚨 High CO₂ level", {
          position: "top-right",
          autoClose: 5000,
        });
        lastToastTimeCo2.current = now;
      }

      
    };

    ws.onerror = (err) => console.error("WebSocket error", err);

    return () => ws.close();
  }, []);



  return (
    <div className="min-h-screen bg-gray-900 text-white p-6">
      <h1 className="text-3xl font-bold mb-6">
        🌍 Real-Time Pollution Dashboard
      </h1>

      {/* Cards */}
      <div className="grid grid-cols-2 gap-4 mb-6">
        <StatCard label="PM2.5" value={latest?.PM2_5} unit="µg/m³" type="pm25" />
        <StatCard label="CO2" value={latest?.CO2} unit="ppb" type="co2" />
      </div>

      {/* Button */}
      <button
        className="bg-blue-600 px-4 py-2 rounded hover:bg-blue-700 mb-6"
        onClick={() => setShowGraphs(!showGraphs)}
      >
        {showGraphs ? "Hide Graphs" : "Show Graphs"}
      </button>

      {/* Graphs */}
      {showGraphs && <PollutantGraph data={data} />}

      <Alerts co2={latest.CO2} pm25={latest.PM2_5} />
      <Recommendations />
      <ToastContainer />
    </div>
  );
}
