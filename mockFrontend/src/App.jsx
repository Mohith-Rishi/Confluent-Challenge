import { useEffect, useState, useRef } from "react";
import StatCard from "./components/statCard";
import PollutantGraph from "./components/pollutionGraphs";
import Alerts from "./components/AlertsPanel";
import Recommendations from "./components/RecommendationsTab";
import "react-toastify/dist/ReactToastify.css";
import { ToastContainer, toast } from "react-toastify";
import ForecastRing from "./components/foreCastRing";
import ForecastCard from "./components/foreCastCard";

const EMPTY_POLLUTION = { PM2_5: null, CO2: null };

/* =========================================================
   MOCK DATA GENERATOR (mirrors Python sine/cos simulator)
   ========================================================= */
let counter = 0;

function generateMockData() {
  counter += 0.1;

  const co2Base = 600;
  const co2Amplitude = 200;
  const co2 =
    co2Base +
    co2Amplitude * Math.sin(counter) +
    (Math.random() * 10 - 5);

  const pm25Base = 20;
  const pm25 =
    pm25Base +
    10 * Math.cos(counter) +
    (Math.random() * 4 - 2);

  return {
    sensorId: "sensor_industrial_01",
    CO2: Math.round(co2),
    PM2_5: Math.round(pm25),
    timestamp: Date.now(),
    time: new Date().toLocaleTimeString(),
  };
}

/* =========================================================
   MOCK AI PREDICTION (mirrors Vertex AI behavior)
   ========================================================= */
function mockAIPredict({ sensorId, CO2, PM2_5 }) {
  // Simulated feature impact (non-linear, AI-like)
  const co2Impact = CO2 * 1.2;
  const pm25Impact = PM2_5 * 15;

  const predictedValue =
    0.6 * co2Impact +
    0.4 * pm25Impact +
    Math.random() * 50; // uncertainty

  let level, message;

  if (predictedValue > 1000) {
    level = "CRITICAL";
    message =
      "The AI model predicts extreme pollution levels. Immediate intervention is required, including halting non-essential operations and activating emergency ventilation systems.";
  } else if (predictedValue > 800) {
    level = "HIGH RISK";
    message =
      "The AI model forecasts high pollution levels. Reduce industrial emissions and enhance air filtration to prevent escalation.";
  } else if (predictedValue > 600) {
    level = "MODERATE";
    message =
      "The AI model indicates moderate pollution risk. Continuous monitoring and minor operational adjustments are recommended.";
  } else {
    level = "LOW";
    message =
      "The AI model predicts acceptable air quality levels. No immediate action is required.";
  }

  return {
    sensor_id: sensorId,
    predicted_value: Math.round(predictedValue),
    level,
    message,
    model: "vertex-ai-mock",
  };
}

export default function App() {
  const lastToastTimeCo2 = useRef(0);
  const lastToastTimePm25 = useRef(0);
  const lastPred=useRef(0);

  const [latest, setLatest] = useState(EMPTY_POLLUTION);
  const [data, setData] = useState([]);
  const [showGraphs, setShowGraphs] = useState(false);
  const [forecastData, setForecastData] = useState(EMPTY_POLLUTION);
  const [aiRecommendation, setAiRecommendation] = useState(null);

  /* =========================================================
     MOCK STREAM (replaces WebSockets)
     ========================================================= */
  useEffect(() => {
    const interval = setInterval(() => {
      const formatted = generateMockData();

      // ---- Live data ----
      setLatest(formatted);
      setData((prev) => [...prev, formatted].slice(-150));

      // ---- Forecast (future-shifted pattern) ----
      setForecastData({
        CO2: Math.round(formatted.CO2 + 30 * Math.sin(counter + 1)),
        PM2_5: Math.round(formatted.PM2_5 + 5 * Math.cos(counter + 1)),
      });

      // ---- Alerts (unchanged logic) ----
      const now = Date.now();

      if (formatted.PM2_5 > 35 && now - lastToastTimePm25.current > 10000) {
        toast.warn("⚠️ High PM2.5 detected");
        lastToastTimePm25.current = now;
      }

      if (formatted.CO2 > 650 && now - lastToastTimeCo2.current > 10000) {
        toast.error("🚨 High CO₂ level");
        lastToastTimeCo2.current = now;
      }

      // ---- AI-aligned mock recommendation ----
      const aiResult = mockAIPredict(formatted);
      if(now-lastPred.current>1000){
        setAiRecommendation(aiResult);
        lastPred.current=now;
      }
      

    }, 100); // ⏱ 1 second frequency

    return () => clearInterval(interval);
  }, []);

  return (
    <div className="min-h-screen bg-gray-900 text-white p-6">
      <h1 className="text-3xl font-bold mb-6">
        🌍 Real-Time Pollution Dashboard
      </h1>

      {/* Live Stats */}
      <div className="grid grid-cols-2 gap-4 mb-6">
        <StatCard label="PM2.5" value={latest?.PM2_5} unit="µg/m³" type="pm25" />
        <StatCard label="CO2" value={latest?.CO2} unit="ppb" type="co2" />
      </div>

      {/* Forecast Section */}
      <div className="border-gray-700 hover:shadow-lg rounded-lg p-4 border border-gray-700 mb-6">
        <h2 className="text-sm font-semibold mb-4 opacity-80">
          🔮 Forecast Severity (Next 5 Minutes)
        </h2>

        <div className="flex gap-8">
          <ForecastCard>
            <ForecastRing
              label="PM2.5"
              now={latest?.PM2_5}
              forecast={forecastData?.PM2_5}
              unit="µg/m³"
              type="pm25"
            />
          </ForecastCard>

          <ForecastCard>
            <ForecastRing
              label="CO₂"
              now={latest?.CO2}
              forecast={forecastData?.CO2}
              unit="ppb"
              type="co2"
            />
          </ForecastCard>
        </div>
      </div>

      {/* Graph Toggle */}
      <button
        className="bg-blue-600 px-4 py-2 rounded hover:bg-blue-700 mb-6"
        onClick={() => setShowGraphs(!showGraphs)}
      >
        {showGraphs ? "Hide Graphs" : "Show Graphs"}
      </button>

      {/* Graphs */}
      {showGraphs && <PollutantGraph data={data} />}

      {/* Alerts + AI Recommendations */}
      <Alerts co2={latest.CO2} pm25={latest.PM2_5} />
      <Recommendations data={aiRecommendation} />

      <ToastContainer />
    </div>
  );
}
