# AI Engine – HSE Sentinel Intelligence Layer

This directory contains the **core intelligence layer** of **HSE Sentinel**.  
It manages the **entire data lifecycle**, from real-time industrial IoT simulation to **Machine Learning forecasting** and **Generative AI–based safety analysis**.

---

## 🏗 AI Architecture

The AI Engine is structured around **four main pillars**:

1. **Ingestion**  
   Pattern-based industrial sensor simulation.

2. **Data Harvesting**  
   Automated dataset generation for machine learning training.

3. **Predictive Analytics**  
   Real-time forecasting using **Google Vertex AI**.

4. **Generative Insights**  
   Safety recommendations powered by **Gemini 2.0 Flash-Lite**.

---

## 📂 Component Breakdown

### `producer.py` — IoT Sensor Simulator
- **Purpose:** Simulates industrial sensors emitting CO₂ and PM2.5 values.
- **Logic:** Uses sine and cosine waves to reproduce realistic daily industrial fluctuations.
- **Kafka Topic:** `telemetry_brute`
- **Frequency:** High-frequency streaming (every 0.1 seconds)

---

### `training/collector.py` — Data Harvester
- **Purpose:** Automatically builds training datasets.
- **Source:** Consumes aggregated streams produced by **Flink SQL**.
- **Kafka Topic:** `moyennes_finales`
- **Feature:**  
  - Handles **Confluent Schema Registry** wire format  
  - Skips the first 5 magic bytes for clean JSON parsing
- **Output:**  
  - Generates `pollution_training_v2.csv` after reaching **1,500 records**

---

### `ai_processor.py` — Vertex AI Inference Engine
- **Role:** Predictive brain of the system
- **Model:** Google Vertex AI **AutoML Tabular**
- **Workflow:**  

Raw Telemetry
↓
Vertex AI Endpoint
↓
5-minute Pollution Forecast
↓
Kafka Topic: predictions_ia


---

### `consumer_ai.py` — Gemini 2.0 HSE Expert
- **Role:** Generative AI safety advisor
- **Model:** Gemini 2.0 Flash-Lite (ultra-low latency)
- **Persona:** Senior HSE (Health, Safety & Environment) Engineer
- **Function:**  
- Analyzes pollution thresholds
- Generates human-readable safety insights
- **Kafka Topic:** `alertes_pollution`
- **Output:**  
- `INDUSTRIAL ALERT`
- Preventive safety status reports

---

## ⚙️ Setup & Installation

### 1️⃣ Prerequisites
- Python **3.9+**
- Google Cloud Project with **Vertex AI enabled**
- Confluent Cloud Kafka Cluster

---

### 2️⃣ Environment Variables

Create a `.env` file in this directory with the following configuration:

```bash
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVER=your_server.confluent.cloud:9092
KAFKA_API_KEY=your_key
KAFKA_API_SECRET=your_secret

# Google Cloud Configuration
GCP_PROJECT_ID=your_project_id
GCP_REGION=us-central1
GCP_ENDPOINT_ID=your_vertex_ai_endpoint_id
```
### 3️⃣ Install Dependencies

Install all required Python packages:

