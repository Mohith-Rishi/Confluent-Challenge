🏭 HSE Sentinel: Predictive Industrial IoT & AI Safety Pipeline
<div align="left"> <img src="https://img.shields.io/badge/Python-3.11-3776AB?style=for-the-badge&logo=python&logoColor=white" /> <img src="https://img.shields.io/badge/Framework-FastAPI-009688?style=for-the-badge&logo=fastapi&logoColor=white" /> <img src="https://img.shields.io/badge/Frontend-React-61DAFB?style=for-the-badge&logo=react&logoColor=black" /> <img src="https://img.shields.io/badge/AI-Gemini_2.0-4285F4?style=for-the-badge&logo=google-gemini&logoColor=white" /> <img src="https://img.shields.io/badge/Streaming-Confluent_Kafka-000000?style=for-the-badge&logo=confluent&logoColor=white" /> <img src="https://img.shields.io/badge/Infrastructure-Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white" /> </div>

📖 Overview

HSE Sentinel is a high-performance, real-time industrial monitoring platform. It bridges the gap between raw IoT telemetry and actionable safety intelligence by combining Confluent Cloud (Kafka & Flink) with Google Vertex AI and Gemini 2.0.

The system doesn't just monitor data; it predicts hazards and provides expert HSE guidance in real-time.

🚀 Advanced Infrastructure

Our architecture is designed for low-latency decision-making and enterprise-grade scalability:

Ingestion: A Python-based IoT simulator streaming high-frequency raw CO2 and PM2.5 data to Confluent Cloud.

Stream Processing: Flink SQL transforms raw data into stable 30-second tumbling windows to eliminate sensor noise and calculate trends.

Predictive AI (Vertex AI): A time-series model integrated into the pipeline to forecast pollutant levels 5 minutes into the future.

Generative HSE Agent (Gemini 2.0): Google's latest LLM analyzes live trends to generate immediate, context-aware safety recommendations.

Real-Time Delivery: A FastAPI backend pushes data to a React dashboard via WebSockets for instant visualization.

🛠️ Tech Stack

Layer	Technologies

Streaming	Confluent Cloud, Kafka, Flink SQL

Artificial Intelligence	Google Gemini 2.0 Flash-Lite, Vertex AI (Forecasting)

Backend	Python 3.11, FastAPI, Asynchronous Kafka Consumers

Frontend	React 18, Vite, Tailwind CSS v4, Recharts, Lucide Icons

Infrastructure	Docker, Nginx (Reverse Proxy), .dockerignore

Exporter vers Sheets

📈 Roadmap & Current Progress

[x] Phase 1: Real-time Python Producer (Sensor Simulation) with sinusoidal patterns.

[x] Phase 2: Flink SQL aggregation for data stability.

[x] Phase 3: Vertex AI integration for predictive severity analysis.

[x] Phase 4: Gemini 2.0 HSE Expert Tip integration.

[x] Phase 5: Full-stack containerization with Docker & Nginx.

⚙️ Setup & Installation

Note: For security, .env and .venv files are excluded. You must provide your own credentials.

1. Environment Variables

Create a .env file in the root directory:

Plaintext

# Confluent Kafka

KAFKA_BOOTSTRAP_SERVER=your_server

KAFKA_API_KEY=your_key

KAFKA_API_SECRET=your_secret

# Google Cloud

GOOGLE_APPLICATION_CREDENTIALS=path_to_your_service_account.json

2. Using Docker (Recommended)

The fastest way to launch the entire stack (Backend + Frontend + Proxy):

Bash

# Build and start all services

docker-compose up --build

# Run in detached mode

docker-compose up -d

3. Manual Installation
   
Backend:

Bash

cd backend

pip install -r requirements.txt

uvicorn main:app --host 0.0.0.0 --port 8000

Frontend:

Bash

cd frontend

npm install

npm run dev

🛡️ Security & Reliability

Graceful Shutdown: Backend uses asynccontextmanager to ensure Kafka consumers close connections properly.

Data Integrity: Flink SQL ensures that spikes are validated over 30s windows to prevent false positive alerts.

Production Ready: Nginx handles static file serving and reverse-proxying to ensure high availability.

🚀 Ready for Submission!
With this README, you are showing that you have a complete enterprise data loop: Ingestion → Processing → Prediction → Recommendation → Visualization.

Would you like me to create a 1-sentence "Value Proposition" for your Devpost header? It's the final punchy line you need!
