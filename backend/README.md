## 🧠 AI Integration
This project features a dual-AI architecture:
* **Predictive Analytics**: Uses a custom-trained **Google Vertex AI** model to forecast pollution levels 5 minutes into the future.
* **Expert Insights**: Integrated **Gemini 2.0 Flash-Lite** to act as an automated HSE (Health, Safety, and Environment) expert, providing real-time safety recommendations based on air quality trends.

## 🏗️ Architecture
1. **Producer**: Simulates industrial sensor data with realistic sinusoidal patterns.
2. **Kafka (Confluent Cloud)**: Handles the real-time data streaming backbone.
3. **FastAPI Backend**: Asynchronously consumes multiple topics (Raw Data, Predictions, Alerts).
4. **WebSockets**: Pushes data instantly to the frontend dashboard.



## 🐳 Docker Deployment
You can also run the backend using Docker:
```bash
docker build -t pollution-backend .
docker run -p 8000:8000 --env-file .env pollution-backend
