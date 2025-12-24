# Confluent-Challenge
🏭 Industrial Air Quality Sentinel

Real-Time IoT Pipeline with Flink SQL & Local AI (Ollama)This project is a hybrid real-time industrial IoT pipeline designed to monitor air quality in factory environments. It captures raw sensor data, processes it via cloud-native streaming, and uses Local AI to generate immediate HSE (Health, Safety, and Environment) recommendations.


🚀 Project Infrastructure

Our infrastructure is built for scale and low-latency decision-making:

-Ingestion: A Python-based IoT simulator publishes raw CO_2 and PM_{2.5} data to Confluent Cloud.

-Stream Processing: Flink SQL transforms high-frequency raw data into stable 30-second averages to eliminate noise.

-Edge AI Inference: 


🛠️ Tech Stack




📈 Current Progress vs. Roadmap 

Step 1: Real-time Python Producer (Sensor Simulation).

Step 2: Flink SQL Tumbling Windows for data aggregation.

Step 3: Local AI HSE Agent (Phi-3 Integration).


⚙️ Setup & Installation

Note: The .env and .venv files are excluded for security. Follow these steps to replicate the environment.

1. Clone the repository
   
Bash

git clone https://github.com/Mohith-Rishi/Confluent-Challenge.git

cd Confluent-Challenge

2. Install Dependencies
   
Bash

pip install -r requirements.txt

3. Environment Variables
   
Create a .env file in the root directory and add your Confluent credentials:

Plaintext

KAFKA_BOOTSTRAP_SERVER=your_server

KAFKA_API_KEY=your_key

KAFKA_API_SECRET=your_secret


⚙️ using Docker
Create .env file in your root directory. Take .env_sample as template and fill missing variables.
Install docker on local environment.

Run:
docker-compose up

Run if you made changes and want to see debug information:
docker-compose up --build

Run detached:
docker-compose up --build -d

Close:
docker-compose down

