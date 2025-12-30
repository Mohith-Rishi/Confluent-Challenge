import os
import time
from dotenv import load_dotenv
from google.cloud import aiplatform

# --- 1. INITIALIZATION ---
load_dotenv()
aiplatform.init(project=os.environ["GCP_PROJECT_ID"], location=os.environ["GCP_REGION"])
endpoint = aiplatform.Endpoint(os.environ["GCP_ENDPOINT_ID"])

def predict_pollution(sensor_name, pm25_val, co2_val):
    """
    Sends a prediction request for a specific sensor.
    """
    print(f"\n📡 Request for: {sensor_name}")
    print(f"📊 Inputs: PM2.5={pm25_val}, CO2={co2_val}")
    
    # Current timestamp (milliseconds) to match your dataset
    now_ms = int(time.time() * 1000)
    
    # Instance based on your CSV columns
    instance = {
        "sensor_id": str(sensor_name), 
        "moy_co2": str(co2_val), 
        "moy_pm25": str(pm25_val), 
        "window_start": str(now_ms),
        "window_end": str(now_ms + 1000)
    }

    try:
        response = endpoint.predict(instances=[instance])
        prediction_raw = response.predictions[0]
        
        # Result extraction (handles both dictionary or numeric formats)
        if isinstance(prediction_raw, dict):
            final_value = float(list(prediction_raw.values())[0])
        else:
            final_value = float(prediction_raw)
            
        print(f"🔮 AI RESULT: {final_value:.2f} ppm")
        
        # Diagnostic note
        if final_value > 1000:
            print("⚠️ NOTE: The model predicts extreme pollution (Saturation).")
            
    except Exception as e:
        print(f"❌ Technical Error: {e}")

# --- 2. DEMONSTRATION ---
if __name__ == "__main__":
    print("🚀 Starting sensor comparison...")

    # TEST 1: Residential Sensor (sensor_01)
    # Using "normal" values
    predict_pollution("sensor_01", 15.0, 600.0)

    # TEST 2: Industrial Sensor (sensor_industrial_01)
    # Simulating heavy activity
    predict_pollution("sensor_industrial_01", 40.0, 1200.0)

    print("\n🏁 Test finished.")