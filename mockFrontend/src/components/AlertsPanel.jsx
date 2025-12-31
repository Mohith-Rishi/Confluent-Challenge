import { useEffect } from "react";
import { toast } from "react-toastify";

export default function Alerts({ co2,pm25 }) {
  
  if (co2== null || pm25==null) {
    return (
      <div className="bg-gray-700 p-4 rounded mb-4">
        ⏳ Waiting for air quality data...
      </div>
    );
  }

  if (co2>650 || pm25>35) {
    return (
      <div className="bg-red-600 p-4 rounded mb-4">
        ⚠️ Unhealthy air quality detected
      </div>
    );
  }

  return (
    <div className="bg-green-600 p-4 rounded mb-4">
      ✅ Air quality is acceptable
    </div>
  );
}
