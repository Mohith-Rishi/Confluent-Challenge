import { useEffect } from "react";
import { toast } from "react-toastify";

export default function Alerts({ value }) {
  useEffect(() => {
    if (value > 150) {
      toast.error("⚠️ Unhealthy air quality detected!");
    }
  }, [value]);

  if (value == null) {
    return (
      <div className="bg-gray-700 p-4 rounded mb-4">
        ⏳ Waiting for air quality data...
      </div>
    );
  }

  if (value > 150) {
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
