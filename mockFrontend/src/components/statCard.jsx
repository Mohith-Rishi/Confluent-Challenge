export default function StatCard({ label, value, unit,type }) {
  if (value === undefined) return null;

  let status = "Good";
  let bg = "bg-green-600";
  let glow = "shadow-green-500/40";

  if ((type=="pm25" && value>35) || (type=="co2" && value>650)) {
    status = "Unhealthy";
    bg = "bg-red-600";
    glow = "shadow-red-500/40";
  } else if ((type=="pm25" && value>25) || (type=="co2" && value>500)) {
    status = "Moderate";
    bg = "bg-yellow-500";
    glow = "shadow-yellow-500/40";
  }

  return (
    <div
      className={`p-5 rounded-xl ${bg} ${glow} shadow-lg transition-transform hover:scale-105`}
    >
      <p className="text-sm opacity-80">{label}</p>

      <div className="flex items-end gap-2 mt-2">
        <span className="text-4xl font-bold">{value !== null && value !== undefined ? value : "--"}{" "}</span>
        <span className="text-sm opacity-80">{unit}</span>
      </div>

      <p className="text-sm mt-2 font-medium">
        Status: {status}
      </p>
    </div>
  );
}
