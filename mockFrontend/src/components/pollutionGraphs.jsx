import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer
} from "recharts";

export default function PollutantGraph({ data }) {
  return (
    <div className="bg-gray-800 p-4 rounded mb-6">
      <h2 className="text-xl mb-4">📈 Last 5 Minutes</h2>

      <ResponsiveContainer width="100%" height={300}>
        <LineChart data={data}>
          <XAxis dataKey="time" hide />
          <YAxis />
          <Tooltip />
          <Line type="monotone" dataKey="PM2_5" stroke="#ef4444" />
          <Line type="monotone" dataKey="CO2" stroke="#3b82f6" />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
