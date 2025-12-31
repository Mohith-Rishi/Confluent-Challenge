export default function Recommendations({ data }) {
  if (!data) return <p className="p-4 opacity-50">Awaiting the HSE expert's analysis...</p>;

  return (
    <div className="mt-6 p-4 bg-gray-800 rounded-lg border-l-4 border-blue-500">
      <h3 className="text-blue-400 font-bold">🤖  Expert Tip </h3>
      <p className="text-gray-200 italic mt-2">"{data.message}"</p>
    </div>
  );
}