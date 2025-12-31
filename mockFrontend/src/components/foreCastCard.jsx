export default function ForecastCard({ title, children }) {
  return (
    <div className="border-l-4 border-l-red-500 rounded-xl border border-gray-700 shadow-md h-40 flex items-center justify-center grid grid-cols-2 gap-6 w-full">

      <div className="flex flex-col items-center">
        <p className="text-sm opacity-80 mb-2">{title}</p>
        {children}
      </div>
    </div>
  );
}
