export function Spinner({ label }: { label?: string }) {
  return (
    <div className="flex items-center gap-3 text-slate-600">
      <span
        aria-hidden
        className="inline-block h-4 w-4 animate-spin rounded-full border-2 border-slate-300 border-t-indigo-600"
      />
      {label && <span className="text-sm">{label}</span>}
    </div>
  );
}
