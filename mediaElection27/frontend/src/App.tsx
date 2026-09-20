import { Route, Routes } from "react-router-dom";

import Articles from "@/routes/Articles";
import Dashboard from "@/routes/Dashboard";
import Leaderboard from "@/routes/Leaderboard";
import ShareOfVoice from "@/routes/ShareOfVoice";
import SourceDrilldown from "@/routes/SourceDrilldown";

export default function App() {
  return (
    <Routes>
      <Route path="/" element={<Dashboard />} />
      <Route path="/leaderboard" element={<Leaderboard />} />
      <Route path="/share" element={<ShareOfVoice />} />
      <Route path="/sources" element={<SourceDrilldown />} />
      <Route path="/articles" element={<Articles />} />
    </Routes>
  );
}
