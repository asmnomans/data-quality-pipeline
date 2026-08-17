import { NavLink, Route, Routes, Navigate } from "react-router-dom";
import RunsList from "./pages/RunsList";
import RunDetail from "./pages/RunDetail";
import CandidateReview from "./pages/CandidateReview";

export default function App() {
  return (
    <div className="app-shell">
      <header className="app-header">
        <h1>DQ Framework</h1>
        <nav>
          <NavLink to="/runs" className={({ isActive }) => (isActive ? "active" : "")}>
            Runs
          </NavLink>
          <NavLink to="/candidates" className={({ isActive }) => (isActive ? "active" : "")}>
            Candidate Review
          </NavLink>
        </nav>
      </header>
      <main className="app-main">
        <Routes>
          <Route path="/" element={<Navigate to="/runs" replace />} />
          <Route path="/runs" element={<RunsList />} />
          <Route path="/runs/:runId" element={<RunDetail />} />
          <Route path="/candidates" element={<CandidateReview />} />
        </Routes>
      </main>
    </div>
  );
}
