import { useEffect, useState } from "react";
import { api, type Candidate, type Module } from "../api/client";

export default function CandidateReview() {
  const [modules, setModules] = useState<Module[]>([]);
  const [selectedModule, setSelectedModule] = useState("");
  const [candidates, setCandidates] = useState<Candidate[]>([]);
  const [busyId, setBusyId] = useState<string | null>(null);
  const [lastComparison, setLastComparison] = useState<unknown>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetch("/api/modules")
      .then((r) => r.json())
      .then((data: Module[]) => {
        setModules(data);
        if (data.length > 0) setSelectedModule(data[0].name);
      });
  }, []);

  const refresh = () => {
    if (!selectedModule) return;
    api.listCandidates(selectedModule, "pending").then(setCandidates).catch((e) => setError(String(e)));
  };

  useEffect(refresh, [selectedModule]);

  const handleApprove = async (candidateId: string) => {
    setBusyId(candidateId);
    setError(null);
    try {
      const { comparison } = await api.approveCandidate(candidateId, selectedModule);
      setLastComparison(comparison);
      refresh();
    } catch (e) {
      setError(String(e));
    } finally {
      setBusyId(null);
    }
  };

  const handleReject = async (candidateId: string) => {
    setBusyId(candidateId);
    setError(null);
    try {
      await api.rejectCandidate(candidateId, selectedModule, "rejected via UI");
      refresh();
    } catch (e) {
      setError(String(e));
    } finally {
      setBusyId(null);
    }
  };

  return (
    <div className="stack">
      <div className="card">
        <div className="row">
          <label>Module:</label>
          <select value={selectedModule} onChange={(e) => setSelectedModule(e.target.value)}>
            {modules.map((m) => (
              <option key={m.name} value={m.name}>
                {m.name}
              </option>
            ))}
          </select>
        </div>
        {error && <p style={{ color: "var(--danger)" }}>{error}</p>}
      </div>

      {lastComparison ? (
        <div className="card">
          <h3 style={{ marginTop: 0 }}>Result of last approval (re-validated, not re-ingested)</h3>
          <pre>{JSON.stringify(lastComparison, null, 2)}</pre>
        </div>
      ) : null}

      <div className="card">
        <h2 style={{ marginTop: 0 }}>Pending candidates ({candidates.length})</h2>
        {candidates.map((c) => (
          <div key={c.candidate_id} className="card" style={{ background: "transparent" }}>
            <div className="row">
              <span className="badge muted">{c.artifact_type}</span>
              <span className="badge muted">{c.rca_result.failure_category}</span>
              <span className={`badge ${c.sandbox_passed ? "success" : "danger"}`}>
                sandbox: {c.sandbox_passed ? "passed" : "failed"}
              </span>
              <span className="muted">run {c.run_id}</span>
            </div>
            <p>{c.rca_result.root_cause_explanation}</p>
            {c.artifact_type === "fix" ? (
              <pre>{c.rca_result.suggested_pyspark_fix}</pre>
            ) : (
              <pre>{JSON.stringify(c.rca_result.new_ge_expectation, null, 2)}</pre>
            )}
            {c.sandbox_notes && <p className="muted">{c.sandbox_notes}</p>}
            <div className="row">
              <button
                className="primary"
                disabled={!c.sandbox_passed || busyId === c.candidate_id}
                onClick={() => handleApprove(c.candidate_id)}
              >
                Approve
              </button>
              <button className="danger" disabled={busyId === c.candidate_id} onClick={() => handleReject(c.candidate_id)}>
                Reject
              </button>
            </div>
          </div>
        ))}
        {candidates.length === 0 && <p className="muted">Nothing pending - every candidate has been decided.</p>}
      </div>
    </div>
  );
}
