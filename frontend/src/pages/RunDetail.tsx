import { useEffect, useState } from "react";
import { useParams } from "react-router-dom";
import { api, type Candidate, type RunMetadata } from "../api/client";

export default function RunDetail() {
  const { runId } = useParams<{ runId: string }>();
  const [run, setRun] = useState<RunMetadata | null>(null);
  const [failures, setFailures] = useState<Record<string, unknown> | null>(null);
  const [rca, setRca] = useState<Record<string, unknown> | null>(null);
  const [compare, setCompare] = useState<Record<string, unknown> | null>(null);
  const [candidates, setCandidates] = useState<Candidate[]>([]);
  const [busyId, setBusyId] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const load = () => {
    if (!runId) return;
    api.getRun(runId).then(async (r) => {
      setRun(r);
      // Candidates aren't scoped to a run by the API (they're module-wide),
      // so pull all three statuses and filter client-side by run_id - this
      // is what lets this page show "here's what this run proposed, and
      // whether it's already been decided" instead of just raw diagnoses.
      const [pending, approved, rejected] = await Promise.all([
        api.listCandidates(r.module, "pending"),
        api.listCandidates(r.module, "approved"),
        api.listCandidates(r.module, "rejected"),
      ]);
      setCandidates([...pending, ...approved, ...rejected].filter((c) => c.run_id === runId));
    }).catch((e) => setError(String(e)));
    api.getRunFailures(runId).then(setFailures).catch(() => {});
    api.getRunRca(runId).then(setRca).catch(() => {});
    api.getRunCompare(runId).then(setCompare).catch(() => {});
  };

  useEffect(load, [runId]);

  if (error) return <p style={{ color: "var(--danger)" }}>{error}</p>;
  if (!run) return <p className="muted">Loading...</p>;

  const failureList = (failures?.failures as Array<Record<string, unknown>>) ?? [];
  const rcaResults = (rca?.results as Array<Record<string, unknown>>) ?? [];

  const candidatesFor = (r: Record<string, unknown>) =>
    candidates.filter(
      (c) =>
        c.rca_result.source_column === r.source_column &&
        c.rca_result.source_expectation_type === r.source_expectation_type
    );

  const decide = async (candidateId: string, action: "approve" | "reject") => {
    if (!run) return;
    setBusyId(candidateId);
    try {
      if (action === "approve") await api.approveCandidate(candidateId, run.module);
      else await api.rejectCandidate(candidateId, run.module, "rejected from run detail");
      load();
    } catch (e) {
      setError(String(e));
    } finally {
      setBusyId(null);
    }
  };

  return (
    <div className="stack">
      <div className="card">
        <h2 style={{ marginTop: 0 }}>{runId}</h2>
        <pre>{JSON.stringify(run, null, 2)}</pre>
      </div>

      {compare && (
        <div className="card">
          <h3 style={{ marginTop: 0 }}>Before / after</h3>
          <pre>{JSON.stringify(compare, null, 2)}</pre>
        </div>
      )}

      <div className="card">
        <h3 style={{ marginTop: 0 }}>Failed expectations ({failureList.length})</h3>
        {failureList.map((f, i) => {
          const sampleRows = (f.sample_failed_rows as Array<Record<string, unknown>>) ?? [];
          return (
            <div key={i} className="card" style={{ background: "transparent" }}>
              <strong>{String(f.expectation_type)}</strong> on <code>{String(f.column)}</code>{" "}
              <span className="muted">
                ({String(f.unexpected_count)}/{String(f.element_count)})
              </span>
              {sampleRows.length > 0 && (
                <details>
                  <summary>
                    Show {sampleRows.length} sample failing row{sampleRows.length === 1 ? "" : "s"}
                    <span className="muted"> (email/phone-style fields are structurally masked)</span>
                  </summary>
                  <div style={{ overflowX: "auto" }}>
                    <table>
                      <thead>
                        <tr>
                          {Object.keys(sampleRows[0]).map((col) => (
                            <th key={col}>{col}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {sampleRows.map((row, ri) => (
                          <tr key={ri}>
                            {Object.keys(sampleRows[0]).map((col) => (
                              <td key={col}>{row[col] === null || row[col] === undefined ? "-" : String(row[col])}</td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </details>
              )}
            </div>
          );
        })}
        {failureList.length === 0 && <p className="muted">No failures - the suite passed cleanly.</p>}
      </div>

      <div className="card">
        <h3 style={{ marginTop: 0 }}>LLM root-cause diagnoses ({rcaResults.length})</h3>
        {rcaResults.map((r, i) => (
          <div key={i} className="card" style={{ background: "transparent" }}>
            <div className="row">
              <span className="badge muted">{String(r.failure_category)}</span>
              <span className="muted">via {String(r.provider_used)}</span>
            </div>
            <p>{String(r.root_cause_explanation)}</p>
            <details>
              <summary>Suggested PySpark fix</summary>
              <pre>{String(r.suggested_pyspark_fix)}</pre>
            </details>
            <details>
              <summary>Proposed new GE rule</summary>
              <pre>{JSON.stringify(r.new_ge_expectation, null, 2)}</pre>
            </details>

            <div className="stack" style={{ marginTop: "0.75rem" }}>
              {candidatesFor(r).map((c) => (
                <div key={c.candidate_id} className="row" style={{ flexWrap: "wrap" }}>
                  <span className="badge muted">{c.artifact_type}</span>
                  <span className={`badge ${c.sandbox_passed ? "success" : "danger"}`}>
                    sandbox: {c.sandbox_passed ? "passed" : "failed"}
                  </span>
                  {c.status === "pending" ? (
                    <>
                      <button
                        className="primary"
                        disabled={!c.sandbox_passed || busyId === c.candidate_id}
                        onClick={() => decide(c.candidate_id, "approve")}
                      >
                        Approve
                      </button>
                      <button
                        className="danger"
                        disabled={busyId === c.candidate_id}
                        onClick={() => decide(c.candidate_id, "reject")}
                      >
                        Reject
                      </button>
                    </>
                  ) : (
                    <span className={`badge ${c.status === "rejected" ? "danger" : "success"}`}>{c.status}</span>
                  )}
                  {!c.sandbox_passed && <span className="muted">{c.sandbox_notes}</span>}
                </div>
              ))}
              {candidatesFor(r).length === 0 && (
                <p className="muted">No candidate was recorded for this diagnosis (unexpected - check backend logs).</p>
              )}
            </div>
          </div>
        ))}
        {rcaResults.length === 0 && <p className="muted">No RCA generated for this run.</p>}
      </div>
    </div>
  );
}
