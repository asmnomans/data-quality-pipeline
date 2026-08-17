import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { api, type Module, type RunMetadata } from "../api/client";

const POLL_MS = 3000;

export default function RunsList() {
  const [modules, setModules] = useState<Module[]>([]);
  const [selectedModule, setSelectedModule] = useState<string>("");
  const [runs, setRuns] = useState<RunMetadata[]>([]);
  const [jobId, setJobId] = useState<string | null>(null);
  const [jobStatus, setJobStatus] = useState<string>("");
  const [jobError, setJobError] = useState<string | null>(null);
  const [file, setFile] = useState<File | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetch("/api/modules")
      .then((r) => r.json())
      .then((data: Module[]) => {
        setModules(data);
        if (data.length > 0) setSelectedModule(data[0].name);
      })
      .catch((e) => setError(String(e)));
  }, []);

  const refreshRuns = () => {
    if (!selectedModule) return;
    fetch(`/api/runs?module=${selectedModule}`)
      .then((r) => r.json())
      .then(setRuns)
      .catch((e) => setError(String(e)));
  };

  useEffect(refreshRuns, [selectedModule]);

  // Poll a just-started job until it finishes, then refresh the run list.
  useEffect(() => {
    if (!jobId) return;
    const interval = setInterval(async () => {
      const job = await api.getJob(jobId);
      setJobStatus(job.status);
      setJobError(job.error ?? null);
      if (job.status !== "running") {
        clearInterval(interval);
        refreshRuns();
      }
    }, POLL_MS);
    return () => clearInterval(interval);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [jobId]);

  const handleRunLatest = async () => {
    setError(null);
    try {
      const { job_id } = await api.startRun(selectedModule);
      setJobId(job_id);
      setJobStatus("running");
      setJobError(null);
    } catch (e) {
      setError(String(e));
    }
  };

  const handleUploadAndRun = async () => {
    if (!file) return;
    setError(null);
    try {
      const { source_ref } = await api.uploadFile(selectedModule, file);
      const { job_id } = await api.startRun(selectedModule, source_ref);
      setJobId(job_id);
      setJobStatus("running");
      setJobError(null);
    } catch (e) {
      setError(String(e));
    }
  };

  return (
    <div className="stack">
      <div className="card">
        <h2 style={{ marginTop: 0 }}>Run the pipeline</h2>
        <div className="row" style={{ marginBottom: "0.75rem" }}>
          <label>Module:</label>
          <select value={selectedModule} onChange={(e) => setSelectedModule(e.target.value)}>
            {modules.map((m) => (
              <option key={m.name} value={m.name}>
                {m.name}
              </option>
            ))}
          </select>
          <button className="primary" onClick={handleRunLatest} disabled={jobStatus === "running"}>
            Run latest incoming file
          </button>
        </div>
        <div className="row">
          <input type="file" accept=".csv" onChange={(e) => setFile(e.target.files?.[0] ?? null)} />
          <button onClick={handleUploadAndRun} disabled={!file || jobStatus === "running"}>
            Upload &amp; Run
          </button>
          {jobId && (
            <span className={jobStatus === "failed" ? "" : "muted"}>
              job {jobId.slice(0, 8)}: {jobStatus}
            </span>
          )}
        </div>
        {jobError && (
          <p style={{ color: "var(--danger)" }}>
            {jobError.includes("already in flight")
              ? "Another run for this module is still in progress (e.g. one started from the CLI/VS Code) - only one run per module at a time is allowed. Wait for it to finish, then try again."
              : jobError}
          </p>
        )}
        {error && <p style={{ color: "var(--danger)" }}>{error}</p>}
      </div>

      <div className="card">
        <h2 style={{ marginTop: 0 }}>Runs</h2>
        <table>
          <thead>
            <tr>
              <th>run_id</th>
              <th>status</th>
              <th>phase</th>
              <th>rows</th>
              <th>failures</th>
              <th>candidates</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {runs.map((r) => (
              <tr key={r.run_id}>
                <td>{r.run_id}</td>
                <td>
                  <span className={`badge ${r.status === "succeeded" ? "success" : r.status === "failed" ? "danger" : "muted"}`}>
                    {r.status}
                  </span>
                </td>
                <td className="muted">{r.phase_reached}</td>
                <td>{r.total_rows ?? "-"}</td>
                <td>{r.failed_expectation_count ?? "-"}</td>
                <td>{r.candidates_generated}</td>
                <td>
                  <Link to={`/runs/${r.run_id}`}>View</Link>
                </td>
              </tr>
            ))}
            {runs.length === 0 && (
              <tr>
                <td colSpan={7} className="muted">
                  No runs yet for this module.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
