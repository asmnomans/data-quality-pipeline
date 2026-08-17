// Thin fetch wrapper over the REST contract in docs/ARCHITECTURE.md section 7.
// Every function here maps 1:1 to one backend route - no client-side business
// logic, mirroring how cli.py is also a thin wrapper over the same core.

const BASE = "/api";

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...init,
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(body.detail ?? `Request failed: ${res.status}`);
  }
  return res.json();
}

export interface Module {
  name: string;
  description: string;
  primary_key: string;
}

export interface JobStatus {
  job_id: string;
  status: "running" | "done" | "failed";
  result?: unknown;
  error?: string | null;
}

export interface Candidate {
  candidate_id: string;
  run_id: string;
  module: string;
  artifact_type: "rule" | "fix";
  created_at: string;
  rca_result: {
    failure_category: string;
    root_cause_explanation: string;
    suggested_pyspark_fix: string;
    new_ge_expectation: Record<string, unknown>;
    source_expectation_type?: string;
    source_column?: string;
    provider_used?: string;
  };
  sandbox_passed: boolean;
  sandbox_notes?: string;
  status: "pending" | "approved" | "rejected" | "auto_promoted";
}

export interface RunMetadata {
  run_id: string;
  module: string;
  source_ref: string;
  started_at: string;
  finished_at?: string;
  status: "running" | "succeeded" | "failed";
  phase_reached: string;
  total_rows?: number;
  failed_expectation_count?: number;
  candidates_generated: number;
  error?: string | null;
}

export const api = {
  startRun: (moduleName: string, sourceRef?: string) =>
    request<{ job_id: string }>(`/modules/${moduleName}/runs`, {
      method: "POST",
      body: JSON.stringify({ source_ref: sourceRef ?? null }),
    }),

  uploadFile: async (moduleName: string, file: File) => {
    const form = new FormData();
    form.append("file", file);
    const res = await fetch(`${BASE}/modules/${moduleName}/upload`, { method: "POST", body: form });
    if (!res.ok) throw new Error(`Upload failed: ${res.status}`);
    return res.json() as Promise<{ source_ref: string }>;
  },

  getJob: (jobId: string) => request<JobStatus>(`/jobs/${jobId}`),

  getRun: (runId: string) => request<RunMetadata>(`/runs/${runId}`),
  getRunFailures: (runId: string) => request<Record<string, unknown>>(`/runs/${runId}/failures`),
  getRunRca: (runId: string) => request<Record<string, unknown>>(`/runs/${runId}/rca`),
  getRunCompare: (runId: string) => request<Record<string, unknown>>(`/runs/${runId}/compare`),

  listCandidates: (moduleName: string, status = "pending") =>
    request<Candidate[]>(`/candidates?module=${moduleName}&status=${status}`),

  approveCandidate: (candidateId: string, moduleName: string) =>
    request<{ candidate: Candidate; comparison: unknown }>(
      `/candidates/${candidateId}/approve?module=${moduleName}`,
      { method: "POST", body: JSON.stringify({ actor: "user:ui" }) }
    ),

  rejectCandidate: (candidateId: string, moduleName: string, reason?: string) =>
    request<Candidate>(`/candidates/${candidateId}/reject?module=${moduleName}`, {
      method: "POST",
      body: JSON.stringify({ actor: "user:ui", reason: reason ?? null }),
    }),
};
