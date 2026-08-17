"""dq_framework: Automated Data Quality & Self-Healing Pipeline.

PySpark ingestion + Great Expectations validation + LLM-driven root cause
analysis and guarded self-remediation, organized as pluggable modules (one
per data domain) behind stable interfaces (DataSource, ValidationEngine,
LLMProvider, RuleStore, Trigger) so new datasets, LLM providers, or trigger
sources can be added without touching core pipeline logic.

See docs/ARCHITECTURE.md at the project root for the full design.
"""

__version__ = "0.1.0"
