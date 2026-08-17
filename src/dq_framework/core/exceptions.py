"""Exception hierarchy for dq_framework.

Every layer raises one of these (never a bare Exception) so callers -
CLI, API, scheduler, watcher - can catch precisely and report cleanly
instead of leaking stack traces to end users.
"""


class DQFrameworkError(Exception):
    """Base class for all framework errors."""


class ModuleNotFoundError_(DQFrameworkError):
    """Raised when a module name isn't present in the module registry."""


class ModuleConfigError(DQFrameworkError):
    """Raised when a module.yaml is missing required fields or is malformed."""


class IngestionError(DQFrameworkError):
    """Raised when a configured data source cannot be read."""


class ValidationEngineError(DQFrameworkError):
    """Raised when the validation engine (Great Expectations) fails to run a suite."""


class FailureExtractionError(DQFrameworkError):
    """Raised when a ValidationResult cannot be normalized into a FailureReport."""


class LLMProviderError(DQFrameworkError):
    """Raised when an LLM provider call fails after retries (before falling back)."""


class LLMAllProvidersExhaustedError(DQFrameworkError):
    """Raised when the primary and fallback LLM providers both fail."""


class UnsafeRemediationCodeError(DQFrameworkError):
    """Raised when an LLM-suggested PySpark snippet fails AST safety validation."""


class RemediationExecutionError(DQFrameworkError):
    """Raised when a safety-validated remediation snippet still fails to execute."""


class RuleValidationError(DQFrameworkError):
    """Raised when a candidate GE rule is structurally invalid or fails baseline dry-run."""


class RuleStoreError(DQFrameworkError):
    """Raised on archive/promote/rollback failures in the rule store."""


class PipelineLockError(DQFrameworkError):
    """Raised when a run is requested for a module that already has one in flight."""


class TriggerError(DQFrameworkError):
    """Raised by scheduler/file-watcher trigger adapters."""
