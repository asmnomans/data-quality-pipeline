"""Configuration loading: global settings.yaml + env vars + per-module module.yaml.

Precedence for paths/secrets: environment variable > config/settings.yaml default.
This is the one place in the codebase that reads YAML/env directly - every other
layer receives already-validated pydantic config objects.
"""
from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[3]


class EnvSettings(BaseSettings):
    """Secrets and environment-specific overrides. Never put a secret in settings.yaml."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore", case_sensitive=False)

    openai_api_key: str | None = None
    local_llama_base_url: str = "http://localhost:11434/v1"
    local_llama_model: str = "llama3.1"
    local_llama_api_key: str = "ollama"

    dq_data_root: str | None = None
    dq_artifacts_root: str | None = None
    dq_modules_root: str | None = None

    dq_api_host: str | None = None
    dq_api_port: int | None = None


class PathsConfig(BaseModel):
    data_root: Path
    artifacts_root: Path
    modules_root: Path
    great_expectations_root: Path


class LLMProviderConfig(BaseModel):
    base_url: str | None = None
    base_url_env: str | None = None
    model: str | None = None
    model_env: str | None = None
    temperature: float = 0
    api_key_env: str | None = None
    # instructor structured-output mode. "tools" is right for real OpenAI, but
    # llama.cpp/Ollama's OpenAI-compat endpoint routinely returns the tool call
    # as plain message *content* instead of a tool_calls entry, which instructor
    # rejects outright ("does not support multiple tool calls") - throwing away
    # a perfectly good answer. "json" parses the content directly. See
    # openai_compatible.py.
    mode: str = "tools"


class LLMConfig(BaseModel):
    primary_provider: str = "openai"
    fallback_provider: str | None = "local_llama"
    max_retries: int = 2
    request_timeout_seconds: int = 60
    providers: dict[str, LLMProviderConfig] = Field(default_factory=dict)


class SamplingConfig(BaseModel):
    max_sample_rows_per_failure: int = 8
    mask_pii: bool = True


class RemediationConfig(BaseModel):
    fix_promotion_mode: Literal["auto", "manual"] = "auto"
    rule_promotion_mode: Literal["auto", "manual"] = "manual"
    # Convergence loop (see docs/ARCHITECTURE.md section 6.2): False = exactly
    # one RCA -> remediate -> re-validate pass, matching the original
    # single-shot behavior. True = keep repeating until either fully clean,
    # or two consecutive loops leave the identical set of primary keys still
    # failing (no further progress possible) - capped at max_loops either way.
    loop_till_no_exception: bool = True
    max_loops: int = 5
    # Self-repair: when the sandbox or rule validator rejects an LLM artifact,
    # hand the rejection reason back to the LLM and let it correct its own
    # output, up to this many extra attempts per artifact. 0 restores the
    # original one-shot behavior. This is what makes a rejected snippet heal
    # inside the run instead of waiting on someone to improve the prompt.
    max_repair_attempts: int = 2
    # True = a fix must REPAIR the value, never delete the row - for any column.
    # A filter trivially drives unexpected_count to zero, so the efficacy gate
    # alone can't tell "fixed it" from "deleted the evidence", and which one the
    # LLM picks is pure chance (one run imputed 5000 customer_ids, the next
    # dropped them). Turning this on costs convergence on columns that genuinely
    # can't be reconstructed (a malformed customer_email has no right answer):
    # those rows stop being repairable and land in QuarantineData instead.
    repair_not_drop: bool = False
    # Writes the rows still failing when the loop closes to a CSV under
    # artifacts/runs/<run_id>/fileOutput/QuarantineData/ - see
    # remediation/quarantine_export.py.
    export_quarantined: bool = True


class JobsConfig(BaseModel):
    runner: str = "inprocess"
    max_concurrent_runs_per_module: int = 1


class APIConfig(BaseModel):
    host: str = "127.0.0.1"
    port: int = 8000
    cors_origins: list[str] = Field(default_factory=list)


class AppConfig(BaseModel):
    paths: PathsConfig
    llm: LLMConfig
    sampling: SamplingConfig
    remediation: RemediationConfig
    jobs: JobsConfig
    api: APIConfig
    env: EnvSettings


def load_settings(settings_path: Path | None = None) -> AppConfig:
    """Load config/settings.yaml, apply env var overrides, return a validated AppConfig."""
    settings_path = settings_path or (PROJECT_ROOT / "config" / "settings.yaml")
    raw = yaml.safe_load(settings_path.read_text(encoding="utf-8"))
    env = EnvSettings()

    raw["paths"]["data_root"] = env.dq_data_root or raw["paths"]["data_root"]
    raw["paths"]["artifacts_root"] = env.dq_artifacts_root or raw["paths"]["artifacts_root"]
    raw["paths"]["modules_root"] = env.dq_modules_root or raw["paths"]["modules_root"]

    for key in ("data_root", "artifacts_root", "modules_root", "great_expectations_root"):
        p = Path(raw["paths"][key])
        raw["paths"][key] = str(p if p.is_absolute() else (PROJECT_ROOT / p).resolve())

    if env.dq_api_host:
        raw["api"]["host"] = env.dq_api_host
    if env.dq_api_port:
        raw["api"]["port"] = env.dq_api_port

    return AppConfig(**raw, env=env)


# ---------------------------------------------------------------------------
# Module registry: one module.yaml per data domain under modules/<name>/
# ---------------------------------------------------------------------------


class TriggerConfig(BaseModel):
    modes: list[Literal["manual", "scheduled", "event_driven"]] = Field(default_factory=lambda: ["manual"])
    schedule: str | None = None
    watch_path: str | None = None


class ColumnSchemaEntry(BaseModel):
    name: str
    type: Literal["string", "double", "integer", "long", "timestamp", "boolean"]
    nullable: bool = True


class SourceConfig(BaseModel):
    baseline_path: str
    incoming_path: str
    format: str = "csv"
    options: dict = Field(default_factory=lambda: {"header": True})
    columns: list[ColumnSchemaEntry] | None = None


class ModuleConfig(BaseModel):
    name: str
    description: str = ""
    source: SourceConfig
    primary_key: str
    critical_columns: list[str] = Field(default_factory=list)
    trigger: TriggerConfig = Field(default_factory=TriggerConfig)
    fix_promotion_mode: Literal["auto", "manual"] | None = None
    rule_promotion_mode: Literal["auto", "manual"] | None = None
    # Omit any of these to inherit config/settings.yaml's remediation defaults.
    loop_till_no_exception: bool | None = None
    max_loops: int | None = None
    export_quarantined: bool | None = None
    repair_not_drop: bool | None = None

    module_dir: Path | None = None  # populated by the registry, not read from YAML


class ModuleRegistry:
    """Discovers and loads every modules/<name>/module.yaml.

    Adding a new module = adding a new folder here. Nothing in this class,
    or in any pipeline stage, needs to change.
    """

    def __init__(self, modules_root: Path):
        self.modules_root = modules_root
        self._modules: dict[str, ModuleConfig] = {}
        self.reload()

    def reload(self) -> None:
        self._modules.clear()
        if not self.modules_root.exists():
            return
        for module_dir in sorted(self.modules_root.iterdir()):
            manifest = module_dir / "module.yaml"
            if module_dir.is_dir() and manifest.exists():
                raw = yaml.safe_load(manifest.read_text(encoding="utf-8"))
                config = ModuleConfig(**raw, module_dir=module_dir)
                self._modules[config.name] = config

    def get(self, name: str) -> ModuleConfig:
        from dq_framework.core.exceptions import ModuleNotFoundError_

        try:
            return self._modules[name]
        except KeyError as exc:
            raise ModuleNotFoundError_(
                f"No module named '{name}'. Known modules: {sorted(self._modules)}"
            ) from exc

    def list(self) -> list[ModuleConfig]:
        return list(self._modules.values())
