"""Builds the primary+fallback LLMProvider chain from AppConfig.

Both providers are real OpenAICompatibleProvider instances; which one is
"primary" vs "fallback" is config (config/settings.yaml `llm.*_provider`),
not a code branch. Missing credentials don't fail at build time (the
underlying OpenAI SDK client doesn't validate keys until a request is
made) - they surface as an LLMProviderError on first call, which is what
lets the fallback chain actually kick in.
"""
from __future__ import annotations

from dq_framework.core.config import AppConfig, LLMProviderConfig
from dq_framework.core.exceptions import LLMAllProvidersExhaustedError, LLMProviderError
from dq_framework.llm.base import T
from dq_framework.llm.openai_compatible import OpenAICompatibleProvider


def _resolve(literal: str | None, env_field: str | None, env_settings) -> str | None:
    if literal:
        return literal
    if env_field:
        return getattr(env_settings, env_field.lower(), None)
    return None


def _build_single(name: str, provider_config: LLMProviderConfig, app_config: AppConfig, max_retries: int):
    base_url = _resolve(provider_config.base_url, provider_config.base_url_env, app_config.env)
    model = _resolve(provider_config.model, provider_config.model_env, app_config.env)
    api_key = getattr(app_config.env, provider_config.api_key_env.lower(), None) if provider_config.api_key_env else None

    if not base_url or not model:
        raise LLMProviderError(f"Provider '{name}' is missing base_url/model configuration.")

    return OpenAICompatibleProvider(
        name=name,
        base_url=base_url,
        api_key=api_key or "",
        model=model,
        temperature=provider_config.temperature,
        timeout=app_config.llm.request_timeout_seconds,
        max_retries=max_retries,
        mode=provider_config.mode,
    )


class FallbackLLMProvider:
    """Tries `primary`; on failure (auth, timeout, exhausted schema retries),
    tries `fallback`. Raises only if both are exhausted."""

    def __init__(self, primary, fallback=None):
        self.primary = primary
        self.fallback = fallback

    def generate_structured(self, system_prompt: str, user_prompt: str, response_model: type[T]) -> tuple[T, str]:
        try:
            return self.primary.generate_structured(system_prompt, user_prompt, response_model), self.primary.name
        except LLMProviderError as primary_exc:
            if self.fallback is None:
                raise LLMAllProvidersExhaustedError(str(primary_exc)) from primary_exc
            try:
                result = self.fallback.generate_structured(system_prompt, user_prompt, response_model)
                return result, self.fallback.name
            except LLMProviderError as fallback_exc:
                raise LLMAllProvidersExhaustedError(
                    f"primary ({self.primary.name}) failed: {primary_exc}; "
                    f"fallback ({self.fallback.name}) failed: {fallback_exc}"
                ) from fallback_exc


def build_llm_provider(app_config: AppConfig) -> FallbackLLMProvider:
    llm_config = app_config.llm
    primary_config = llm_config.providers[llm_config.primary_provider]
    primary = _build_single(llm_config.primary_provider, primary_config, app_config, llm_config.max_retries)

    fallback = None
    if llm_config.fallback_provider:
        fallback_config = llm_config.providers[llm_config.fallback_provider]
        fallback = _build_single(llm_config.fallback_provider, fallback_config, app_config, llm_config.max_retries)

    return FallbackLLMProvider(primary=primary, fallback=fallback)
