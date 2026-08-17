"""The provider's instructor mode is per-provider config, not a constant.

Ollama/llama.cpp advertise tool-calling but often serialize the call into
message content, which instructor's default "tools" mode discards - that
silently zeroed out RCA. Real OpenAI stays on "tools".
"""
import pytest

from dq_framework.core.config import LLMProviderConfig
from dq_framework.core.exceptions import LLMProviderError
from dq_framework.llm.openai_compatible import _INSTRUCTOR_MODES, OpenAICompatibleProvider


def test_defaults_to_tools_so_openai_is_unchanged():
    assert LLMProviderConfig().mode == "tools"


def test_both_modes_are_supported():
    assert set(_INSTRUCTOR_MODES) == {"tools", "json"}


def test_configured_mode_reaches_the_client():
    provider = OpenAICompatibleProvider(
        name="local", base_url="http://127.0.0.1:11434/v1", api_key="x", model="m", mode="json"
    )
    assert provider._client.mode is _INSTRUCTOR_MODES["json"]


def test_unknown_mode_fails_loudly_at_construction():
    with pytest.raises(LLMProviderError, match="unknown instructor mode"):
        OpenAICompatibleProvider(
            name="local", base_url="http://x/v1", api_key="x", model="m", mode="functions"
        )
