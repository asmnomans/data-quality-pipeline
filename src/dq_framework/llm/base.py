"""LLMProvider abstraction. One implementation class (OpenAICompatibleProvider)
satisfies both "OpenAI API" and "local LLaMA" from the assignment brief -
Ollama serves an OpenAI-compatible /v1 endpoint, so the only difference
between the two is base_url/api_key/model, all config, not code.
"""
from __future__ import annotations

from typing import Protocol, TypeVar

from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


class LLMProvider(Protocol):
    name: str

    def generate_structured(self, system_prompt: str, user_prompt: str, response_model: type[T]) -> T:
        """Return a validated instance of response_model, or raise LLMProviderError."""
        ...
