"""The one LLMProvider implementation, parameterized by base_url/api_key/model.

Structured output is enforced by `instructor` (tool-calling under the hood,
with automatic re-prompt-and-retry on schema validation failure) rather than
hand-parsed JSON - this is what "handling non-deterministic responses
gracefully" means in practice: a model that returns slightly-off JSON gets
one or two corrective retries before we give up, instead of a bare
`json.loads()` crash.
"""
from __future__ import annotations

from typing import TypeVar

import instructor
from openai import OpenAI
from pydantic import BaseModel

from dq_framework.core.exceptions import LLMProviderError

T = TypeVar("T", bound=BaseModel)

# "tools" is instructor's default and is the most reliable path on real OpenAI.
# Local llama.cpp/Ollama servers advertise tool-calling but frequently answer
# with the call serialized into message content, which instructor discards -
# "json" reads that content directly. Configured per provider in settings.yaml.
_INSTRUCTOR_MODES = {
    "tools": instructor.Mode.TOOLS,
    "json": instructor.Mode.JSON,
}


class OpenAICompatibleProvider:
    def __init__(
        self,
        name: str,
        base_url: str,
        api_key: str,
        model: str,
        temperature: float = 0,
        timeout: int = 60,
        max_retries: int = 2,
        mode: str = "tools",
    ):
        self.name = name
        self.model = model
        self.temperature = temperature
        self.max_retries = max_retries
        if mode not in _INSTRUCTOR_MODES:
            raise LLMProviderError(
                f"[{name}] unknown instructor mode '{mode}'; expected one of {sorted(_INSTRUCTOR_MODES)}."
            )
        raw_client = OpenAI(base_url=base_url, api_key=api_key or "not-needed", timeout=timeout)
        self._client = instructor.from_openai(raw_client, mode=_INSTRUCTOR_MODES[mode])

    def generate_structured(self, system_prompt: str, user_prompt: str, response_model: type[T]) -> T:
        try:
            return self._client.chat.completions.create(
                model=self.model,
                temperature=self.temperature,
                max_retries=self.max_retries,
                response_model=response_model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
            )
        except Exception as exc:
            raise LLMProviderError(f"[{self.name}] structured generation failed: {exc}") from exc
