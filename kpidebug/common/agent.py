from __future__ import annotations

import asyncio
import logging

from google.adk.agents import Agent
from google.adk.models.google_llm import Gemini
from google.adk.models.lite_llm import LiteLlm
from google.adk.runners import InMemoryRunner
from google.genai import types as genai_types

logger = logging.getLogger(__name__)

AGENT_MODEL = "anthropic/claude-haiku-4-5-20251001"
DEFAULT_MAX_TURNS = 20


def run_adk_agent(
    agent: Agent, prompt: str, max_turns: int = DEFAULT_MAX_TURNS,
) -> str:
    """Run an ADK agent with an in-memory session and return the
    final text response.

    Creates a fresh InMemoryRunner and session, sends the prompt,
    collects events, and returns the last text output from the agent.
    Stops after max_turns LLM round trips to prevent runaway agents.
    """
    runner = InMemoryRunner(
        agent=agent, app_name="kpidebug",
    )
    session = asyncio.run(
        runner.session_service.create_session(
            app_name="kpidebug",
            user_id="system",
        )
    )

    user_content = genai_types.Content(
        role="user",
        parts=[genai_types.Part(text=prompt)],
    )

    final_text = ""
    llm_turns = 0
    for event in runner.run(
        user_id="system",
        session_id=session.id,
        new_message=user_content,
    ):
        if event.content and event.content.parts:
            if event.author == agent.name:
                llm_turns += 1
                for part in event.content.parts:
                    if part.text:
                        final_text = part.text
                if llm_turns >= max_turns:
                    logger.warning(
                        "Agent '%s' hit max turns (%d), "
                        "stopping",
                        agent.name, max_turns,
                    )
                    break

    return final_text


def make_model(model: str = AGENT_MODEL) -> Gemini | LiteLlm:
    """Create an LLM model instance for use with ADK agents.

    For Gemini models (no prefix or "gemini/" prefix), returns a
    native Gemini instance with built-in retry support. For any
    other provider, uses LiteLLM as a bridge.

    To switch providers, change AGENT_MODEL at the top of this file:
      - "gemini-2.5-flash-lite"          (native Gemini)
      - "anthropic/claude-haiku-4-5-20251001"  (via LiteLLM)
      - "openai/gpt-4o-mini"             (via LiteLLM)

    Set the corresponding API key in .env (GOOGLE_API_KEY,
    ANTHROPIC_API_KEY, OPENAI_API_KEY, etc.).
    """
    if "/" not in model or model.startswith("gemini/"):
        gemini_name = model.removeprefix("gemini/")
        return Gemini(
            model=gemini_name,
            retry_options=genai_types.HttpRetryOptions(
                attempts=5,
                initial_delay=2.0,
                max_delay=30.0,
            ),
        )
    return LiteLlm(model=model, num_retries=3)
