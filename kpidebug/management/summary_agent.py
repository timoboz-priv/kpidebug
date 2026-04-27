import asyncio
import io

import httpx
from google.adk.agents import Agent
from google.adk.runners import InMemoryRunner
from google.genai import types as genai_types

from kpidebug.management.artifact_store import AbstractArtifactStore
from kpidebug.management.types import ArtifactType, ProjectArtifact


SUMMARY_INSTRUCTION = """You are a business analyst creating a concise company/project summary.

You have access to tools that let you read the content of project artifacts (URLs and files).
Use these tools to gather information, then produce a summary.

Your summary should be a single page maximum (about 3-5 short paragraphs) covering:
- What this company or project does (core product/service)
- Industry and market positioning
- Maturity stage (startup, growth, established, etc.)
- Key goals or objectives (if evident from the artifacts)
- Any other notable facts a business analyst would want to know (team size, funding, technology, partnerships, etc.)

Be factual and concise. Only include information you can derive from the provided artifacts.
If artifacts provide limited information, work with what's available and note the limitations briefly.

Start by reading all available artifacts using the provided tools, then write the summary.
"""


_artifact_store: AbstractArtifactStore | None = None
_artifacts: list[ProjectArtifact] = []


def read_url_content(url: str) -> str:
    """Fetch and return the text content of a web page at the given URL."""
    try:
        return _fetch_with_playwright(url)
    except Exception:
        pass

    try:
        with httpx.Client(timeout=15, follow_redirects=True) as client:
            response = client.get(url, headers={"User-Agent": "KPIDebug-Bot/1.0"})
            response.raise_for_status()

        content_type = response.headers.get("content-type", "")
        if "html" in content_type:
            return _extract_text_from_html(response.text)
        return response.text[:50000]
    except Exception as e:
        return f"Error fetching URL: {e}"


def _fetch_with_playwright(url: str) -> str:
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url, wait_until="networkidle", timeout=20000)
        text = page.inner_text("body")
        browser.close()

    text = " ".join(text.split())
    return text[:50000]


def read_file_content(artifact_id: str) -> str:
    """Read and return the text content of a file artifact by its artifact ID.
    Supports PDF, Word documents (.docx), and plain text files."""
    if _artifact_store is None:
        return "Error: artifact store not available"

    artifact = None
    for a in _artifacts:
        if a.id == artifact_id:
            artifact = a
            break
    if artifact is None:
        return f"Error: artifact {artifact_id} not found"

    content = _artifact_store.get_file_content(artifact.project_id, artifact_id)
    if content is None:
        return "Error: file content not available"

    mime = artifact.file_mime_type.lower()

    if "pdf" in mime:
        return _extract_text_from_pdf(content)
    elif "wordprocessingml" in mime or "msword" in mime or artifact.file_name.endswith(".docx"):
        return _extract_text_from_docx(content)
    else:
        try:
            return content.decode("utf-8", errors="replace")[:50000]
        except Exception:
            return "Error: unable to decode file as text"


def _extract_text_from_html(html: str) -> str:
    import re
    text = re.sub(r"<script[^>]*>.*?</script>", "", html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:50000]


def _extract_text_from_pdf(content: bytes) -> str:
    try:
        from PyPDF2 import PdfReader
        reader = PdfReader(io.BytesIO(content))
        pages = []
        for page in reader.pages:
            text = page.extract_text()
            if text:
                pages.append(text)
        return "\n\n".join(pages)[:50000]
    except Exception as e:
        return f"Error extracting PDF text: {e}"


def _extract_text_from_docx(content: bytes) -> str:
    try:
        from docx import Document
        doc = Document(io.BytesIO(content))
        paragraphs = [p.text for p in doc.paragraphs if p.text.strip()]
        return "\n\n".join(paragraphs)[:50000]
    except Exception as e:
        return f"Error extracting Word document text: {e}"


def _build_artifact_list_message(artifacts: list[ProjectArtifact]) -> str:
    if not artifacts:
        return "No artifacts are available for this project."

    lines = ["Here are the project artifacts you can read:\n"]
    for a in artifacts:
        if a.type == ArtifactType.URL:
            lines.append(f"- URL: {a.value} (use read_url_content with this URL)")
        else:
            lines.append(
                f"- File: {a.file_name} ({a.file_mime_type}, {a.file_size} bytes) "
                f"(use read_file_content with artifact_id=\"{a.id}\")"
            )
    lines.append("\nRead all artifacts, then write the business summary.")
    return "\n".join(lines)


def generate_summary(
    artifact_store: AbstractArtifactStore,
    project_id: str,
) -> str:
    global _artifact_store, _artifacts
    _artifact_store = artifact_store
    _artifacts = artifact_store.list(project_id)

    try:
        agent = Agent(
            name="summary_agent",
            model="gemini-3.1-flash-lite-preview",
            instruction=SUMMARY_INSTRUCTION,
            description="Generates a business summary from project artifacts.",
            tools=[read_url_content, read_file_content],
        )

        artifact_message = _build_artifact_list_message(_artifacts)

        runner = InMemoryRunner(agent=agent, app_name="kpidebug_summary")
        session = asyncio.run(
            runner.session_service.create_session(
                app_name="kpidebug_summary",
                user_id="system",
            )
        )

        user_content = genai_types.Content(
            role="user",
            parts=[genai_types.Part(text=artifact_message)],
        )

        final_text = ""
        for event in runner.run(
            user_id="system",
            session_id=session.id,
            new_message=user_content,
        ):
            if event.content and event.content.parts:
                for part in event.content.parts:
                    if part.text and event.author == "summary_agent":
                        final_text = part.text

        result = final_text.strip()
        if not result:
            raise RuntimeError("The agent returned an empty summary. Check that GOOGLE_API_KEY is set.")
        return result
    except Exception:
        raise
    finally:
        _artifact_store = None
        _artifacts = []
