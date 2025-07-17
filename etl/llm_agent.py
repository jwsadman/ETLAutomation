
import requests
from typing import Optional
from .config import GROQ_API_KEY, GROQ_MODEL

GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"

class GroqError(RuntimeError):
    pass

def query_groq(
    prompt: str,
    *,
    model: Optional[str] = None,
    max_tokens: int = 2048,
    temperature: float = 0.2,
    system: str = "You are a data cleaning and transformation assistant. Respond EXACTLY in the requested format."
) -> str:
    if not GROQ_API_KEY:
        raise GroqError("Missing GROQ_API_KEY. Set it in your .env.")

    model = model or GROQ_MODEL
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": prompt}
        ],
        "temperature": temperature,
        "max_tokens": max_tokens
    }
    resp = requests.post(GROQ_URL, headers=headers, json=payload, timeout=60)
    if not resp.ok:
        raise GroqError(f"Groq API error {resp.status_code}: {resp.text}")
    data = resp.json()
    try:
        return data["choices"][0]["message"]["content"]
    except Exception as e:
        raise GroqError(f"Unexpected Groq response: {data}") from e
