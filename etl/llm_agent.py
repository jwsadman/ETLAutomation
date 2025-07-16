
import os
import requests

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"

def query_groq(prompt):
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }
    data = {
        "model": "mixtral-8x7b-32768",  # or llama3-70b
        "messages": [{"role": "user", "content": prompt}]
    }
    res = requests.post(GROQ_URL, json=data, headers=headers)
    res.raise_for_status()
    return res.json()['choices'][0]['message']['content']
