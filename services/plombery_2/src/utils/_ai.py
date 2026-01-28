import os
import time
from openai import OpenAI

client = OpenAI(
    base_url="https://openrouter.ai/api/v1", api_key=os.getenv("OPENROUTER_API_KEY")
)


def ping_llm():
    """
    Ping the LLM service to check connectivity and availability.
    Raises an exception if the LLM service is unavailable.
    """
    try:
        response = ask_llm("Hello world")
        return response
    except Exception as e:
        raise Exception(f"LLM service is unavailable: {e}")


def ask_llm(question):
    # Delay to respect 8 req/min rate limit (7.5s between requests minimum)
    # Using 10s to be safe with CONCURRENCY=1
    time.sleep(10)

    completion = client.chat.completions.create(
        extra_headers={
            "HTTP-Referer": "<YOUR_SITE_URL>",  # Optional. Site URL for rankings on openrouter.ai.
            "X-Title": "<YOUR_SITE_NAME>",  # Optional. Site title for rankings on openrouter.ai.
        },
        extra_body={},
        model=os.getenv("OPENROUTER_MODEL"),
        messages=[{"role": "user", "content": question}],
    )
    return completion.choices[0].message.content



