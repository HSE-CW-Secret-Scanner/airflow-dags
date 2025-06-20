import requests
from airflow.decorators import task

from scan_dag.utils.llm_prompt import generate_llm_prompt

@task
def analyze_results_with_llm(report: str):
    model = "llama3.1"
    prompt = generate_llm_prompt(report)

    response = requests.post(
        "http://localhost:11434/api/generate",
        json={
            "model": model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": 0.1,
                "top_p": 0.9,
                "max_tokens": 500,
            }
        }
    )

    return response.json()["response"]
