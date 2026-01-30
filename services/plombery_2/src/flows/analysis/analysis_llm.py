import json
import os
import gc
import asyncio
from string import Template

import httpx
from pydantic import BaseModel, Field
from plombery import task, get_logger, register_pipeline

from ...utils._db import SessionLocal, Datasource
from ...utils._tools import insert_datasource_analysis, exists_analysis_by_datasource_id
from ...utils._ai import ask_llm, ping_llm


PROMPT_PATH = "src/flows/analysis/prompt.txt"


def load_prompt():
    with open(PROMPT_PATH, "r") as f:
        return f.read()


class InputParams(BaseModel):
    ai_model: str = Field(os.getenv("OPENROUTER_MODEL", ""), alias="AI_MODEL")
    prompt: str = Field(
        load_prompt(), alias="PROMPT", description="Prompt template for the LLM."
    )


@task
async def ping_llm_check():
    """Check LLM connectivity before starting the analysis."""
    logger = get_logger()
    logger.info("Checking LLM connectivity...")
    try:
        response = await asyncio.to_thread(ping_llm)
        logger.info(f"✓ LLM is available. Response: {response[:50]}...")
        return response
    except Exception as e:
        logger.error(f"✗ LLM service check failed: {e}")
        raise


@task
async def main():
    logger = get_logger()
    prompt_template = Template(load_prompt())

    total_analyzed = 0

    logger.info("Starting analysis for single datasource")

    session = SessionLocal()
    try:
        datasource = await asyncio.to_thread(
            lambda: session.query(Datasource)
            .filter(Datasource.analyzed == False)
            .first()
        )

        if not datasource:
            logger.info("No datasource to analyze. Analysis complete.")
            return {"analyzed": 0}

        if not datasource:
            logger.info("No datasource to analyze. Analysis complete.")
            return {"analyzed": 0}

        logger.info(
            f"Processing datasource {datasource.id}: {datasource.title[:50]}..."
        )

        try:
            prompt = prompt_template.substitute(
                title=datasource.title, abstract=datasource.abstract_or_summary or ""
            )

            response = await asyncio.to_thread(ask_llm, prompt)

            try:
                result = json.loads(response)
            except Exception as e:
                logger.error(
                    f"Failed to parse LLM response for datasource {datasource.id}: {e}"
                )
                return {"analyzed": 0}

            analysis_data = {
                "datasource_id": datasource.id,
                "topics": ", ".join(result.get("topics", [])),
                "keywords": ", ".join(result.get("keywords", [])),
                "emerging_algorithms": ", ".join(
                    result.get("emerging_algorithms", [])
                ),
                "summary": result.get("summary"),
                "impact": result.get("impact"),
            }

            if exists_analysis_by_datasource_id(datasource.id, session=session):
                logger.info(
                    f"Analysis already exists for datasource {datasource.id}, skipping."
                )
                datasource.analyzed = True
            elif insert_datasource_analysis(analysis_data, session=session):
                datasource.analyzed = True
                total_analyzed += 1
                logger.info(
                    f"✓ Successfully analyzed datasource {datasource.id}"
                )
            else:
                logger.warning(
                    f"Analysis for datasource {datasource.id} was not inserted."
                )

        except Exception as e:
            logger.error(f"Error processing datasource {datasource.id}: {e}")

        await asyncio.to_thread(session.commit)

    except Exception as e:
        logger.error(str(e))
        session.rollback()
    finally:
        session.close()
        gc.collect()

    return {"analyzed": total_analyzed}


@task
async def trigger_report_generation():
    logger = get_logger()

    max_retries = 3
    retry_delay = 60  # seconds

    for attempt in range(max_retries):
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.post(
                    f'{os.getenv("HOST2")}/api/pipelines/report_generation/run',
                    json={"params": {}},
                )
                response.raise_for_status()

            logger.info(f"Report generation triggered: {response.status_code}")

            try:
                return response.json()
            except Exception:
                return {
                    "status_code": response.status_code,
                    "text": response.text,
                }

        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(
                    f"Trigger report attempt {attempt + 1} failed: {e}. "
                    f"Retrying in {retry_delay}s..."
                )
                await asyncio.sleep(retry_delay)
            else:
                logger.error(
                    f"Failed to trigger report generation after "
                    f"{max_retries} attempts: {e}"
                )
                return {"error": str(e)}


register_pipeline(
    id="datasource_analysis_llm",
    description="Analyze unanalyzed data sources using LLM and save results.",
    tasks=[ping_llm_check, main, trigger_report_generation],
    params=InputParams,
)
