import json
from string import Template
from plombery import task, get_logger, Trigger, register_pipeline

from ...utils._db import SessionLocal, Datasource
from ...utils._tools import insert_datasource_analysis, exists_analysis_by_datasource_id
from ...utils._ai import ask_llm
import os
from pydantic import BaseModel, Field


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
async def main():
    import asyncio

    logger = get_logger()
    prompt_template = Template(load_prompt())
    session = SessionLocal()
    datasources = []
    try:
        # Get all unanalyzed datasources (blocking, so run in thread)
        datasources = await asyncio.to_thread(
            lambda: session.query(Datasource).filter(Datasource.analyzed == False).all()
        )
        logger.info(f"Found {len(datasources)} unanalyzed datasources.")
        for ds in datasources:
            prompt = prompt_template.substitute(
                title=ds.title, abstract=ds.abstract_or_summary or ""
            )

            # Run ask_llm in a thread
            response = await asyncio.to_thread(ask_llm, prompt)
            try:
                result = json.loads(response)
            except Exception as e:
                logger.error(
                    f"Failed to parse LLM response for datasource {ds.id}: {e}\nResponse: {response}"
                )
                continue

            # Prepare data for insertion
            analysis_data = {
                "datasource_id": ds.id,
                "topics": ", ".join(result.get("topics", [])),
                "keywords": ", ".join(result.get("keywords", [])),
                "emerging_algorithms": ", ".join(result.get("emerging_algorithms", [])),
                "summary": result.get("summary"),
                "impact": result.get("impact"),
            }

            # Check if analysis already exists (run in thread)
            exists = await asyncio.to_thread(exists_analysis_by_datasource_id, ds.id)
            if exists:
                logger.info(
                    f"Analysis already exists for datasource {ds.id}, skipping."
                )
                continue

            # Insert analysis (run in thread)
            inserted = await asyncio.to_thread(
                insert_datasource_analysis, analysis_data
            )
            if inserted:
                # Mark datasource as analyzed (run in thread)
                def mark_analyzed():
                    ds.analyzed = True
                    session.commit()

                await asyncio.to_thread(mark_analyzed)
                logger.info(f"Analyzed datasource {ds.id} ({ds.title})")
            else:
                logger.info(
                    f"Analysis for datasource {ds.id} was not inserted (duplicate or error)."
                )

    except Exception as e:
        logger.error(str(e))
    finally:
        session.close()
    return {"analyzed": len(datasources)}


import httpx


@task
async def trigger_report_generation():
    # Trigger Report generation after analysis
    logger = get_logger()
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f'{os.getenv("HOST")}/api/pipelines/report_generation/run',
                json={"params": {}},
                timeout=10,
            )

        logger.info(f"Report generation triggered: {response.status_code}")
        try:
            return response.json()
        except Exception:
            logger.warning(f"Non-JSON response: {response.text}")
            return {"status_code": response.status_code, "text": response.text}
    except Exception as e:
        logger.error(f"Failed to trigger Report generation: {e}")
        return {"error": str(e)}


register_pipeline(
    id="datasource_analysis_llm",
    description="Analyze unanalyzed data sources using LLM and save results.",
    tasks=[main, trigger_report_generation],
    triggers=[
        # Trigger(
        #     id="manual",
        #     name="Manual",
        #     description="Run the analysis manually",
        #     schedule=ManualTrigger(),
        # ),
    ],
    params=InputParams,
)
