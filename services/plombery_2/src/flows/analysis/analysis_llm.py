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
    import gc

    logger = get_logger()
    prompt_template = Template(load_prompt())

    BATCH_SIZE = 10
    CONCURRENCY = 1  # Changed to 1 to respect 8 req/min rate limit with 10s delay
    total_analyzed = 0
    
    logger.info(f"Starting analysis with CONCURRENCY={CONCURRENCY}, BATCH_SIZE={BATCH_SIZE}")

    while True:
        session = SessionLocal()
        try:
            # Fetch a batch of unanalyzed datasources
            datasources = await asyncio.to_thread(
                lambda: session.query(Datasource)
                .filter(Datasource.analyzed == False)
                .limit(BATCH_SIZE)
                .all()
            )

            if not datasources:
                logger.info("No more datasources to analyze. Analysis complete.")
                break

            logger.info(f"Processing batch of {len(datasources)} datasources (IDs: {[ds.id for ds in datasources]})")

            # Extract data to avoid threading issues with SQLAlchemy objects
            ds_items = [(ds.id, ds.title, ds.abstract_or_summary) for ds in datasources]

            sem = asyncio.Semaphore(CONCURRENCY)

            async def process_one(item):
                ds_id, title, abstract = item
                async with sem:
                    try:
                        logger.info(f"Starting analysis for datasource {ds_id}: {title[:50]}...")
                        prompt = prompt_template.substitute(
                            title=title, abstract=abstract or ""
                        )

                        # Run ask_llm in a thread
                        response = await asyncio.to_thread(ask_llm, prompt)
                        logger.info(f"Received LLM response for datasource {ds_id}")
                        try:
                            result = json.loads(response)
                        except Exception as e:
                            logger.error(
                                f"Failed to parse LLM response for datasource {ds_id}: {e}"
                            )
                            return None

                        analysis_data = {
                            "datasource_id": ds_id,
                            "topics": ", ".join(result.get("topics", [])),
                            "keywords": ", ".join(result.get("keywords", [])),
                            "emerging_algorithms": ", ".join(
                                result.get("emerging_algorithms", [])
                            ),
                            "summary": result.get("summary"),
                            "impact": result.get("impact"),
                        }
                        return (ds_id, analysis_data)
                    except Exception as e:
                        logger.error(f"Error processing datasource {ds_id}: {e}")
                        return None

            results = await asyncio.gather(*(process_one(item) for item in ds_items))

            # Free items memory
            del ds_items

            # Process results sequentially in session
            for res in results:
                if not res:
                    continue
                ds_id, analysis_data = res

                # Find the datasource object by id
                ds = session.query(Datasource).filter(Datasource.id == ds_id).first()
                if not ds:
                    continue

                # Check if analysis already exists (using session)
                if exists_analysis_by_datasource_id(ds_id, session=session):
                    logger.info(
                        f"Analysis already exists for datasource {ds_id}, skipping."
                    )
                    ds.analyzed = True
                    continue

                # Insert analysis (using session)
                if insert_datasource_analysis(analysis_data, session=session):
                    ds.analyzed = True
                    total_analyzed += 1
                    logger.info(f"✓ Successfully analyzed and saved datasource {ds_id} (total: {total_analyzed})")
                else:
                    logger.warning(f"Analysis for datasource {ds_id} was not inserted.")

            # Free results memory
            del results

            # Commit batch
            await asyncio.to_thread(session.commit)

        except Exception as e:
            logger.error(str(e))
            session.rollback()
        finally:
            session.close()
            gc.collect()

    return {"analyzed": total_analyzed}


import httpx


@task
async def trigger_report_generation():
    # Trigger Report generation after analysis
    logger = get_logger()
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f'{os.getenv("HOST2")}/api/pipelines/report_generation/run',
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
