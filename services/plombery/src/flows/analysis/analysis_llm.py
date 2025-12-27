import json
from string import Template
from plombery import task, get_logger, Trigger, register_pipeline

from ...utils._db import SessionLocal, Datasource, DatasourceAnalysis
from ...utils._ai import ask_llm

PROMPT_PATH = "src/prompt.txt"

def load_prompt():
    with open(PROMPT_PATH, "r") as f:
        return f.read()

@task
async def main():
    logger = get_logger()
    session = SessionLocal()
    prompt_template = Template(load_prompt())
    try:
        datasources = (
            session.query(Datasource).filter(Datasource.analyzed == False).all()
        )
        logger.info(f"Found {len(datasources)} unanalyzed datasources.")
        for ds in datasources:
            prompt = prompt_template.substitute(
                title=ds.title, abstract=ds.abstract_or_summary or ""
            )

            response = ask_llm(prompt)
            try:
                result = json.loads(response)
            except Exception as e:
                logger.error(
                    f"Failed to parse LLM response for datasource {ds.id}: {e}\nResponse: {response}"
                )
                continue
            analysis = DatasourceAnalysis(
                datasource_id=ds.id,
                topics=", ".join(result.get("topics", [])),
                keywords=", ".join(result.get("keywords", [])),
                emerging_algorithms=", ".join(result.get("emerging_algorithms", [])),
                summary=result.get("summary"),
                impact=result.get("impact"),
            )
            session.add(analysis)
            ds.analyzed = True
            session.commit()
            logger.info(f"Analyzed datasource {ds.id} ({ds.title})")

    except Exception as e:
        logger.error(str(e))

    finally:
        session.close()
    return {"analyzed": len(datasources)}

register_pipeline(
    id="datasource_analysis_llm",
    description="Analyze unanalyzed data sources using LLM and save results.",
    tasks=[main],
    triggers=[
        # Trigger(
        #     id="manual",
        #     name="Manual",
        #     description="Run the analysis manually",
        #     schedule=ManualTrigger(),
        # ),
    ],
    params=None,
)
