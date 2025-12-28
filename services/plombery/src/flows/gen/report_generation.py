"""
Flow: report_generation
Tasks: report_generation, charts_generation, report_concat
"""

import os
import shutil
from datetime import datetime
import asyncio
from plombery import task, get_logger, Trigger, register_pipeline
from apscheduler.triggers.interval import IntervalTrigger

from ...utils._tools import fetch_analysis_rows
from .tasks.report_llm import (
    generate_report,
    load_template,
    PROMPT_MARKDOWN_PATH,
)
from .tasks.charts_generation import generate_all_charts
from .tasks.report_concat import append_charts_section
from ...utils._tools import save_report_and_documents
from ...utils._db import SessionLocal
from pydantic import BaseModel, Field
import pandas as pd

# Input parameters for the flow


# Loader for the LLM prompt template (async)
def load_llm_prompt():
    with open(PROMPT_MARKDOWN_PATH, "r", encoding="utf-8") as f:
        return f.read()



class InputParams(BaseModel):
    num_rows: int = Field(4, description="Number of rows to use for report generation.")
    batch_size: int = Field(4, description="Batch size for report generation.")
    prompt: str = Field(
        load_llm_prompt(), alias="PROMPT", description="Prompt template for the LLM."
    )


# Single task: Full report and charts pipeline



@task
async def report_and_charts_pipeline(params: InputParams):
    logger = get_logger()
    num_rows = params.num_rows
    batch_size = params.batch_size

    # 1. Create timestamped folder in /tmp (async)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"/tmp/report_{timestamp}"
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, os.makedirs, output_dir, True)
    logger.info(f"Created output directory: {output_dir}")

    # 2. Fetch analysis data rows (sync DB, but can run in executor)
    from ...utils._db import Datasource
    session = SessionLocal()
    try:
        def fetch_rows():
            analysis_rows = fetch_analysis_rows(session, num_rows)
            rows_dicts = []
            for row in analysis_rows:
                d = row.__dict__.copy()
                d.pop('_sa_instance_state', None)
                datasource = session.query(Datasource).filter_by(id=row.datasource_id).first()
                d['source'] = datasource.source if datasource else None
                d['date'] = datasource.date if datasource else None
                rows_dicts.append(d)

            
            df = pd.DataFrame(rows_dicts)
            if 'source' in df.columns:
                df['source'] = df['source'].astype('category')
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'], errors='coerce')
            return analysis_rows, df
        
        analysis_rows, df = await loop.run_in_executor(None, fetch_rows)

    except Exception as e:
        logger.error(f"Error fetching analysis rows: {e}")
        session.close()
        raise
    
    if not len(analysis_rows):
        logger.info("Nothing Interesting for now...")
        return "Not enough data"

    # 3. Generate report (run in executor)
    prompt_markdown = load_template(PROMPT_MARKDOWN_PATH)
    report_path = os.path.join(output_dir, "report.md")
    try:
        logger.info("Generating report...")
        def gen_report():
            return generate_report(analysis_rows, prompt_markdown, batch_size)
        md_report = await loop.run_in_executor(None, gen_report)
        def write_report():
            with open(report_path, "w", encoding="utf-8") as f:
                f.write(md_report)
        await loop.run_in_executor(None, write_report)
        logger.info(f"Report written to {report_path}")
    except Exception as e:
        logger.error(f"Error generating report: {e}")
        session.close()
        raise

    # 4. Generate charts (run in executor)
    try:
        logger.info("Generating charts...")
        await loop.run_in_executor(None, generate_all_charts, df, output_dir)
        logger.info(f"Charts generated in {output_dir}")
    except Exception as e:
        logger.error(f"Error generating charts: {e}")
        session.close()
        raise

    # 5. Append charts section to report (run in executor)
    try:
        logger.info("Appending charts section to report...")
        await loop.run_in_executor(
            None,
            append_charts_section,
            os.path.join(output_dir, "report.md"),
            os.path.join(output_dir, "report_charts.md"),
        )
        logger.info(
            f"Charts section appended to {os.path.join(output_dir, 'report_charts.md')}"
        )
    except Exception as e:
        logger.error(f"Error appending charts section: {e}")
        session.close()
        raise

    logger.info(f"Report and charts generated in {output_dir}")

    # 6. save to db (run in executor)
    await loop.run_in_executor(None, save_report_and_documents, output_dir, session)
    logger.info(f"Report and charts saved to db")


    # Mark used rows as exported and commit
    for row in analysis_rows:
        row.exported = True
    session.commit()

    session.close()
    await loop.run_in_executor(None, shutil.rmtree, output_dir)
    logger.info(f"Cleaned")

    return f"Report and charts generated as Saved to DB"


register_pipeline(
    id="report_generation",
    description="Generate a report and charts from analysis data.",
    tasks=[report_and_charts_pipeline],
    triggers=[
        # Trigger(
        #     id="hourly",
        #     name="Hourly",
        #     description="Run the pipeline every hour",
        #     schedule=IntervalTrigger(hours=1),
        # ),
    ],
    params=InputParams,
)
