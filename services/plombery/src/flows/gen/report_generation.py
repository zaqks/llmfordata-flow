"""
Flow: report_generation
Tasks: report_generation, charts_generation, report_concat
"""

import os
import shutil
from datetime import datetime
from plombery import task, get_logger, Trigger, register_pipeline
from apscheduler.triggers.interval import IntervalTrigger

from .tasks.report_llm import (
    fetch_analysis_rows,
    generate_report,
    load_template,
    PROMPT_MARKDOWN_PATH,
)
from .tasks.charts_generation import generate_all_charts
from .tasks.report_concat import append_charts_section
from ...utils._tools import save_report_and_documents
from ...utils._db import SessionLocal
from pydantic import BaseModel, Field


# Input parameters for the flow
class InputParams(BaseModel):
    num_rows: int = Field(4, description="Number of rows to use for report generation.")
    batch_size: int = Field(4, description="Batch size for report generation.")


# Single task: Full report and charts pipeline


@task
def report_and_charts_pipeline(params: InputParams):
    logger = get_logger()
    num_rows = params.num_rows
    batch_size = params.batch_size

    # 1. Create timestamped folder in /tmp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"/tmp/report_{timestamp}"
    os.makedirs(output_dir, exist_ok=True)
    logger.info(f"Created output directory: {output_dir}")

    # 2. Generate report
    prompt_markdown = load_template(PROMPT_MARKDOWN_PATH)
    session = SessionLocal()
    report_path = os.path.join(output_dir, "report.md")
    try:
        logger.info("Generating report...")
        md_report = generate_report(session, prompt_markdown, num_rows, batch_size)
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(md_report)
        logger.info(f"Report written to {report_path}")
    except Exception as e:
        logger.error(f"Error generating report: {e}")
        raise
    finally:
        session.close()

    # 3. Generate charts (ensure charts are saved in output_dir)
    try:
        logger.info("Generating charts...")
        generate_all_charts(output_dir=output_dir)
        logger.info(f"Charts generated in {output_dir}")
    except Exception as e:
        logger.error(f"Error generating charts: {e}")
        raise

    # 4. Append charts section to report
    try:
        logger.info("Appending charts section to report...")
        append_charts_section(
            os.path.join(output_dir, "report.md"),
            os.path.join(output_dir, "report_charts.md"),
        )
        logger.info(
            f"Charts section appended to {os.path.join(output_dir, 'report_charts.md')}"
        )
    except Exception as e:
        logger.error(f"Error appending charts section: {e}")
        raise

    logger.info(f"Report and charts generated in {output_dir}")

    # 5. save to db
    save_report_and_documents(output_dir, session)
    logger.info(f"Report and charts saved to db")

    shutil.rmtree(output_dir)
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
