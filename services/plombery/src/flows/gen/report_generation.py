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
from ...utils._db import SessionLocal
from pydantic import BaseModel, Field


# Input parameters for the flow
class InputParams(BaseModel):
    num_rows: int = Field(4, description="Number of rows to use for report generation.")
    batch_size: int = Field(4, description="Batch size for report generation.")


# Single task: Full report and charts pipeline
@task
def report_and_charts_pipeline(params: InputParams):
    num_rows = params.num_rows
    batch_size = params.batch_size

    # 1. Create timestamped folder in /tmp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"/tmp/report_{timestamp}"
    os.makedirs(output_dir, exist_ok=True)

    # 2. Generate report
    prompt_markdown = load_template(PROMPT_MARKDOWN_PATH)
    session = SessionLocal()
    report_path = os.path.join(output_dir, "report.md")
    try:
        md_report = generate_report(session, prompt_markdown, num_rows, batch_size)
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(md_report)
    finally:
        session.close()

    # 3. Generate charts (ensure charts are saved in output_dir)
    # If generate_all_charts supports output_dir, pass it; else, move files after generation
    generate_all_charts(output_dir=output_dir)

    # 4. Append charts section to report
    # Patch: temporarily chdir to output_dir so append_charts_section finds images

    append_charts_section(
        os.path.join(output_dir, "report.md"),
        os.path.join(output_dir, "report_charts.md"),
    )

    return f"Report and charts generated in {output_dir}"


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
