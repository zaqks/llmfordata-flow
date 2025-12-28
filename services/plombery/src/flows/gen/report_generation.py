"""
Flow: report_generation
Tasks: chat_generation, result_generation, report_concat
"""

from plombery import task, get_logger, Trigger, register_pipeline
from apscheduler.triggers.interval import IntervalTrigger


from .report_llm import *
from .charts_generation import *
from .report_concat import *

from ...utils._db import SessionLocal
from pydantic import BaseModel, Field


# Input parameters for the flow
class InputParams(BaseModel):
    num_rows: int = Field(4, description="Number of rows to use for report generation.")
    batch_size: int = Field(4, description="Batch size for report generation.")


# Shared data row fetcher (from report_llm)
def get_shared_rows(num_rows):
    session = SessionLocal()
    try:
        rows = fetch_analysis_rows(session, num_rows)
        return rows
    finally:
        session.close()


# Task 1: Chat Generation
@task
def chat_generation(params: InputParams):
    num_rows = params.num_rows
    batch_size = params.batch_size
    rows = get_shared_rows(num_rows)
    prompt_markdown = load_template(PROMPT_MARKDOWN_PATH)
    session = SessionLocal()
    try:
        md_report = generate_report(session, prompt_markdown, num_rows, batch_size)
        # Write the report to REPORT_PATH
        with open(REPORT_PATH, "w", encoding="utf-8") as f:
            f.write(md_report)
        return f"Report written to {REPORT_PATH}"
    finally:
        session.close()


# Task 2: Result Generation
@task
def result_generation(params: InputParams):
    # num_rows = params.num_rows
    # batch_size = params.batch_size
    # rows = get_shared_rows(num_rows)
    # Generate all charts using the same rows (if needed, adapt charts_generation to accept rows)
    generate_all_charts()  # This uses all data, can be adapted
    return "Charts generated."


# Task 3: Report Concat
@task
def report_concat_task(params: InputParams):
    # Use the same report path as in report_llm
    report_path = REPORT_PATH
    append_charts_section(report_path)
    return f"Charts section appended to {report_path}"


register_pipeline(
    id="report_generation",
    description="Generate a report and charts from analysis data.",
    tasks=[chat_generation, result_generation, report_concat_task],
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
