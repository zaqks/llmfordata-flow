# Incremental report and dashboard generator using LLM
# Processes all rows from datasource_analysis in batches, updating report and dashboard in memory, then writes to disk

import gc
import logging
from string import Template
from ....utils._ai import ask_llm

logger = logging.getLogger(__name__)


# Output file path
REPORT_PATH = "report.md"

# Prompt template path
PROMPT_MARKDOWN_PATH = "src/flows/gen/prompt_markdown.txt"


def batch_iterable(iterable, batch_size):
    """Yield successive batches from iterable."""
    total = len(iterable)
    for i in range(0, total, batch_size):
        yield iterable[i:i+batch_size]


def load_template(path):
    with open(path, "r", encoding="utf-8") as f:
        return Template(f.read())


def rows_to_markdown(batch_rows):
    return "\n".join([
        f"- **Topics:** {row.topics}\n  **Keywords:** {row.keywords}\n  **Emerging Algorithms:** {row.emerging_algorithms}\n  **Summary:** {row.summary}\n  **Impact:** {row.impact}\n  **Source:** {getattr(row, 'source', '')}\n  **Date:** {getattr(row, 'date', '')}\n  **Authors:** {getattr(row, 'authors', '')}\n  **URL:** {getattr(row, 'url', '')}"
        for row in batch_rows
    ])


def generate_report(analysis_rows, prompt_markdown, batch_size):
    """Generate the markdown report using LLM in batches."""
    md_report = ""
    total_rows = len(analysis_rows)
    num_batches = (total_rows + batch_size - 1) // batch_size
    
    logger.info(f"Starting report generation: {total_rows} rows in {num_batches} batch(es) (batch_size={batch_size})")
    logger.info(f"Note: Each batch makes 1 LLM API call. With 10s delay, this will take ~{num_batches * 10}s")
    
    for i, batch in enumerate(batch_iterable(analysis_rows, batch_size), 1):
        logger.info(f"[Report Batch {i}/{num_batches}] Processing rows {((i-1)*batch_size)+1}-{min(i*batch_size, total_rows)}...")
        rows_md = rows_to_markdown(batch)
        report_prompt = prompt_markdown.substitute(current_report=md_report or "(empty)", rows=rows_md)
        
        # Free batch data immediately
        del rows_md
        
        try:
            logger.info(f"[Report Batch {i}/{num_batches}] Calling LLM API (with 10s rate limit delay)...")
            md_report = ask_llm(report_prompt)
            logger.info(f"[Report Batch {i}/{num_batches}] ✓ Successfully received LLM response")
        except Exception as e:
            logger.error(f"[Report Batch {i}/{num_batches}] Error updating report with LLM: {e}")
            continue
        finally:
            del report_prompt
            gc.collect()
    
    logger.info(f"Report generation complete: processed {num_batches} batch(es)")

    md_report = md_report.replace("```markdown\n", "")
    md_report = md_report.replace("\n```", "")

    return md_report
