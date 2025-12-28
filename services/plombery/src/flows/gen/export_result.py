# Incremental report and dashboard generator using LLM
# Processes all rows from datasource_analysis in batches, updating report and dashboard in memory, then writes to disk


import os
from string import Template
from ...utils._db import SessionLocal, DatasourceAnalysis
from ...utils._ai import ask_llm

# Configurable batch size
BATCH_SIZE = 10

# Output file paths
REPORT_PATH = "report.md"
DASHBOARD_PATH = "dashboard.py"

# Prompt template paths
PROMPT_MARKDOWN_PATH = os.path.join(os.path.dirname(__file__), "prompt_markdown.txt")
PROMPT_DASHBOARD_PATH = os.path.join(os.path.dirname(__file__), "prompt_dashboard.txt")

def fetch_analysis_rows(session):
	"""Fetch up to 10 rows from datasource_analysis ordered by id (testing purpose)."""
	return session.query(DatasourceAnalysis).order_by(DatasourceAnalysis.id).limit(10).all()

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

def rows_to_dashboard(batch_rows):
	return "\n".join([
		f"- Topics: {row.topics}; Keywords: {row.keywords}; Emerging Algorithms: {row.emerging_algorithms}; Impact: {row.impact}; Date: {getattr(row, 'date', '')}"
		for row in batch_rows
	])


def generate_report(session, prompt_markdown):
	"""Generate the markdown report using LLM in batches."""
	md_report = ""
	all_rows = fetch_analysis_rows(session)
	total_rows = len(all_rows)
	if not all_rows:
		print("No analysis rows found for report.")
		return ""
	print(f"Total rows to process for report: {total_rows}")
	num_batches = (total_rows + BATCH_SIZE - 1) // BATCH_SIZE
	for i, batch in enumerate(batch_iterable(all_rows, BATCH_SIZE), 1):
		print(f"Processing report batch {i}/{num_batches} (rows {((i-1)*BATCH_SIZE)+1}-{min(i*BATCH_SIZE, total_rows)})...")
		rows_md = rows_to_markdown(batch)
		report_prompt = prompt_markdown.substitute(current_report=md_report or "(empty)", rows=rows_md)
		try:
			md_report = ask_llm(report_prompt)
		except Exception as e:
			print(f"Error updating report with LLM: {e}")
			continue
		print(f"Finished report batch {i}/{num_batches}.")
	return md_report

def generate_dashboard(session, prompt_dashboard):
	"""Generate the dashboard HTML using LLM in batches."""
	dashboard_html = ""
	all_rows = fetch_analysis_rows(session)
	total_rows = len(all_rows)
	if not all_rows:
		print("No analysis rows found for dashboard.")
		return ""
	print(f"Total rows to process for dashboard: {total_rows}")
	num_batches = (total_rows + BATCH_SIZE - 1) // BATCH_SIZE
	for i, batch in enumerate(batch_iterable(all_rows, BATCH_SIZE), 1):
		print(f"Processing dashboard batch {i}/{num_batches} (rows {((i-1)*BATCH_SIZE)+1}-{min(i*BATCH_SIZE, total_rows)})...")
		rows_dash = rows_to_dashboard(batch)
		dashboard_prompt = prompt_dashboard.substitute(current_dashboard=dashboard_html or "(empty)", rows=rows_dash)
		try:
			dashboard_html = ask_llm(dashboard_prompt)
		except Exception as e:
			print(f"Error updating dashboard with LLM: {e}")
			continue
		print(f"Finished dashboard batch {i}/{num_batches}.")
	return dashboard_html

def main():
	# Load prompt templates
	prompt_markdown = load_template(PROMPT_MARKDOWN_PATH)
	prompt_dashboard = load_template(PROMPT_DASHBOARD_PATH)

	session = SessionLocal()
	try:
		# Generate report first
		md_report = generate_report(session, prompt_markdown)
		# Then generate dashboard
		dashboard_html = generate_dashboard(session, prompt_dashboard)

		# Write outputs to disk
		print("Writing outputs to disk...")
		with open(REPORT_PATH, "w", encoding="utf-8") as f:
			f.write(md_report)
		with open(DASHBOARD_PATH, "w", encoding="utf-8") as f:
			f.write(dashboard_html)
		print(f"Report written to {REPORT_PATH}")
		print(f"Dashboard written to {DASHBOARD_PATH}")
	finally:
		session.close()

if __name__ == "__main__":
	main()

