# Incremental report and dashboard generator using LLM
# Processes all rows from datasource_analysis in batches, updating report and dashboard in memory, then writes to disk


import os
from string import Template
from ...utils._db import SessionLocal, DatasourceAnalysis
from ...utils._ai import ask_llm

# Configurable batch size
BATCH_SIZE = 10

# Output file path
REPORT_PATH = "report.md"

# Prompt template path
PROMPT_MARKDOWN_PATH = os.path.join(os.path.dirname(__file__), "prompt_markdown.txt")

def fetch_analysis_rows(session):
	"""Fetch up to 10 rows from datasource_analysis ordered by id (testing purpose)."""
	return session.query(DatasourceAnalysis).order_by(DatasourceAnalysis.id).limit(4).all()

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

	
	md_report = md_report.replace("```markdown\n", "")
	md_report = md_report.replace("\n```", "")

	return md_report



def main():
	# Load prompt template
	prompt_markdown = load_template(PROMPT_MARKDOWN_PATH)

	session = SessionLocal()
	try:
		# Generate report only
		md_report = generate_report(session, prompt_markdown)

		# Write output to disk
		print("Writing report to disk...")
		with open(REPORT_PATH, "w", encoding="utf-8") as f:
			f.write(md_report)
		print(f"Report written to {REPORT_PATH}")
	finally:
		session.close()

if __name__ == "__main__":
	main()

