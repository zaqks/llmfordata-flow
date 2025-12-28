from src.flows.gen.report_llm import *
from src.flows.gen.charts_generation import *
from src.flows.gen.report_concat import *

main()
generate_all_charts()
append_charts_section("report.md")
