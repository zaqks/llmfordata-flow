from src.flows.gen.report_generation import *
from src.flows.gen.charts_generation import *
from src.flows.gen.result_generation import *

# main()
generate_all_charts()
append_charts_section("report.md")
