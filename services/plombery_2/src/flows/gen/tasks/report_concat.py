import os
import base64


def append_charts_section(md_path, output_path=None):
    """
    Appends a # Charts section with embedded images and captions to the markdown file.
    Args:
        md_path (str): Path to the input markdown file.
        output_path (str): Path to the output markdown file. If None, overwrite input.
    """
    # Read the markdown file
    with open(md_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Chart info: (figure number, title)
    chart_info = [
        (1, "Publications over time"),
        (2, "Publications by source"),
        (3, "Topic frequency distribution"),
        (4, "Keyword frequency (top terms)"),
        (5, "Topic co-occurrence heatmap"),
        (6, "Emerging algorithms by topic"),
        (7, "Emerging algorithms trend over time"),
        (8, "Impact distribution"),
        (9, "Impact by topic"),
        (10, "Source vs impact heatmap"),
    ]

    charts_section = ["\n---\n# Charts\n"]
    for num, title in chart_info:
        fname = f"{num}.png"
        fname = os.path.join(os.path.dirname(md_path), fname)

        if os.path.isfile(fname):
            with open(fname, "rb") as imgf:
                b64 = base64.b64encode(imgf.read()).decode("utf-8")
            ext = os.path.splitext(fname)[1][1:]
            charts_section.append(
                f"|   |\n|---|\n| ![Figure {num}](data:image/{ext};base64,{b64}) |\n| *Figure {num}: {title}* |\n"
            )
        else:
            charts_section.append(f"*Image file '{fname}' not found.*\n")

    new_content = content.rstrip() + "\n" + "\n".join(charts_section)

    if not output_path:
        output_path = md_path.replace(".md", "_charts.md")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(new_content)
    print(f"Charts section appended and written to {output_path}")


if __name__ == "__main__":
    # Default: process report.md in current directory
    append_charts_section("report.md")
