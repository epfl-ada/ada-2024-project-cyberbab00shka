import json
import sys
import textwrap
from html import escape


def is_cell_valid(cell):
    source = cell.get("source", [])
    if len(source) == 0:
        return True
    return source[0].strip().lower().find("# ignore") != 0 and source[0].strip().lower().find("#ignore") != 0


def merge_cells(cells):
    if len(cells) == 0:
        return []

    cells = list(filter(lambda x: x['cell_type'] != 'code' or len(x['source']) > 0, cells))

    result = [cells[0]]
    for cell in cells[1:]:
        if (
            result[-1]['cell_type'] == cell['cell_type'] == 'code' and
            len(result[-1]['outputs']) == len(cell['outputs']) == 0
        ):
            result[-1]['source'] += '\n\n#\n\n'
            result[-1]['source'] += cell['source']
        else:
            result.append(cell)
    return result


def overflowing_div(content):
    return (
        "<div style='overflow-x:scroll'>" +
        content +
        "</div>"
    )


def render(cell):
    match cell['cell_type']:
        case 'code':
            return render_code(cell)
        case 'markdown':
            return render_markdown(cell)
    assert False, f"unknown type {cell['cell_type'] = }"


def render_code(cell):
    template = """\
    <details><summary>code</summary>

    ```python
    {code}
    ```

    </details>

    {output}
    """
    template = textwrap.dedent(template)


    output_text = ''
    if len(cell['outputs']) > 0:
        for output in cell['outputs']:
            if output['output_type'] == 'execute_result':
                data = output['data']
                if "text/html" in data:
                    output_text += (
                        '\n' +
                        overflowing_div(''.join(data["text/html"])) +
                        '\n'
                    )
                elif "text/plain" in data:
                    output_text += (
                        "\n<pre>\n"+
                        escape(''.join(data["text/plain"])) +
                        "\n</pre>\n"
                    )
                else:
                    assert False, "unknown cell"
            elif output['output_type'] == 'stream':
                output_text += (
                    "\n<pre>\n"+
                    escape(''.join(output['text'])) +
                    "\n</pre>\n"
                )
            elif output['output_type'] == 'display_data':
                data = output['data']
                if "image/png" in data:
                    output_text += (
                        f'\n<img src="data:image/png;base64, {data["image/png"]}" alt="{"".join(data.get("text/plain", []))}" />\n'
                    )
                else:
                    assert False, f"unknown format: {data.keys() = }"
            elif output['output_type'] == 'error':
                output_text += "\n<span style='color: darkred'>Error!</span>\n"
            else:
                assert False, f"unknown format: {output = }"

    return template.format(
        code=''.join(cell['source']),
        output=output_text,
    )

def render_markdown(cell):
    return (
        "\n" +
        escape(''.join(cell['source'])) +
        "\n"
    )

notebook = sys.argv[1]

with open(notebook) as file:
    notebook = json.load(file)

result_markdown = """\
---
layout: page
title: ""
date:   2024-12-18 14:17:15 +0100
categories: jekyll update
mathjax: true
---

"""

for cell in merge_cells(notebook["cells"]):
    result_markdown += render(cell)


print(result_markdown)
