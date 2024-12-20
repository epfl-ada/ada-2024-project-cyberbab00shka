import json
import sys
import textwrap
from html import escape
import base64
import os
import glob

IMAGES_DIR = os.path.join(
    os.path.dirname(__file__),
    "assets",
    "img",
)


def is_cell_valid(cell):
    source = cell.get("source", [])
    if len(source) == 0:
        return True
    return source[0].strip().lower().find("# ignore") != 0 and source[0].strip().lower().find("#ignore") != 0


def should_render_code(cell):
    source = cell.get("source", [])
    if len(source) == 0:
        return True
    return source[0].strip().lower().find("# hidecode") != 0 and source[0].strip().lower().find("#hidecode") != 0

def merge_cells(cells):
    if len(cells) == 0:
        return []

    cells = list(filter(lambda x: x['cell_type'] != 'code' or len(x['source']) > 0, cells))

    result = [cells[0]]
    for cell in cells[1:]:
        if (
            result[-1]['cell_type'] == cell['cell_type'] == 'code' and
            len(result[-1]['outputs']) == 0
        ):
            result[-1]['source'] += '\n\n#\n\n'
            result[-1]['source'] += cell['source']
            result[-1]['outputs'] += cell['outputs']
        else:
            result.append(cell)
    return result


def overflowing_div(content):
    return (
        "\n<div style='overflow-x:scroll'>\n" +
        content +
        "\n</div>\n"
    )

def wide_div(content):
    return (
        "\n<div class='wide-section'>\n" +
        content +
        "\n</div>\n"
    )

def mathjax_div(content):
    return (
        "\n<div class='mathjax_process'>\n\n" +
        content +
        "\n\n</div>\n"
    )

def raw_context(content):
    return (
        '\n{% raw %}\n<div markdown="0">\n' +
        content +
        '\n</div>\n{% endraw %}\n'
    )


def render(cell):
    match cell['cell_type']:
        case 'code':
            return render_code(cell)
        case 'markdown':
            return render_markdown(cell)
    assert False, f"unknown type {cell['cell_type'] = }"


def save_png_and_get_name(base64_image):
    imname = "generated_" + str(hash(base64_image)) + ".png"
    path = os.path.join(
        IMAGES_DIR,
        imname
    )
    with open(path, "wb") as fh:
        fh.write(base64.b64decode(base64_image.encode()))
    return imname


def save_svg_and_get_name(svg_code):
    imname = "generated_" + str(hash(svg_code)) + ".svg"
    path = os.path.join(
        IMAGES_DIR,
        imname
    )
    with open(path, "w") as fh:
        fh.write(svg_code)
    return imname


def render_code(cell):
    render_code = should_render_code(cell)
    if render_code:
        template = """\
        <details><summary>code</summary>

        ```python
        {code}
        ```

        </details>"""
        template = textwrap.dedent(template)
    else:
        template = ""

    template += """\n{output}\n"""



    output_text = ''
    if len(cell['outputs']) > 0:
        for output in cell['outputs']:
            if output['output_type'] == 'execute_result':
                data = output['data']
                if "text/html" in data:
                    output_text += (
                        '\n' +
                        raw_context(overflowing_div(''.join(data["text/html"]))) +
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
                if "text/html" in data:
                    output_text += (
                        wide_div(
                            raw_context(''.join(
                                    filter(
                                        lambda x: len(x.strip()) > 0,
                                        data["text/html"]
                                    )
                                )
                            )
                        )
                    )
                elif "image/svg+xml" in data:
                    output_text += wide_div(
                        '\n<img src="{{ im_path }}/' +
                        save_svg_and_get_name(''.join(data["image/svg+xml"])) +
                        f'" alt="{"".join(data.get("text/plain", []))}" />\n'
                    )
                elif "image/png" in data:
                    output_text += wide_div(
                        '\n<img class="wider-section" src="{{ im_path }}/' +
                        save_png_and_get_name(data["image/png"]) +
                        f'" alt="{"".join(data.get("text/plain", []))}" />\n'
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
    return mathjax_div(
        "\n" +
        ''.join(cell['source']) +
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

{% assign im_path = site.baseurl | append: "/assets/img/" %}

"""

for hgx in (
    glob.glob(os.path.join(IMAGES_DIR, "generated*.png")) +
    glob.glob(os.path.join(IMAGES_DIR, "generated*.svg"))
):
  os.remove(hgx)

for cell in merge_cells(notebook["cells"]):
    result_markdown += render(cell)


print(result_markdown)
