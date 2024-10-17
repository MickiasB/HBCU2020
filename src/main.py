import os
# import click

from data import generate

# @click.group()
# def entry_point():
#     pass


if __name__ == "__main__":
    options = {
        "vuln": {
            "function": generate.insert_vulnerability_data
        }
    }
    func_name = os.environ.get("ACTION", "")
    job = options.get(func_name, {}).get("function", None)
    if job is None:
        print(f"No job found for function key: {func_name}")
    else:
        job()