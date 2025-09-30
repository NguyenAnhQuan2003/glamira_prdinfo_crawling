import json
import logging
import os
from config.dir.address import data_summary_jsonl, data_header
from logs.config_logs import setup_logging
setup_logging()


input_file = data_summary_jsonl
output_file = data_header

all_fields = set()
os.makedirs(data_header, exist_ok=True)
with open(input_file, "r", encoding="utf-8") as infile:
    for i, line in enumerate(infile, 1):
        try:
            data = json.loads(line.strip())
            all_fields.update(data.keys())
            logging.info(f"Processing line {i}")
        except json.JSONDecodeError as e:
            logging.error("Failed to parse ling {}".format(e))
            continue

with open(output_file, "w", encoding="utf-8") as outfile:
    json.dump(list(all_fields), outfile, indent=2)

print(f"Header đã được xuất ra file: {output_file}")