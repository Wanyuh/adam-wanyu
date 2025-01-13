import os
import asyncio
from data_prepare import data_process_starter
from flow_log_process import flow_log_process_starter, save_csv_as_plain_text

current_file_dir = os.path.dirname(os.path.abspath(__file__))
FLOW_LOGS_PATH = os.path.join(current_file_dir, 'data', 'flow_logs.txt')
LOOKUP_PATH = os.path.join(current_file_dir, 'data', 'lookup.csv')

# Exmaple output path
DOWNLOAD_PATH = os.path.expanduser('~/Downloads')


if __name__ == "__main__":

    # Step1: save lookup table in redis
    asyncio.run(data_process_starter(LOOKUP_PATH))

    # Step2: flow log logic
    tag_counts, port_protocol_counts = asyncio.run(flow_log_process_starter())

    # Step3: save results to text file
    save_csv_as_plain_text(DOWNLOAD_PATH, tag_counts, 'tag_counts.txt')
    save_csv_as_plain_text(DOWNLOAD_PATH, port_protocol_counts, 'port_protocol_counts.txt')