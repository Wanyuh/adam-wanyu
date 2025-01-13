# Flow Log Processor

This project is designed to process flow log data using Redis for tag lookup and count occurrences of tags and port/protocol combinations. The project is split into two parts: **Data Preparation** and **Flow Log Processing**.

### Two Main Parts:
1. **Data Preparation**: This part prepares the Redis lookup table from a CSV file and is handled in the `data_prepare.py` file. It uses the `data_process_starter()` function to load the lookup table into Redis.
2. **Flow Log Processing**: After data preparation, the main flow log processing logic is executed, counting tags and port/protocol combinations from the flow log file.

All test data is located in the `data` directory, and results are saved as screenshots in the `res_screenshots` directory.

## Features

- **Data Preparation**: Loads a CSV lookup table into Redis, mapping destination ports and protocols to tags.
- **Flow Log Processing**: Processes flow logs, maps rows to tags using Redis, and counts occurrences of tags and port/protocol combinations.
- **Efficient File Handling**: Uses memory mapping and batch Redis lookups for efficient handling of large log files.
- **Result Screenshots**: Saves the results of the processing as screenshots in the `res_screenshots` directory.

## Requirements

- Python 3.9+
- Redis server (local or remote)
- `redis` Python library
- `asyncio` for asynchronous execution

## QuickStart
The script **test.py** contains both the data preparation and flow log processing steps. Simply run the script to execute both parts.
This will:
- Load the lookup table into Redis.
- Process the flow logs.
- Save results and screenshots in the **res_screenshots** directory.

## Assumptions Made

1. **Log Format**: The program assumes the flow log format follows the default version, which includes the following fields:
   - **Version**
   - **Account ID**
   - **ENI ID**
   - **Source IP**
   - **Destination IP**
   - **Source Port**
   - **Destination Port**
   - **Protocol**
   - **Packets**
   - **Bytes**
   - **Start & End Time**
   - **Flags**
   - **Status**

2. **Supported Log Version**: The only supported version is **Version 2**, which corresponds to the default log format provided by AWS when exporting flow logs. Any other versions or custom formats are not supported.

3. **Redis**: The program assumes Redis is running locally at the default host (`localhost`) and port (`6379`) for Redis lookups. Redis will be used for fast key-value lookups during the log processing phase.

4. **Flow Log File Format**: The program assumes that the flow log files provided are in the format expected (one log entry per line with the fields mentioned above). If the log file deviates from this format (e.g., missing columns or additional fields), the program may not work as expected.

5. **Chunk Processing**: The program processes flow logs in chunks of 10,000 lines by default. This chunking strategy ensures that large log files can be handled efficiently, though the chunk size can be adjusted if needed.

These assumptions are essential for the correct operation of the program. If any of these conditions change (such as a different log format or Redis configuration), the program may require adjustments to function correctly.


## Author's Thoughts on Tech Choice
I chose **Redis** for this scenario due to the following reasons:
- Fast Lookups: Redis is an in-memory data store, offering extremely fast access to data, which is ideal for high-performance lookups like mapping ports and protocols to tags.
- Simple Key-Value Mapping: Redis natively supports key-value pairs, making it perfect for mapping dstport:protocol to tag.
- Scalable: Redis can easily scale for datasets like ours (up to 10,000 mappings) and remains fast even as the data grows.
- Ease of Use: Redis provides simple, efficient APIs for handling large files and performing fast lookups, making it easy to integrate with Python. 

While **Kafka** and **OpenSearch** are powerful tools, they may be overkill for this straightforward lookup task. However, in a production environment, they could be considered if more complex needs arise. Redis is the best choice for this case due to its speed, simplicity, and scalability.