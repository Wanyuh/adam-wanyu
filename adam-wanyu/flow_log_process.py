import logging
import mmap
import asyncio
import os
from collections import defaultdict
from redis.asyncio import Redis
from data_prepare import RedisDataManager
current_file_dir = os.path.dirname(os.path.abspath(__file__))
FLOW_LOGS_PATH = os.path.join(current_file_dir, 'data', 'flow_logs.txt')


# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Protocol mapping
PROTOCOL_MAP = {
    1: "icmp",
    6: "tcp",
    17: "udp",
    41: "ipv6_encapsulation",
    47: "gre",
    50: "esp",
    51: "ah",
}

class FlowLogProcessor:
    def __init__(self, redis_client: Redis, flow_log_file: str, chunk_size: int = 10_000):
        """
        Initialize the FlowLogProcessor with required parameters.

        Args:
            redis_client (Redis): The Redis client to use for data fetching.
            flow_log_file (str): Path to the flow log file to process.
            chunk_size (int): Number of lines per chunk to process (default is 10_000).
        """
        self.redis_client = redis_client
        self.flow_log_file = flow_log_file
        self.chunk_size = chunk_size

    async def process_chunk(self, chunk):
        """
        Process a chunk of flow log lines, map rows to tags using Redis batch lookup, and count tag occurrences.

        Args:
            chunk (list[str]): Lines from the chunk to process.

        Returns:
            tuple: Tag counts and port/protocol counts.
        """
        tag_counts = defaultdict(int)
        port_protocol_counts = defaultdict(int)

        keys = []
        for line in chunk:
            fields = line.split()
            if len(fields) < 11:
                logging.warning("Invalid log entry: %s", line)
                continue

            dstport = fields[8]
            protocol_number = int(fields[7])
            protocol = PROTOCOL_MAP.get(protocol_number, "unknown").lower()

            # Query Redis for the tag key
            key = f"{dstport}:{protocol}"
            keys.append(key)

            # Count port/protocol combinations
            port_protocol_counts[f"{dstport}/{protocol}"] += 1

        # Perform a batch Redis lookup for tags
        tags_batch = await self.redis_client.mget(*keys)

        # Process the tags from the batch lookup
        for tag in tags_batch:
            # Check if the tag is None or invalid, treat it as "Untagged"
            if tag is None:
                tag_counts["Untagged"] += 1
            else:
                # Decode tag if it's not None
                try:
                    tag_counts[tag] += 1
                except (AttributeError, UnicodeDecodeError) as e:
                    # Log unexpected cases where decoding fails
                    logging.warning(f"Failed to decode tag: {tag} | Error: {e}")
                    tag_counts["Untagged"] += 1

        return tag_counts, port_protocol_counts

    async def parse_flow_log(self):
        """
        Parse the flow log file using memory mapping, divide into chunks, and process concurrently with batch Redis lookups.

        Returns:
            tuple: CSV strings for tag counts and port/protocol counts.
        """
        try:
            with open(self.flow_log_file, "r+b") as file:
                with mmap.mmap(file.fileno(), length=0, access=mmap.ACCESS_READ) as mmapped_file:
                    # Read all lines into memory-mapped file
                    lines = mmapped_file.read().decode("ascii").splitlines()

            # Divide lines into chunks
            chunks = [lines[i:i + self.chunk_size] for i in range(0, len(lines), self.chunk_size)]

            # Process chunks concurrently
            tasks = [self.process_chunk(chunk) for chunk in chunks]
            results = await asyncio.gather(*tasks)

            # Combine results from all chunks
            combined_tag_counts = defaultdict(int)
            combined_port_protocol_counts = defaultdict(int)
            for tag_counts, port_protocol_counts in results:
                for tag, count in tag_counts.items():
                    combined_tag_counts[tag] += count
                for port_protocol, count in port_protocol_counts.items():
                    combined_port_protocol_counts[port_protocol] += count

            # Generate result text
            tag_counts_csv = "Tag,Count\n" + "\n".join(
                f"{tag},{count}" for tag, count in combined_tag_counts.items()
            )
            port_protocol_counts_csv = (
                "Port,Protocol,Count\n" +
                "\n".join(
                    f"{port_protocol.split('/')[0]},{port_protocol.split('/')[1]},{count}"
                    for port_protocol, count in combined_port_protocol_counts.items()
                )
            )

            return tag_counts_csv, port_protocol_counts_csv

        except FileNotFoundError as e:
            logging.error("Flow log file not found: %s", e)
            raise
        except Exception as e:
            logging.error("Error processing flow log: %s", e)
            raise

    async def get_flow_log_counts(self):
        """
        Initialize Redis connection, process the flow log, and return the counts.
        """
        try:
            tag_counts, port_protocol_counts = await self.parse_flow_log()
            return tag_counts, port_protocol_counts
        except Exception as e:
            logging.error("An error occurred while processing flow log: %s", e)
            raise

def save_csv_as_plain_text(save_path, csv_string, file_name):
    """
    Save a CSV-like string as plain text.

    Args:
        csv_string (str): The CSV-like input string.
        file_name (str): The name of the file to save the plain text.
    """
    try:
        file_path = os.path.join(save_path, file_name)

        # Process the CSV-like string
        lines = csv_string.strip().split("\n")
        plain_text_lines = []

        for line in lines[1:]:  # Skip the header
            plain_text_lines.append(" ".join(line.split(",")))  # Replace commas with spaces

        # Save the processed plain text to a file
        with open(file_path, "w", encoding="utf-8") as file:
            file.write("\n".join(plain_text_lines))

        logging.info(f"Plain text saved to {file_name}")

    except Exception as e:
        logging.info(f"Error saving plain text: {e}")

async def flow_log_process_starter():
    manager = RedisDataManager()
    redis_client = await manager.get_redis_client()
    flow_log_processor = FlowLogProcessor(redis_client=redis_client, flow_log_file=FLOW_LOGS_PATH)
    tag_counts, port_protocol_counts = await flow_log_processor.get_flow_log_counts()

    return tag_counts, port_protocol_counts
