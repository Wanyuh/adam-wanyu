import logging
from typing import Optional
from redis.asyncio import Redis
import csv
from redis.exceptions import RedisError, ConnectionError


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class RedisDataManager:
    def __init__(self, host: str = 'localhost', port: int = 6379):
        """
        Initialize the RedisLookupManager instance with default Redis connection details.

        Args:
            host (str): The Redis server host (default is 'localhost').
            port (int): The Redis server port (default is 6379).
        """
        self.host = host
        self.port = port
        self.redis_client: Optional[Redis] = None

    async def get_redis_client(self) -> Redis:
        """Initialize and return a Redis client."""
        if self.redis_client is None:
            try:
                self.redis_client = Redis(host=self.host, port=self.port, decode_responses=True)
                # Test connection
                await self.redis_client.ping()
                logging.info("Successfully connected to Redis")
            except ConnectionError as e:
                logging.error("Failed to connect to Redis: %s", e)
                raise
        return self.redis_client

    async def load_lookup_table(self, lookup_file: str) -> None:
        """Load lookup table from CSV file into Redis."""
        if not self.redis_client:
            await self.get_redis_client()

        if self.redis_client:
            try:
                with open(lookup_file, 'r', encoding='ascii') as file:
                    reader = csv.DictReader(file)
                    for row in reader:
                        try:
                            # Case insensitive key and tag
                            key = f"{row['dstport']}:{row['protocol'].lower()}"
                            tag = row['tag'].lower()

                            # Set the tag as a string for the key
                            await self.redis_client.set(key, tag)
                        except KeyError as e:
                            logging.warning("Missing expected column in CSV row: %s | Row: %s", e, row)
                        except RedisError as e:
                            logging.error("Error pushing data to Redis: %s", e)
                logging.info("Lookup table successfully loaded into Redis")
            except FileNotFoundError as e:
                logging.error("Lookup file not found: %s", e)
                raise
            except IOError as e:
                logging.error("Error reading lookup file: %s", e)
                raise


async def data_process_starter(lookup_file):
    manager = RedisDataManager()
    await manager.load_lookup_table(lookup_file)
