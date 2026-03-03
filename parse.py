from multiprocessing import Pool, cpu_count
from datetime import datetime
import csv
from collections import deque

VALID_LEVELS = {"INFO", "WARNING", "ERROR"}
VALID_SERVICES = {"auth", "payment", "search", "profile"}


def process_chunk(rows):
    local_level_count = {}
    local_service_count = {}
    local_error_logs = []

    for ts_str, level, service, message in rows:
        if level not in VALID_LEVELS:
            continue
        if service not in VALID_SERVICES:
            continue

        try:
            # Faster than strptime
            ts = datetime.fromisoformat(ts_str)
        except Exception:
            continue

        local_level_count[level] = local_level_count.get(level, 0) + 1
        local_service_count[service] = local_service_count.get(service, 0) + 1

        if level == "ERROR":
            local_error_logs.append((ts, service, message))

    return local_level_count, local_service_count, local_error_logs


def chunk_generator(reader, chunk_size):
    chunk = []
    for row in reader:
        chunk.append(row)
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


class Parser:

    def __init__(self, log_store):
        self.log_store = log_store

    def parse_logs(self, file_path, workers=None, chunk_size=10000):

        if workers is None:
            workers = cpu_count()

        with open(file_path, newline="") as f:
            reader = csv.reader(f)

            with Pool(processes=workers) as pool:

                for level_count, service_count, error_logs in pool.imap_unordered(
                    process_chunk,
                    chunk_generator(reader, chunk_size),
                    chunksize=1
                ):
                    # Merge incrementally (reduces memory spike)
                    self.log_store.level_count.update(level_count)
                    self.log_store.service_count.update(service_count)
                    self.log_store.error_logs.extend(error_logs)

        self.log_store.error_logs = deque(
            sorted(self.log_store.error_logs, key=lambda x: x[0])
        )