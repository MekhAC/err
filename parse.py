import csv
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from info import LogStore


class Parser:
    def __init__(self, log_store: LogStore):
        self.log_store = log_store

        # validation constants
        self.VALID_LEVELS = {"INFO", "WARN", "ERROR"}
        self.VALID_SERVICES = {"auth", "payment", "order"}

    def _process_chunk(self, rows):
        """
        Process a chunk of CSV rows.
        Uses local aggregation to minimize lock contention.
        """

        local_level_count = {}
        local_service_count = {}
        local_error_logs = []

        for row in rows:
            try:
                ts_str, level, service, message = row

                # domain validation
                if level not in self.VALID_LEVELS:
                    continue
                if service not in self.VALID_SERVICES:
                    continue

                # type validation
                ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")

                # local aggregation
                local_level_count[level] = local_level_count.get(level, 0) + 1
                local_service_count[service] = local_service_count.get(service, 0) + 1

                if level == "ERROR":
                    local_error_logs.append((ts, service, message))

            except Exception:
                # invalid row should never crash ingestion
                continue

        # 🔒 single critical section per chunk
        with self.log_store.lock:
            self.log_store.level_count.update(local_level_count)
            self.log_store.service_count.update(local_service_count)
            self.log_store.error_logs.extend(local_error_logs)

    def parse_logs(self, file_path="data.txt", workers=4, chunk_size=5000):
        """
        Stream CSV file and process it using a thread pool.
        """

        with open(file_path, "r", newline="", encoding="utf-8") as file:
            reader = csv.reader(file)
            next(reader, None)  # skip header

            chunk = []
            futures = []

            with ThreadPoolExecutor(max_workers=workers) as executor:
                for row in reader:
                    chunk.append(row)

                    if len(chunk) >= chunk_size:
                        futures.append(
                            executor.submit(self._process_chunk, chunk)
                        )
                        chunk = []

                # process remaining rows
                if chunk:
                    futures.append(
                        executor.submit(self._process_chunk, chunk)
                    )

                # ensure all threads complete
                for future in futures:
                    future.result()
