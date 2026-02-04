import csv
from datetime import datetime
from info import LogStore

class Parser:
    def __init__(self, LogEntry):
        self.LogEntry = LogEntry

    def parse_logs(self):
            
        VALID_LEVELS = {"INFO", "WARN", "ERROR"}
        VALID_SERVICES = {"auth", "payment", "order"}

        with open('data.csv', 'r') as file:
            reader = csv.reader(file)
            header = next(reader)  # Skip header row
            for row in reader:
                try:
                    ts_str, level, service, message = row

                    if level not in VALID_LEVELS:
                        continue
                    if service not in VALID_SERVICES:
                        continue
                    ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")

                    self.LogEntry.level_count[level]+=1
                    self.LogEntry.service_count[service]+=1

                    if level == "ERROR":
                        self.LogEntry.error_logs.append((ts, service, message))

                except Exception:
                    continue

