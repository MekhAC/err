from collections import Counter, deque
import threading

class LogStore:
    def __init__(self):
        self.level_count = Counter()
        self.service_count = Counter()
        self.error_logs = deque()  # (timestamp, service, message)
        self.lock = threading.Lock()