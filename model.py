from datetime import datetime

class LogEntry:
    def __init__(self, timestamp: datetime , level: str , service: str , message: str  ):
        self.timestamp = timestamp 
        self.level = level
        self.service = service
        self.message = message

    def __repr__(self):
        return f"<timestamp='{self.timestamp}', ErrorLog(level='{self.level}', service='{self.service}', message='{self.message}')>"