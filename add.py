import csv
from datetime import datetime
from random import randrange

with open('data.txt', 'a', newline='') as file:
    writer = csv.writer(file)

    # Generate and append random log entries
    for i in range(10):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        level = ['INFO', 'WARN', 'ERROR'][randrange(3)]
        service = ['auth', 'payment', 'order'][randrange(3)]
        message = f'Random log message {i}'
        writer.writerow([timestamp, level, service, message])