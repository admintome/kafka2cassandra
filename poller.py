import sys
from kafka import KafkaConsumer
import json
from cassandra.cluster import Cluster

consumer = KafkaConsumer(
    'www_logs', bootstrap_servers="mslave2.admintome.lab:31000")

cluster = Cluster(['192.168.1.47'])
session = cluster.connect('admintome')

# start the loop
try:
    for message in consumer:
        entry = json.loads(json.loads(message.value))['log']
        print("Entry: {} Source: {} Type: {}".format(
            entry['datetime'],
            entry['source'],
            entry['type']))
        print("Log: {}".format(entry['log']))
        print("--------------------------------------------------")
        session.execute(
            """
INSERT INTO logs (log_source, log_type, log_datetime, log_id, log)
VALUES (%s, %s, %s, now(), %s)
""",
            (entry['source'],
             entry['type'],
             entry['datetime'],
             entry['log']))
except KeyboardInterrupt:
    sys.exit()
