import os
import time
import threading
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError

from kafka import KafkaProducer
import logging
from json import dumps, loads
import csv
logging.basicConfig(level=logging.INFO)


producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092', value_serializer=lambda K:dumps(K).encode('utf-8'))

with open('monster_com-job_sample.csv', 'r') as file:
  reader = csv.reader(file)
  for row in reader:
  	message = " ".join(row)
  	producer.send('jobs', message)
  	producer.flush()
