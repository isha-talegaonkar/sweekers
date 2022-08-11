from kafka import KafkaConsumer
import json 
import time
import sys
import random
import requests

def slackbotNotification(message):
    print(message)
    url = "https://hooks.slack.com/services/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"
    title = (f"New job posting")
    slack_data = {
        "username": "NotificationBot",
        "icon_emoji": ":satellite:",
        "channel" : "#job-postings",
        "attachments": [
            {
                "color": "#9733EE",
                "fields": [
                    {
                        "title": title,
                        "value": message[0],
                        "short": "false",
                    }
                ]
            }
        ]
    }
    byte_length = str(sys.getsizeof(slack_data))
    headers = {'Content-Type': "application/json", 'Content-Length': byte_length}
    response = requests.post(url, data=json.dumps(slack_data), headers=headers)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)


consumer = KafkaConsumer(
    'jobs',
     bootstrap_servers=['127.0.0.1:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    jobs = message.value
    time.sleep(5)
    slackbotNotification(jobs)
    #print('{} added'.format(jobs))


