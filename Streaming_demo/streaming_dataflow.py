# required libraries
import os
import csv
import time
from google.cloud import pubsub_v1
import json
from datetime import datetime


# Set authentication (Required if running locally)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/workspaces/de_zoomcamp/terraform_demo/Keys/tf_keys.json" 

## GCP project and topic
project_id = "de-project-449017"
topic_id = "emp-streaming-topic"

## pubsub publisher client
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id,topic_id)

## function to publish message to pubsub
def publish_message(empid,firstname,salary):
    timestamp = datetime.utcnow().isoformat() + "Z" # add current UTC timestamp
    message = {
        "empid":empid,
        "firstname":firstname,
        "salary":salary,
        "timestamp":timestamp,
    }

    # convert message to json and encode as bytes
    message_json = json.dumps(message).encode("utf-8")
    ## publish the message
    publish_status = publisher.publish(topic_path,message_json)
    print(f"Published message with ID: {publish_status.result()} - {message}")

## Path to CSV file
csv_file_path = "employee_data.csv"

# Publish record one by one
with open(csv_file_path, mode="r") as file:
    reader = csv.DictReader(file)
    for row in reader:
        empid = row["EmployeeID"]
        firstname = row["FirstName"]
        salary = row["Salary"]
        # Publish the record
        publish_message(empid,firstname,salary)
        # wait for 30 seconds before publishing the next round
        time.sleep(10) # adjusted time to 30 seconds