import time
from datetime import datetime
import subprocess


def get_hour():
    cur_time = datetime.now()
    return cur_time.hour


command = 'spark-submit --conf spark.jars.ivy=/opt/app --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,com.datastax.spark:spark-cassandra-connector_2.12:3.1.0" --master spark://spark-batch-master:7077 --deploy-mode client /opt/app/batch_processor.py'


if __name__ == "__main__":
    print("Script Started!")
    prev_hour = get_hour()
    while True:
        cur_hour = get_hour()
        if cur_hour != prev_hour:
            prev_hour = cur_hour
            print("Started Command Execution!")
            subprocess.run(command, shell=True, capture_output=True, text=True)
        time.sleep(60)
