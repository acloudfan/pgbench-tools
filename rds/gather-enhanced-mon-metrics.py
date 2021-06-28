#!/usr/bin/env python
# Pulls the Enhanced Monitoring metrics from CloudWatch
# https://docs.aws.amazon.com/cli/latest/reference/logs/get-log-events.html
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/logs.html#CloudWatchLogs.Client.get_log_events
# https://www.epochconverter.com/

import boto3
from datetime import datetime

client = boto3.client('logs',region_name='us-east-2')
# session = boto3.session.Session('logs',profile_name='blog')
# client=session.client('logs',region_name='us-east-2')

LOG_GROUP_NAME='RDSOSMetrics'
LOG_STREAM_DBCLUSTER='db-V2R2O3XWYK6N4CYIM2UC3L6IZE'

# Converts UTC timestamp to epoch time
def utc_to_epoch(utc_timestamp):
    # datetime.strptime("2009-03-08T00:27:31.807Z", "%Y-%m-%dT%H:%M:%S.%fZ")
    utc_time = datetime.strptime(utc_timestamp, "%Y-%m-%d %H:%M:%S.%f")
    epoch_time = ((utc_time - datetime(1970, 1, 1)).total_seconds())*1000
    return int(epoch_time)

utc_1="2021-06-27 14:23:32.22111"
startTime=utc_to_epoch(utc_1)
print(startTime)

utc_2="2021-06-27 14:24:33.105007"
endTime=utc_to_epoch(utc_2)
print(endTime)



logs = client.get_log_events(logGroupName=LOG_GROUP_NAME,
                      logStreamName=LOG_STREAM_DBCLUSTER,
                       startTime=startTime,
                       endTime=endTime )
print(logs)
