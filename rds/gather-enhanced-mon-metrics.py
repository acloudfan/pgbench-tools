#
# Good link for getting idea on the enhanced mon metric
# https://docs.datadoghq.com/integrations/amazon_rds/?tab=enhanced

import psycopg2
import os
import boto3
from datetime import datetime
import json



LOG_GROUP_NAME='RDSOSMetrics'
# LOG_STREAM_DB_INSTANCE='db-V2R2O3XWYK6N4CYIM2UC3L6IZE'
LOG_STREAM_DB_INSTANCE=os.environ['LOG_STREAM_DB_INSTANCE']


client = boto3.client('logs')

USER=os.environ['PGUSER']
PASSWORD=os.environ['PGPASSWORD']
HOST=os.environ['PGHOST']
PORT=os.environ['PGPORT']
RESULTDB='pgbenchtools'


conn = psycopg2.connect("dbname="+RESULTDB+" user="+USER+"  password="+PASSWORD)

# Class that holds the test data including the metrics JSON
class  test:
        def __init__(self, test, start_time, end_time,clients,scale):
                self.test = test
                self.start_time=start_time
                self.end_time=end_time
                self.clients=clients
                self.scale=scale
                self.events='{}'

# Converts UTC timestamp to epoch time
def utc_to_epoch(utc_timestamp):
    # datetime.strptime("2009-03-08T00:27:31.807Z", "%Y-%m-%dT%H:%M:%S.%fZ")
    utc_time = datetime.strptime(utc_timestamp, "%Y-%m-%d %H:%M:%S.%f")
    epoch_time = ((utc_time - datetime(1970, 1, 1)).total_seconds())*1000
    return int(epoch_time)

# Get the set number for the latest set
def get_latest_set(server):
        sql='SELECT max(set) FROM testset WHERE server=\''+server+'\''
        cur = conn.cursor()
        cur.execute(sql)
        set_max=cur.fetchone()[0]
        return set_max

# Get the test numbers for the latest set
def get_test_numbers(set):
        sql='SELECT test, start_time, end_time, clients, scale FROM tests WHERE set='+str(set)
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        tests_data=[]
        for row in rows:
                tests_data.append(test(row[0], row[1], row[2], row[3], row[4]))

        return tests_data

# Insert data in the pmon table
def insert_into_pmon_table(server, set, testdata):
  sql_delete="DELETE FROM test_pmon_metrics_data WHERE set="+str(set)+" AND test="+str(testdata.test)
  # sql_insert="INSERT INTO test_pmon_metrics_data(server, set, test, events) VALUES('"+server+"', "+str(set)+", "+str(testdata.test)+",'"+ json.dumps(testdata.events)+"')"
  
  sql_insert='''
    INSERT INTO test_pmon_metrics_data(server, set, test, events)
    VALUES('%s', %i, %i, '%s')
  ''' % (server, set, testdata.test, json.dumps(testdata.events))

  # print(sql_insert)
  for event in testdata.events['events']:
    insert_stripped_event_pmon_metric(server, set, testdata.test,testdata.clients, testdata.scale, event)
  cur = conn.cursor()
  cur.execute(sql_delete)
  cur.execute(sql_insert)
  conn.commit()

# Seggregate the diskIO metric into Latency, Throughput, QueueDepth
def seggregate_diskIO_metric(eventdata_category):
  metric_dict_latency = {"category": "diskIO.latency", "metric_data" : {} }
  metric_dict_throughput = {"category": "diskIO.throughput", "metric_data" : {}}
  metric_dict_iops = {"category": "diskIO.iops", "metric_data" : {}}
  metric_dict_queue_depth={"category": "diskIO.diskQueueDepth", "metric_data" : {}}

  # picke the data from non rdstemp
  for metric in eventdata_category[0]:
    if metric == 'readLatency' or metric == 'writeLatency':
      metric_dict_latency['metric_data'][metric]=eventdata_category[0][metric]
    elif metric == 'readThroughput' or metric == 'writeThroughput':
      metric_dict_throughput['metric_data'][metric]=eventdata_category[0][metric]
    elif metric == 'readIOsPS' or metric == 'writeIOsPS':
      metric_dict_iops['metric_data'][metric]=eventdata_category[0][metric]
    elif metric == 'diskQueueDepth' :
      # print(eventdata_category[0][metric])
      metric_dict_queue_depth['metric_data'][metric]=round(eventdata_category[0][metric])

  return [metric_dict_latency,metric_dict_throughput,metric_dict_iops,metric_dict_queue_depth]
  
# Strip the pmon data and add to the pmon_metric_stripped table
def insert_stripped_event_pmon_metric(server, set, test, clients, scale, event):
  sql_delete="DELETE FROM pmon_metric_stripped WHERE set="+str(set)+" AND test="+str(test)

  cur = conn.cursor()
  cur.execute(sql_delete)

  # print(event['message'])
  eventdata=json.loads(event['message'])
  collected=eventdata['timestamp']
  timestamp=event['timestamp']

  # network is an array so we need to flatten it out or just use eth0
  categories=['tasks','cpuUtilization', 'loadAverageMinute','memory','swap','network','diskIO']
  for category in categories:
    # Network category returns an array of n/w interfaces - just spit data for eth0
    if category=='network' :
      eventdata_category=eventdata[category][0]
      # print(category+" "+str(eventdata_category))
      insert_metric_data(eventdata_category,category,collected,timestamp, server, set, test,clients,scale)
    elif  category=='diskIO':
      seggregated_metric_sets=seggregate_diskIO_metric(eventdata[category])
      for disk_metrics  in seggregated_metric_sets:
        category=disk_metrics['category']
        eventdata_category=disk_metrics['metric_data']
        insert_metric_data(eventdata_category,category,collected,timestamp, server, set, test,clients,scale)
    else:
      eventdata_category= eventdata[category]
      print(eventdata_category)
      # Insert the metric data
      insert_metric_data(eventdata_category,category,collected,timestamp, server, set, test,clients,scale)


    # for metric in eventdata_category:
    #   print(category+" "+metric+" "+str(eventdata_category[metric]))
    #   insert_metric_data(eventdata_category,category,collected,timestamp, server, set, test,clients,scale)
      # data = eventdata[category][metric]
      # data=eventdata_category[metric]
      # Check type of data
  #     if (isinstance(data, str)):
  #       values=data
  #       value=0
  #     else:
  #       values=''
  #       value=data
  #     # Insert the metric
  #     sql_insert='''
  #       INSERT INTO pmon_metric_stripped(collected,timestamp, server, set, test,clients,scale, category, metric, value, values)
  #       VALUES('%s',%i,'%s', %i, %i,%i,%i, '%s','%s',%f,'%s')
  #     ''' % (collected, timestamp, server, set, test,clients,scale, category,metric, value,values )
  #     cur.execute(sql_insert)
  # conn.commit()

#Insert the metric data in the table
def insert_metric_data(eventdata_category,category,collected,timestamp, server, set, test,clients,scale):
  cur = conn.cursor()
  for metric in eventdata_category:
      # print(category+" "+metric+" "+str(eventdata_category[metric]))
      # data = eventdata[category][metric]
      data=eventdata_category[metric]
      # Check type of data
      if (isinstance(data, str)):
        values=data
        value=0
      else:
        values=''
        value=data
      # Insert the metric
      sql_insert='''
        INSERT INTO pmon_metric_stripped(collected,timestamp, server, set, test,clients,scale, category, metric, value, values)
        VALUES('%s',%i,'%s', %i, %i,%i,%i, '%s','%s',%f,'%s')
      ''' % (collected, timestamp, server, set, test,clients,scale, category,metric, value,values )

      # print(sql_insert)
      cur.execute(sql_insert)
  conn.commit()

#For each test get the  performance data
def get_emon_data(server, set, tests_data):
        for testdata in tests_data:
                # testdata.events= test()
                test_num=testdata.test
                start_time=testdata.start_time
                end_time=testdata.end_time
                print(str(test_num)+ ' '+str(start_time))
                response = client.get_log_events(logGroupName=LOG_GROUP_NAME,
                      logStreamName=LOG_STREAM_DB_INSTANCE,
                       startTime=utc_to_epoch(str(start_time)),
                       endTime=utc_to_epoch(str(end_time)))
                # print(response)
                testdata.events=response
                # Insert the response in the pmon table
                insert_into_pmon_table(server, set, testdata)

server = 'rdsa'

set_max=get_latest_set('rdsa')
print(set_max)

tests=get_test_numbers(set_max)
# print(tests)

get_emon_data(server, set_max, tests)

# print()

# print(tests[0].events)