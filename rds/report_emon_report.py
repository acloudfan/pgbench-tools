#!/bin/bash
#Generates the report from the pmon data
#https://www.xmodulo.com/plot-bar-graph-gnuplot.html
#https://realpython.com/python-formatted-output/
#http://www.bersch.net/gnuplot-doc/histograms.html

import psycopg2
import os
import json

exclude_category_metric={
    # "cpuUtilization": ["idle"],
    # "memory": ["total","hugePagesRsvd","hugePagesFree","hugePagesSurp", "hugePagesTotal","hugePagesSize","irq" ],
    # "loadAverageMinute": ["dummy"],
    # "tasks": ["dummy"],
    # "swap": ["swap"],
    # "network": ["interface", "dummy"],
    # "diskIO": ["dummy"],
    # "diskIO.latency": ["dummy"],
    # "diskIO.throughput": ["dummy"],
    # "diskIO.iops": ["dummy"],
    # "diskIO.diskQueueDepth": ["dummy"]
}

# Holds information on the reports to be generated
metric_reports={}

#Destination for all resports
SERVER_FOLDER='./results/'+os.getenv('SERVERNAME')
SERVER_FOLDER_DATA=SERVER_FOLDER+'/emon_data'

# Read the config file and setup the environment vars
USER=os.environ['PGUSER']
PASSWORD=os.environ['PGPASSWORD']
HOST=os.environ['PGHOST']
PORT=os.environ['PGPORT']
RESULTDB='pgbenchtools'

conn = psycopg2.connect("dbname="+RESULTDB+" user="+USER+"  password="+PASSWORD)

# Reads the environment variables to setup configuration
def setup_config():


    # Setup metric exclusion dictionary
    metrics=["cpuUtilization","memory","loadAverageMinute","tasks","swap","network"]
    for metric in metrics:
        env_var='METRIC_'+metric+'_exclude'
        # print(env_var, os.getenv(env_var))
        exclude_category_metric[metric] =os.getenv(env_var,"dummy").split(",")
    # take care of the distIO split
    metrics=["diskIO","diskIO.latency","diskIO.throughput","diskIO.iops","diskIO.diskQueueDepth"]
    env_var='METRIC_diskIO_exclude'
    for metric in metrics:
        exclude_category_metric[metric] =os.getenv(env_var,"dummy").split(",")

    # Set up report dictionary
    PMON_REPORTS_HISTO_CLIENTS=os.getenv("PMON_REPORTS_HISTO_CLIENTS")
    for report_metric in PMON_REPORTS_HISTO_CLIENTS.split(" "):
        # print(PMON_REPORTS_HISTO_CLIENTS, report_metric)
        report_metric=report_metric.strip()
        if report_metric=='':
            continue
        if report_metric not in metric_reports.keys():
            metric_reports[report_metric]=[]
        metric_reports[report_metric].append('clients')

    PMON_REPORTS_HISTO_SCALE=os.getenv("PMON_REPORTS_HISTO_SCALE")
    for report_metric in PMON_REPORTS_HISTO_SCALE.split(" "):
        report_metric=report_metric.strip()
        if report_metric=='':
            continue
        if report_metric not in metric_reports.keys():
            metric_reports[report_metric]=[]
        metric_reports[report_metric].append('scale')
    print(metric_reports)

# Gets the average for the desired catrgory & metric combination orders by scale | clients
# Writes to the passed dictionary
def get_average_metric(scale_or_clients, category, metric, dict):

    if category == 'loadAverageMinute':
        sql = 'SELECT '+scale_or_clients+',10*(AVG(value)) FROM pmon_metric_stripped WHERE category=\''+category+'\' AND metric=\''+metric+'\' GROUP BY '+ scale_or_clients +' ORDER BY '+ scale_or_clients
    else:
        sql = 'SELECT '+scale_or_clients+',ROUND(AVG(value)) FROM pmon_metric_stripped WHERE category=\''+category+'\' AND metric=\''+metric+'\' GROUP BY '+ scale_or_clients +' ORDER BY '+ scale_or_clients
    # print(sql)
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    for row in rows:
        # Add the scale/clients if its not there
        if str(row[0]) not in dict.keys():
            dict[str(row[0])]=[]
        dict[str(row[0])].append(row[1])

# Gets the average for the desired test
def get_average_metric_write(scale_or_clients, category, metric):

    sql = 'SELECT '+scale_or_clients+',ROUND(AVG(value)) FROM pmon_metric_stripped WHERE category=\''+category+'\' AND metric=\''+metric+'\' GROUP BY '+ scale_or_clients +' ORDER BY '+ scale_or_clients

    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()

    # Open a file for write
    fname='avg_'+category+'_'+metric+'.txt'
    f=open(fname,'w')
    # f.write('# Category='+category+' Metric='+metric+'\n')
    f.write(scale_or_clients+' avg \n')
    
    for row in rows:
        # print(row[0], row[1])
        f.write(str(row[0])+' '+str(row[1])+'\n')
    f.close()

# Gets the distinct metric in a category
def get_distinct_metric_in_category(category):
    sql="SELECT distinct metric from pmon_metric_stripped where category='%s' " % (category)
    exclude_metric_list = exclude_category_metric[category]
    if exclude_metric_list is not None:
        sql += 'AND metric not in ' + str(exclude_metric_list)
        sql=sql.replace("[","(")
        sql=sql.replace("]",")")
        
    # print(sql)
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    metrics=[]
    for row in rows:
        metrics.append(row[0])
    return metrics

#Used for setting the max y
def find_max_metric_value(category):
    sql="SELECT max(value) from pmon_metric_stripped WHERE category='%s'"     %   (category)
    exclude_metric_list = exclude_category_metric[category]
    if exclude_metric_list is not None:
        sql += 'AND metric not in ' + str(exclude_metric_list)
        sql=sql.replace("[","(")
        sql=sql.replace("]",")")
    cur = conn.cursor()
    cur.execute(sql)
    row, = cur.fetchone()

    return row;
    

#Setup the histo data based on category
#Write the data file
def generate_average_metric_in_category(scale_or_clients, category):

    metrics=get_distinct_metric_in_category(category)
    
    # print(metrics)
    dict={
        "category": category,
        "metrics": metrics
    }
    # Read the metrics data
    for metric in metrics:
        get_average_metric(scale_or_clients, category, metric,dict)
        # scale_client_values=[]
    # print(dict)
    # Create the data file
    data_file=scale_or_clients+"-"+category+"-avg.txt"
    data_file=SERVER_FOLDER_DATA+"/"+data_file
    print("Creating Data File: "+data_file)
    f = open(data_file, 'w')

    # Write out the header
    f.write(scale_or_clients+ ' ')
    for metric in metrics:
        f.write(metric+" ")
    f.write('\n')
    
    #
    for met in dict.keys():
        if met not in ["category", "metrics"]:
            
            f.write(scale_or_clients+"="+met+" ")
            # read the metrics data now
            for data in dict[met]:
                if category=="memory":
                    data = data/1024
                f.write(str(data)+' ')
            f.write("\n")
    image_file=SERVER_FOLDER+"/"+scale_or_clients+"-"+category+"-avg.png"

    title="Average "+category+" vs. "+scale_or_clients

    # Category specific stuff
    y_max=find_max_metric_value(category)
    if category == "cpuUtilization":
         y_max=((y_max)*1.2)
         if y_max > 100:
             y_max=100
    elif category=='tasks':
        y_max=y_max*1.2
    elif category=='network':
        y_max=y_max*1.2
        title += ' (MBPS)'
    elif category=='swap':
        sql = 'SELECT value FROM pmon_metric_stripped WHERE metric=\'total\' AND category=\'swap\''
        cur=conn.cursor()
        cur.execute(sql)
        row, = cur.fetchone()
        total_swap=(row/1024)
        title += " (Swap Memory = %i MB)"   %   total_swap
        y_max=y_max/1024
    elif category=='loadAverageMinute':
        # For actual divide by 10
        y_max=y_max*10
        title += '  (divide by 10 for actual load)'
    elif category == "memory":
        # Select max(total) for metric=total
        sql = 'SELECT value FROM pmon_metric_stripped WHERE metric=\'total\' AND category=\'memory\''
        cur=conn.cursor()
        cur.execute(sql)
        row, = cur.fetchone()
        total_mem=(row/1024)
        title += " (Total Memory = %i MB)"   %   total_mem
        y_max=find_max_metric_value(category)
        y_max=y_max/1024
    generate_metric_avg_histo_png(category, title, data_file, image_file, metrics,y_max)

    return dict

###############
# Generates the histo chart for the given file
def generate_metric_avg_histo_png(category, title, data_file, image_file, metrics,y_max):

    # data_file=SERVER_FOLDER_DATA+"/"+data_file
    # image_file=SERVER_FOLDER+"/"+data_file+".png"
    print("Creating PNG: "+image_file)

    gp = os.popen('gnuplot', 'w')
    
    # print(metrics)
    
    # gp.write("set terminal pngcairo size 640,480 enhanced font 'sans,10';\n")
    gp.write("set output '" +image_file +"';\n")
    gp.write('set terminal png size 800,500 enhanced font "default,15";\n')
    gp.write('red = "#FF0000"; green = "#00FF00"; blue = "#0000FF"; skyblue = "#87CEEB"; yellow="#F1C40F"; orange="#E67E22"; purple="#76448A"; silver="#CACFD2"; salmon="#FFA07A"; \n')
    colors = ['red', 'green', 'blue', 'skyblue', 'yellow', 'orange','purple', 'silver','salmon']

    yrange = 'set yrange [0:%i]\n' %  y_max
    gp.write(yrange) #'set yrange [0:100]\n')

    gp.write('set style data histogram\n')
    gp.write('set style histogram cluster gap 1\n')
    gp.write('set style fill solid\n')
    gp.write('set boxwidth 0.9\n')
    gp.write('set xtics format ""\n')
    gp.write('set grid ytics\n')
    # gp.write('plot "data_file.txt"') # using 2:xtic(1) title "Dan" linecolor rgb red' )
    gp.write("set title '"+title+"\'\n")

    gp.write('set lmargin 5 \n')
    gp.write('set key left \n')

    plot =  'plot "bar.dat" using 2:xtic(1) title "Dan" linecolor rgb red,   '\
     '"bar.dat" using 3 title "Sophia" linecolor rgb blue,   '\
     '"bar.dat" using 4 title "Jody" linecolor rgb green,    '\
     '"bar.dat" using 5 title "Christina" linecolor rgb skyblue'

    # print(plot)
    # print("-------")

    # plot =  'plot "'+data_file+'" using 2:xtic(1) title "Dan" linecolor rgb red'

    i = 0
    plot='plot "%s" using 2:xtic(1) title "%s" linecolor rgb %s'  %  (data_file, metrics[0], colors[i])
    i=1
    # ctr=1
    while i < len(metrics):
        # if metrics[i] in exclude_category_metric[category]:
        #     ctr += 1
        #     continue
        plot += ', "%s" using %i title "%s" linecolor rgb %s'   %   (data_file,(i+2), metrics[i], colors[i % len(colors)])
        i += 1
    # print(plot)
        


    gp.write(plot)

###############

# Generates the bar chart for the given file
def generate_png_histo(title, data_file, image_file):
    gp = os.popen('gnuplot', 'w')
    
    
    # gp.write("set terminal pngcairo size 640,480 enhanced font 'sans,10';\n")
    gp.write("set output '" +image_file + "';\n")
    gp.write('set terminal png size 800,500 enhanced font "default,20";\n')
    gp.write('red = "#FF0000"; green = "#00FF00"; blue = "#0000FF"; skyblue = "#87CEEB"; yellow="#F1C40F"; orange="#E67E22"; purple="#76448A"; silver="#CACFD2"; salmon="#FFA07A"; \n')
    gp.write('set yrange [0:100]\n')
    gp.write('set style data histogram\n')
    gp.write('set style histogram cluster gap 1\n')
    gp.write('set style fill solid\n')
    gp.write('set boxwidth 0.9\n')
    gp.write('set xtics format ""\n')
    gp.write('set grid ytics\n')
    # gp.write('plot "data_file.txt"') # using 2:xtic(1) title "Dan" linecolor rgb red' )
    gp.write("set title '"+title+"\'\n")

    plot =  'plot "bar.dat" using 2:xtic(1) title "Dan" linecolor rgb red,   '\
     '"bar.dat" using 3 title "Sophia" linecolor rgb blue,   '\
     '"bar.dat" using 4 title "Jody" linecolor rgb green,    '\
     '"bar.dat" using 5 title "Christina" linecolor rgb skyblue'

    plot =  'plot "'+data_file+'" using 2:xtic(1) title "Dan" linecolor rgb red'

    gp.write(plot)


# Setup config using env vars defined in config
setup_config()

# generate the requested reports
for category in metric_reports:
    for scale_or_clients in metric_reports[category]:
        generate_average_metric_in_category(scale_or_clients, category)


# print(SERVER_FOLDER_DATA)
# metrics=get_distinct_metric_in_category('cpuUtilization')
# print(metrics)


# dict=get_average_metric_in_category('clients','cpuUtilization')
# dict=get_average_metric_in_category('scale','cpuUtilization')

# dict=get_average_metric_in_category('clients','memory')
# dict=get_average_metric_in_category('scale','memory')

# dict=get_average_metric_in_category('clients','loadAverageMinute')
# dict=get_average_metric_in_category('scale','loadAverageMinute')

# dict=get_average_metric_in_category('clients','tasks')

# dict=get_average_metric_in_category('clients','swap')
# dict=get_average_metric_in_category('clients','network')

# # dict=get_average_metric_in_category('clients','diskIO')
# # dict=get_average_metric_in_category('scale','diskIO')

# dict=get_average_metric_in_category('clients','diskIO.latency')
# dict=get_average_metric_in_category('clients','diskIO.throughput')
# dict=get_average_metric_in_category('clients','diskIO.iops')