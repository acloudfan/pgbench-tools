#https://docs.datadoghq.com/integrations/amazon_rds/?tab=standard
import psycopg2
import os
import json


exclude_category_metric={}

# Holds information on the reports to be generated
metric_reports={}

#Destination for all resports
SERVER_FOLDER='./results/'+os.getenv('SERVERNAME')
SERVER_FOLDER_DATA=SERVER_FOLDER+'/emon_data'
SERVER_FOLDER_SET_INFO='x'

# Read the config file and setup the environment vars
USER=os.environ['PGUSER']
PASSWORD=os.environ['PGPASSWORD']
HOST=os.environ['PGHOST']
PORT=os.environ['PGPORT']
RESULTDB='pgbenchtools'

conn = psycopg2.connect("dbname="+RESULTDB+" user="+USER+"  password="+PASSWORD)

# Generates the file for the metric for given scale
def gen_data_file_by_scale(set,set_info,category,metric, scale):
    sql='SELECT clients,value_min,value_avg,value_max from pmon_metric_processed where set=%i and category=\'%s\' and metric=\'%s\' and scale=%i order by clients'
    
    sql=sql % (set,category,metric,scale)
    print(sql)
    cur=conn.cursor()
    cur.execute(sql)
    rows=cur.fetchall()
    data = {
            "category": category,
            "metric": metric,
            "scale": scale,
            "values":{}
        }
    for row in rows:
        k={}
        data['values'][row[0]]=[row[1],row[2],row[3]]  #.append({ row[0] : [row[1],row[2],row[3]] })
    print(data)
    cur.close()
    write_data_file(set_info,'scale',data);
    
# Generates the file for the metric for given clients
def gen_data_file_by_clients(set,set_info,category,metric,clients):
    sql='SELECT scale,value_min,value_avg,value_max from pmon_metric_processed where set=%i and category=\'%s\' and metric=\'%s\' and clients=%i order by scale'
    
    sql=sql % (set,category,metric,clients)
    print(sql)
    cur=conn.cursor()
    cur.execute(sql)
    rows=cur.fetchall()
    data = {
            "category": category,
            "metric": metric,
            "clients": clients,
            "values":{}
        }
    for row in rows:
        k={}
        data['values'][row[0]]=[row[1],row[2],row[3]]  #.append({ row[0] : [row[1],row[2],row[3]] })
    print(data)
    cur.close()
    write_data_file(set_info,'clients',data);


# Create the data file name
def create_data_file_name(set_info, scale_or_clients,value,category,metric):
    return SERVER_FOLDER_DATA+"/"+set_info+"/"+scale_or_clients+'-'+str(value)+'-'+category+'-'+metric
    
#Writes the data file
#{'category': 'cpuUtilization', 'metric': 'total', 'scale': 1000, 'values': {10: [94.3, 95.0, 96.0], 100: [99.5, 100.0, 100.0]}
def write_data_file(set_info,scale_or_clients, data):
    
    delimiter=","
    
     # create folder if needed
    try:
        # os.makedirs(SERVER_FOLDER_DATA+"/"+SERVER_FOLDER_SET_INFO)
        os.makedirs(SERVER_FOLDER_DATA+"/"+set_info)
    except OSError as error:
        print(error)
        
    # data_file=scale_or_clients+'-'+str(data[scale_or_clients])+'-'+data['category']+'-'+data['metric']+'.csv'
    data_file=create_data_file_name(set_info,scale_or_clients,str(data[scale_or_clients]),data['category'],data['metric'] )+'.csv'
    # data_file=SERVER_FOLDER_DATA+"/"+SERVER_FOLDER_SET_INFO+"/"+data_file
    print("Creating Data File: "+data_file)
    f = open(data_file, 'w')

    xcol_name='scale'
    if scale_or_clients=='scale':
        xcol_name='clients'

    # Write out the header
    keys = data['values'].keys()
    f.write((xcol_name+'%s min %s avg %s max \n')  %  (delimiter,delimiter,delimiter))
    for key in keys:
        # writes x as "clients=#"
        # f.write(xcol_name+'='+str(key)+delimiter)
        # writes x as just #
        f.write(str(key)+delimiter)
        
        f.write(str(data['values'][key][0])+delimiter)
        f.write(str(data['values'][key][1])+delimiter)
        f.write(str(data['values'][key][2])+' \n')
        
    
   
        
    
################################################
# Generates the histo chart for the given file
def generate_metric_histo_png(category, title, data_file, image_file, metrics,y_max):

    # data_file=SERVER_FOLDER_DATA+"/"+data_file
    # image_file=SERVER_FOLDER+"/"+data_file+".png"
    print("Creating PNG: "+image_file)

    gp = os.popen('gnuplot', 'w')
    
    # print(metrics)
    
    # gp.write("set terminal pngcairo size 640,480 enhanced font 'sans,10';\n")
    gp.write("set output '" +image_file +"';\n")
    gp.write('set terminal png size 640,480 enhanced font "default,10";\n')
    
    gp.write(' green = "#00FF00"; blue = "#0000FF"; red = "#FF0000"; skyblue = "#87CEEB"; yellow="#F1C40F"; orange="#E67E22"; purple="#76448A"; silver="#CACFD2"; salmon="#FFA07A"; \n')
    colors = [ 'green', 'blue', 'red','skyblue', 'yellow', 'orange','purple', 'silver','salmon']

    yrange = 'set yrange [0:%i]\n' %  y_max
    gp.write(yrange) #'set yrange [0:100]\n')

    gp.write('set style data histogram\n')
    gp.write('set style histogram cluster gap 1\n')
    
    # gp.write('set style line cluster gap 1\n')
    
    gp.write("set datafile separator \',\'\n")
    
    gp.write('set style fill solid\n')
    gp.write('set boxwidth 0.9\n')
    gp.write('set xtics format ""\n')
    gp.write('set grid ytics\n')
    # gp.write('plot "data_file.txt"') # using 2:xtic(1) title "Dan" linecolor rgb red' )
    gp.write("set title '"+title+"\'\n")

    # gp.write('set lmargin 5 \n')
    gp.write('set key left \n')

    # plot =  'plot "bar.dat" using 2:xtic(1) title "Dan" linecolor rgb red,   '\
    #  '"bar.dat" using 3 title "Sophia" linecolor rgb blue,   '\
    #  '"bar.dat" using 4 title "Jody" linecolor rgb green,    '\
    #  '"bar.dat" using 5 title "Christina" linecolor rgb skyblue'

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

#Returns the test sets for the given server
def get_sets_for_server():
  server = os.getenv('SERVERNAME')
  sql='SELECT set, info FROM testset WHERE server=\'%s\''   %    server
  cur=conn.cursor()
  cur.execute(sql)
  rows=cur.fetchall()
  sets = []
  for row in rows:
    sets.append({"set": row[0], "info": row[1]})
  conn.commit()
  return sets;

# Returns distinct values as an array - print(get_distinct('category'))  print(get_distinct('metric'))
def get_distinct(field, set):
    sql='select distinct %s from pmon_metric_processed where set=%i order by %s '  % (field,set,field)
    cur=conn.cursor()
    cur.execute(sql)
    rows=cur.fetchall()
    distinct_list=[]
    for row in rows:
        distinct_list.append(row[0])
    conn.commit()
    
    return distinct_list
    
# Find the max in value_max
def get_y_max_value_for_scale(category, metric,clients):
    sql='SELECT MAX(value_max) FROM pmon_metric_processed  WHERE category=\'%s\' AND metric=\'%s\' AND clients=%i' 
    sql= sql % (category, metric, clients)
    # print(sql)
    cur=conn.cursor()
    cur.execute(sql)
    row, = cur.fetchone()
    cur.close()
    return row*1.1
    
# Find the max in value_max
def get_y_max_value_for_clients(category, metric,scale):
    sql='SELECT MAX(value_max) FROM pmon_metric_processed  WHERE category=\'%s\' AND metric=\'%s\' AND scale=%i' 
    sql= sql % (category, metric, scale)
    # print(sql)
    cur=conn.cursor()
    cur.execute(sql)
    row, = cur.fetchone()
    cur.close()
    return row*1.1

def generate_report(category, metric):
    

    # get all sets
    all_sets=get_sets_for_server()
    
    
    for set in all_sets:
        # get distinct scale
        distinct_scale=get_distinct('scale', set['set'])
        distinct_client=get_distinct('clients', set['set'])
        SERVER_FOLDER_SET_INFO=set['info']
        html_file_name='%s/%s/%s-%s.htm'  %  (SERVER_FOLDER_DATA, set['info'],category,metric)
        html=''
        
        print('Reporting on set# %i     %s   %s'   %     (set['set'], set['info'], html_file_name))
        
        # gen reports for fixed scale
        html +='<h1>By Scale</h1>'
        for scale in distinct_scale:
            gen_data_file_by_scale(set['set'],set['info'],category,metric,scale)
            fname=create_data_file_name(set['info'],'scale',scale,category,metric)
            title='By Scale : %s[%s] Scale=%i'   % (category, metric, scale)
            
            y_max=get_y_max_value_for_clients(category, metric, scale)
            
            generate_metric_histo_png(category,title, fname+'.csv',fname+'.png',['min','avg','max'],y_max)
            html += '<img src="%s"/>'  %  ('scale-'+str(scale)+'-'+category+'-'+metric+'.png')
            html += '<a href="%s"  type="text/csv">Data</a>'  %  ('scale-'+str(scale)+'-'+category+'-'+metric+'.csv')
            
        # gen reports for fixed clients
        html +='<h1>By Clients</h1>'
        for clients in distinct_client:
            gen_data_file_by_clients(set['set'],set['info'],category,metric,clients)
            fname=create_data_file_name(set['info'],'clients',clients,category,metric)
            title='By Client : %s[%s] Clients=%i'   % (category, metric, clients)
            
            y_max=get_y_max_value_for_scale(category, metric, clients)
            
            generate_metric_histo_png(category,title, fname+'.csv',fname+'.png',['min','avg','max'],y_max)
            html += '<img src="%s"/>'  %  ('clients-'+str(clients)+'-'+category+'-'+metric+'.png')
            html += '<a href="%s"  type="text/csv">Data</a>'  %  ('clients-'+str(clients)+'-'+category+'-'+metric+'.csv')
            
    
       
            
        f=open(html_file_name,'w')
        f.write(html)
        f.close()



##########
# set terminal pngcairo size 640,480 enhanced font 'Verdana,10'
# set output "clients-sets.png"
# set title "pgbench transactions/sec"
# set grid xtics ytics
# set key bottom right
# set xlabel "Clients"
# set ylabel "TPS"
# plot \
# 'clients-35.txt' using 1:2 axis x1y1 title 'dbr6g.large' with linespoints linewidth 2 pointtype 7 pointsize 1.5,\
# 'clients-67.txt' using 1:2 axis x1y1 title 'dbr6g.xlarge' with linespoints linewidth 2 pointtype 7 pointsize 1.5,\
# 'clients-133.txt' using 1:2 axis x1y1 title 'dbr5.large' with linespoints linewidth 2 pointtype 7 pointsize 1.5

#data={"file": "file", "title": "title", "index": 2}
def generate_metric_all_sets_png(xlabel, ylabel, title, data, image_file) :

    # data_file=SERVER_FOLDER_DATA+"/"+data_file
    # image_file=SERVER_FOLDER_DATA+"/"+img_file+".png"
    # print("Creating PNG: "+img_file)
    
    gp = os.popen('gnuplot', 'w')
    gp.write('set terminal pngcairo size 640,480 enhanced font \'Verdana,10\' ; \n')
    # gp.write('set terminal png size 640,480 enhanced font \'default,10\' ; \n')
    gp.write("set datafile separator \',\'\n")
    
    gp.write(('set output "%s"; \n') % image_file)
    # gp.write("set output '" +image_file +"';\n")
    
    gp.write(('set title "%s"\n')  %  title)
    gp.write('set grid xtics ytics\n')
    gp.write('set key bottom right\n')
    gp.write(('set xlabel "%s"\n') % xlabel)
    gp.write(('set ylabel "%s"\n') % ylabel)
    gp.write('plot ')
    line = "'%s' using 1:%i axis x1y1 title '%s' with linespoints linewidth 2 pointtype 7 pointsize 1.5,"
    # line = "'clients-133.txt' using 2:2 axis x1y1 title 'dbr5.large' with linespoints linewidth 2 pointtype 7pointsize 1.5"
    for dat in data:
        gp.write(line % (dat['file'], dat['index'], dat['title']))
        # gp.write(line)

# Get the scale value
# Reports across clients - contains the 
# index=1 for min, 2 for avg, 3 for max
# avg_min_max=avg,min,max
def png_across_sets_clients(scale, category, metric, sets_info, avg_min_max):
    
    if avg_min_max=='avg':
        index=3
    elif avg_min_max=='min':
        index=2
    else:
        index=4
        
    data_file='scale-%i-%s-%s.csv' % (scale,category,metric)
    data=[]
    for info in sets_info:
        dat={}
        dat['file']=SERVER_FOLDER_DATA+"/"+info+"/"+data_file
        dat['index']=index
        dat['title']=info
        data.append(dat)
    image_file='scale-%i-%s-%s-%s.png' % (scale,category,avg_min_max,metric)
    image_file=SERVER_FOLDER_DATA+"/"+image_file
    
    xlabel='Clients'
    ylabel='%s [%s]'  % (category,metric)
    title='[Scale=%i] Clients vs (%s)%s'   %  (scale, avg_min_max, ylabel)
    
    generate_metric_all_sets_png(xlabel,ylabel, title, data, image_file)
        
    
    

generate_report('cpuUtilization','total')
#generate_report('cpuUtilization','user')
#generate_report('cpuUtilization','wait')
#generate_report('cpuUtilization','idle')
# generate_report('memory','free')  
#generate_report('memory','buffers')  
#generate_report('diskIO.latency','writeLatency')  
#generate_report('diskIO.iops','writeIOsPS')  
#generate_report('diskIO.throughput','writeThroughput')
#generate_report('network','tx')

# data=[{"file": SERVER_FOLDER_DATA+"/dbr5.large/scale-100-cpuUtilization-total.csv", "index": 2, "title": "dbr5.large"}]
# generate_metric_all_sets_png("Clients","CPU total pct", "Clients vs CPU",data, SERVER_FOLDER_DATA+'/'+"test.png")

# png_across_sets_clients(100,'memory','free',['db.r5.large','db.r6g.large','db.r5.2xlarge','db.r6g.2xlarge'],'avg')
# png_across_sets_clients(100,'memory','buffers',['dbr5.large','dbr6g.large','dbr6g.xlarge'],'avg')
png_across_sets_clients(3000,'cpuUtilization','total',['db.r5.large','db.r6g.large','db.r5.2xlarge','db.r6g.2xlarge'],'avg')
# png_across_sets_clients(100,'cpuUtilization','idle',['dbr5.large','dbr6g.large','dbr6g.xlarge'],'avg')

# # png_across_sets_clients(100,'diskIO.iops','writeIOsPS',['dbr5.large','dbr6g.large','dbr6g.xlarge'],'avg')
# png_across_sets_clients(1000,'diskIO.latency','writeLatency',['dbr5.large','dbr6g.large','dbr6g.xlarge'],'avg')
# png_across_sets_clients(1000,'diskIO.throughput','writeThroughput',['dbr5.large','dbr6g.large','dbr6g.xlarge'],'avg')
# png_across_sets_clients(1000,'network','tx',['dbr5.large','dbr6g.large','dbr6g.xlarge'],'avg')

#     SERVER_FOLDER_SET_INFO=set['info']
    
#     title='CPU Utilization [Total] for Clients='
#     gen_data_file_by_clients(set['set'],'cpuUtilization','total',100)
#     gen_data_file_by_scale(set['set'],'cpuUtilization','total',100)
#     fname=create_data_file_name('clients',100,'cpuUtilization','total')
#     generate_metric_histo_png('cpuUtilization',title, fname+'.txt',fname+'.png',['min','avg','max'],100) #    category, title, data_file, image_file, metrics,y_max


# gen_data_file_by_clients(35,'cpuUtilization','total',100)
# gen_data_file_by_scale(35,'cpuUtilization','total',100)

# fname=create_data_file_name('clients',100,'cpuUtilization','total')
# generate_metric_histo_png('cpuUtilization','title', fname+'.txt',fname+'.png',['min','avg','max'],100) #    category, title, data_file, image_file, metrics,y_max