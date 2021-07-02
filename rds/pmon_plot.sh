#!/bin/bash
#Plots the desired metrics for the specified category & metric


select pmon_metric_stripped.test, scale,clients, category, metric, avg(value) FROM pmon_metric_stripped, tests where category='cpuUtilization' AND metric='total' GROUP BY pmon_metric_stripped.test, category, metric, tests.scale, tests.clients;

SELECT clients,AVG(value)
FROM pmon_metric_stripped
WHERE category='cpuUtilization' AND metric='total'
GROUP BY clients;

SELECT scale,ROUND(AVG(value))
FROM pmon_metric_stripped
WHERE category='memory' AND metric='dirty'
GROUP BY scale;