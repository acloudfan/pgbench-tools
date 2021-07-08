#!/bin/bash
#Generates the report from the pmon data
#https://www.xmodulo.com/plot-bar-graph-gnuplot.html
#https://realpython.com/python-formatted-output/
#http://www.bersch.net/gnuplot-doc/histograms.html
#https://docs.datadoghq.com/integrations/amazon_rds/?tab=standard

import psycopg2
import os
import json


def setup_config():
    print('setup')
    
def g