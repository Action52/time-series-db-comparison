#!/bin/bash

echo "log4j.rootCategory=INFO, console" >> /spark/conf/log4j.properties

if [ 'master' == $1 ]; then :
  echo "Building Master Node." 
  /spark/bin/spark-class org.apache.spark.deploy.master.Master --ip `hostname` --port 7077 --webui-port 8080; else :
  echo "Building worker Node."
  /spark/bin/spark-class org.apache.spark.deploy.worker.Worker --webui-port 8080 spark://spark-master:7077; fi