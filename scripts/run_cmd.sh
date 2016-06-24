#!/bin/bash
source /etc/profile.d/freeheap_environment.sh

if [ -z "$1" ]
  then
    CASS_HOST=127.0.0.1
  else
    CASS_HOST=$1
fi


CASS_PORT=7104
CASS_KEYSPACE = 'keyspace_sample'


sudo /apps/spark/bin/spark-submit --driver-memory 10g --executor-memory 10g --class "com.freeheap.datastore.sparkjobs.CassandraSparkJob" $CASS_HOST $CASS_KEYSPACE $CASS_PORT
