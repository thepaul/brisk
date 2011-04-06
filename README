DataStax Brisk
==============

This is a internal readme file for developers...

This package contains a HDFS compatable layer (CassandraFS) and a CassandraJobConf
which can be used to run MR jobs without HDFS or dedicated job/task trackers

Hive and Pig are setup to work with MR cluster.


Required Setup
==============

One thing to note on linux systems, you need to set as root

    echo 1 > /proc/sys/vm/overcommit_memory

This is to avoid OOM errors when tasks are spawned.

Getting Started
===============

To try it out run:

1. compile and download all dependencies
   
    ant

2. start cassandra with built in job/task trackers

    ./bin/brisk cassandra -t -f 

3. view jobtracker
   
    http://localhost:50030

4. examine CassandraFS

    ./bin/brisk hadoop fs -lsr cassandra:///

5. start hive shell or webUI
   
   ./bin/brisk hive

   or
 
   ./bin/brisk hive --service hwi
   open web browser to http://localhost:9999/hwi
