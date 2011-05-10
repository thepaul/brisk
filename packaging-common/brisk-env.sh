#!/bin/sh

if [ -z "$JAVA_HOME" ]; then
    export JAVA_HOME=$(readlink -f `which java` | sed "s:bin/java::")
fi

export CASSANDRA_HOME=/usr/share/brisk/cassandra
export CASSANDRA_BIN=/usr/sbin

#
# Add brisk jar
#
export CLASSPATH=$CLASSPATH:/usr/share/brisk/brisk.jar

#
# Source cassandra env
#
if [ "x$CASSANDRA_INCLUDE" = "x" ]; then
    for include in /usr/share/cassandra/cassandra.in.sh \
                   /usr/local/share/cassandra/cassandra.in.sh \
                   /opt/cassandra/cassandra.in.sh \
                   ~/.cassandra.in.sh \
                   `dirname $0`/cassandra.in.sh; do
        if [ -r $include ]; then
            . $include
            break
        fi
    done
elif [ -r $CASSANDRA_INCLUDE ]; then
    . $CASSANDRA_INCLUDE
fi

#
#Add hive cassandra driver
#
for jar in $BRISK_HOME/resources/hive/lib/hive-cassandra*.jar; do
    export CLASSPATH=$CLASSPATH:$jar
done

#hadoop requires absolute home
export HADOOP_HOME=/usr/share/brisk/hadoop
export HADOOP_CONF_DIR=/etc/brisk/hadoop
export HADOOP_BIN=$HADOOP_HOME/bin
export HADOOP_LOG_DIR=/var/log/hadoop

# needed for webapps
CLASSPATH=$CLASSPATH:$HADOOP_HOME:$HADOOP_HOME:/etc/brisk/hadoop

if [ -n "$HADOOP_NATIVE_ROOT" ]; then
    for jar in $HADOOP_NATIVE_ROOT/*.jar; do
    export CLASSPATH=$CLASSPATH:$jar
    done

    JAVA_PLATFORM=`$HADOOP_HOME/bin/hadoop org.apache.hadoop.util.PlatformName | sed -e "s/ /_/g"`

    export JAVA_LIBRARY_PATH=$HADOOP_NATIVE_ROOT/lib/native/${JAVA_PLATFORM}/
fi

#
# Add hadoop libs
#
for jar in $HADOOP_HOME/*.jar $HADOOP_HOME/lib/*.jar; do
    CLASSPATH=$CLASSPATH:$jar
done

export HADOOP_CLASSPATH=$CLASSPATH


#make the hadoop command accessible
export PATH=$HADOOP_BIN:$PATH

#
# Initialize Hive env
#
export HIVE_HOME=/usr/share/brisk/hive
export HIVE_CONF_DIR=/etc/brisk/hive
export HIVE_BIN=$HIVE_HOME/bin
export HIVE_LOG_ROOT=/var/log/hive
