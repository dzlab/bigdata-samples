
## Start HDFS daemons
echo "Format the namenode direcotry (DO THIS ONLY ONCE, THE FIRST TIME)"
$HADOOP_PREFIX/bin/hdfs namenode -format
# Strat the namenode daemon
$HADOOP_PREFIX/sbin/hadoop-daemon.sh start namenode
# Start the datanode daemon
$HADOOP_PREFIX/sbin/hadoop-daemon.sh start datanode

## Start Yarn daemon
# Start the resource manager daemon
$HADOOP_PREFIX/sbin/yarn-daemon.sh start resourcemanager
# Start the node manager daemon
$HADOOP_PREFIX/sbin/yarn-daemon.sh start nodemanager

## Start other services
# Start the Timeline server daemon
echo "Waiting for name node to leave its safe mode"
sleep 60 # wait for the name node until it leaves the safe mode
mkdir -p /tmp/hadoop-heavenize/yarn/timeline/generic-history/ApplicationHistoryDataRoot
$HADOOP_PREFIX/sbin/yarn-daemon.sh start historyserver

# testing
#cd $HADOOP_PREFIX
#./bin/hadoop jar share/hadoop/yarn/hadoop-yarn-applications-distributedshell-2.5.0.jar org.apache.hadoop.yarn.applications.distributedshell.Client --jar share/hadoop/yarn/hadoop-yarn-applications-distributedshell-2.5.0.jar --shell_command date --num_containers 2 --master_memory 1024
# or
#$HADOOP_PREFIX/bin/hadoop jar $HADOOP_PREFIX/share/hadoop/yarn/hadoop-yarn-applications-distributedshell-2.5.0.jar org.apache.hadoop.yarn.applications.distributedshell.Client --jar $HADOOP_PREFIX/share/hadoop/yarn/hadoop-yarn-applications-distributedshell-2.5.0.jar --shell_command date --num_containers 2 --master_memory 1024

echo "starting thrift server to expose Hive to presto"
hive --service hiveserver &
echo "launching hive services (metastore, web interface) and command line"
hive --service metastore
hive --service hwi
hive

echo "launching prestodb"
$PRESTO_HOME/bin/launcher
