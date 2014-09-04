# ~/.profile: executed by the command interpreter for login shells.
# This file is not read by bash(1), if ~/.bash_profile or ~/.bash_login
# exists.
# see /usr/share/doc/bash/examples/startup-files for examples.
# the files are located in the bash-doc package.

# the default umask is set in /etc/profile; for setting the umask
# for ssh logins, install and configure the libpam-umask package.
#umask 022

# if running bash
if [ -n "$BASH_VERSION" ]; then
    # include .bashrc if it exists
    if [ -f "$HOME/.bashrc" ]; then
	. "$HOME/.bashrc"
    fi
fi

# set PATH so it includes user's private bin if it exists
if [ -d "$HOME/bin" ] ; then
    PATH="$HOME/bin:$PATH"
fi

export JAVA_HOME=/usr/lib/jvm/default-java
export APACHE_HOME=/usr/local/apache2
export HBASE_HOME=/home/heavenize/bin/hbase-0.94.8
#export CATALINA_HOME=/usr/share/tomcat7
#export CATALINA_BASE=/usr/share/tomcat7

# hadoop applications environment variables
export HADOOP_PREFIX=/home/heavenize/Programs/hadoop-2.5.0
export HADOOP_HOME=$HADOOP_PREFIX
export HADOOP_COMMON_HOME=$HADOOP_PREFIX
export HADOOP_CONF_DIR=$HADOOP_PREFIX/etc/hadoop
export HADOOP_HDFS_HOME=$HADOOP_PREFIX
export HADOOP_MAPRED_HOME=$HADOOP_PREFIX
export HADOOP_YARN_HOME=$HADOOP_PREFIX

# TEZ
export TEZ_VERSION=0.4.1
export TEZ_HOME=/home/heavenize/bin/tez
export TEZ_CONF_DIR=${HADOOP_HOME}/etc/hadoop/
export TEZ_JARS=${TEZ_HOME}/tez-dist/target/tez-${TEZ_VERSION}-incubating/tez-${TEZ_VERSION}-incubating:${TEZ_HOME}/tez-dist/target/tez-${TEZ_VERSION}-incubating/tez-${TEZ_VERSION}-incubating/lib

export HIVE_HOME=/home/heavenize/Programs/apache-hive-0.13.1-bin

export PATH=$PATH:$HBASE_HOME/bin:$HADOOP_HOME/bin:$HIVE_HOME/bin
