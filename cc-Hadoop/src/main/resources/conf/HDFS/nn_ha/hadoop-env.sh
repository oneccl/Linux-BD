
### Hadoop HA高可用（QJM） ###
### 前提：配置Zookeeper ###
### NN HA下的Yarn HA高可用：见文件：Yarn/rm_ha/ ###

# HDFS分布式文件管理系统的配置

### 1、Hadoop环境配置 ###

# 文件位置：/opt/module/hadoop-2.7.7/etc/hadoop/hadoop-env.sh
# Hadoop官网：https://hadoop.apache.org

# Hadoop依赖JVM，需要配置JAVA_HOME

export JAVA_HOME=/opt/module/jdk1.8

export HDFS_NAMENODE_USER=root
export HDFS_DATANODE_USER=root
export HDFS_SECONDARYNAMENODE_USER=root
export YARN_RESOURCEMANAGER_USER=root
export YARN_NODEMANAGER_USER=root


