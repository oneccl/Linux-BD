
# HBase环境配置

# Java运行环境
export JAVA_HOME=/opt/module/jdk1.8


# jdk1.8+不需要配置HBASE_MASTER_OPTS、HBASE_REGIONSERVER_OPTS

# export HBASE_MASTER_OPTS="$HBASE_MASTER_OPTS -XX:PermSize=128m -XX:MaxPermSize=128m -XX:ReservedCodeCacheSize=256m"
# export HBASE_REGIONSERVER_OPTS="$HBASE_REGIONSERVER_OPTS -XX:PermSize=128m -XX:MaxPermSize=128m -XX:ReservedCodeCacheSize=256m"

# 暂时不配置HBASE_PID_DIR
# export HBASE_PID_DIR=/var/hadoop/pids

# 关闭HBase自带的zookeeper服务(默认true)，使用自己搭建的
export HBASE_MANAGES_ZK=false


