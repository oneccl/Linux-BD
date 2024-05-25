package com.shell;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/29
 * Time: 20:02
 * Description:
 */
public class DemoShellScript {

    // 常用shell脚本

    // 1、scp分发脚本
    /*
    #!/bin/bash

    # 获取当前主机
    HOSTNAME=`hostname`

    # ``表示引用结果
    # awk '{print $n}': 获取第n列的值
    # grep -v n : 取反，获取除了n的其它值
    hosts=`cat /etc/hosts | grep bd | awk '{print $2}' | grep -v $HOSTNAME`

    # for循环
    for host in $hosts
    # do : 开始
    do
    # echo :
    echo "正在将${1}从${HOSTNAME}发送到${host}:${2}..."
    # 语句中{}可以省略，$1表示调用脚本执行的第1个参数，$2表示调用脚本执行的第2个参数
    scp -r $1 $host:$2
    echo "发送完成!"

    # 结束
    done
    */

    // 2、sqoop导入导出(MySQL2Hive)
    /*
    #!/bin/bash
    source /etc/profile;
    sqoop import \
    --connect "jdbc:mysql://bd91:3306/$1" \
    --username root --password 123456 \
    --table "$2" \
    --num-mappers 1 \
    --hive-import \
    --delete-target-dir \
    --fields-terminated-by '\t' \
    --hive-overwrite \
    --hive-table "exercises.$3"
    */
    // ODS层加载数据脚本
    /*
    #!/bin/bash
    # 刷新环境变量
    source /opt/client/bigdata_env
    # Hive数据库名
    APP=batch
    # Hive beeline命令
    beeline=/opt/client/Hive/Beeline/bin/beeline

    # 如果是有输入的日期则按照输入日期；如果没有输入日期则按当天时间的前一天
    if [ -n "$1" ] ;then
       do_date=$1
    else
       do_date=`date -d "-1 day" +%F`
    fi

    echo "-----+----日志日期为 $do_date----+-----"
    # 导入启动数据和事件数据
    sql="
    load data inpath '/batch/logs/topic_start/$do_date' into table "$APP".ods_start_log partition(dt='$do_date');
    load data inpath '/batch/logs/topic_event/$do_date' into table "$APP".ods_event_log partition(dt='$do_date');
    "
    $beeline -e "$sql"
    */

    // 3、Hadoop集群启动、停止
    /*
    #!/bin/bash
    host=`hostname`
    # 未输入参数时执行
    if [ $# -eq 0 ]
    then
        echo "Please Input Args [start] OR [stop]"
        exit;
    fi
    case $1 in
    "start")
        echo "+-----+-----正在启动Hadoop集群...------+-----+"
        echo "+-----+-----+---正在启动hdfs...--+-----+-----+"
        ssh $host "source /etc/profile;start-dfs.sh"
        echo "+-----+-----+---正在启动yarn...--+-----+-----+"
        ssh $host "source /etc/profile;start-yarn.sh;yarn-daemon.sh start resourcemanager"
        ssh bd92 "source /etc/profile;yarn-daemon.sh start resourcemanager"
        echo "+-----+----正在启动historyserver...----+-----+"
        ssh $host "source /etc/profile;mr-jobhistory-daemon.sh start historyserver"
    ;;
    "stop")
        echo "+-----+-----正在停止Hadoop集群...------+-----+"
        echo "+-----+----正在停止historyserver...----+-----+"
        ssh $host "source /etc/profile;mr-jobhistory-daemon.sh stop historyserver"
        echo "+-----+-----+---正在停止yarn...--+-----+-----+"
        ssh $host "source /etc/profile;stop-yarn.sh;yarn-daemon.sh stop resourcemanager"
        ssh bd92 "source /etc/profile;yarn-daemon.sh stop resourcemanager"
        echo "+-----+-----+---正在停止hdfs...--+-----+-----+"
        ssh $host "source /etc/profile;stop-dfs.sh"
    ;;
    # 当输入的参数不为start或stop时执行
    *)
        echo "Input Args Error..."
    ;;
    esac
    */

    // 4、Zookeeper集群启动、停止
    /*
    #!/bin/bash
    # 未输入参数时执行
    if [ $# -eq 0 ]
    then
        echo "Please Input Args [start] OR [stop] OR [status]"
        exit;
    fi
    case $1 in
    "start")
        hosts=`cat /etc/hosts | grep bd | awk '{print $2}'`
        for host in $hosts
        do
            echo "+----+----$host ZK服务正在启动...----+----+"
            # 需要先刷新环境变量，否则找不到命令
            ssh $host "source /etc/profile;zkServer.sh start"
        done
    ;;
    "stop")
        hosts=`cat /etc/hosts | grep bd | awk '{print $2}'`
        for host in $hosts
        do
            echo "+----+----$host ZK服务正在停止...----+----+"
            ssh $host "source /etc/profile;zkServer.sh stop"
        done
    ;;
    "status")
        hosts=`cat /etc/hosts | grep bd | awk '{print $2}'`
        for host in $hosts
        do
            echo "+----+----$host 正在查询ZK状态...----+----+"
            ssh $host "source /etc/profile;zkServer.sh status"
        done
    ;;
    # 当输入的参数不为start或stop或status时执行
    *)
        echo "Input Args Error..."
    ;;
    esac
    */

    // 5、Kafka集群启动、停止
    /*
    #!/bin/bash
    # 未输入参数时执行
    if [ $# -eq 0 ]
    then
        echo "Please Input Args [start] OR [stop]"
        exit;
    fi
    case $1 in
    "start")
        hosts=`cat /etc/hosts | grep bd | awk '{print $2}'`
        for host in $hosts
        do
            echo "+----+----$host Kafka服务正在启动...----+----+"
            ssh $host "source /etc/profile;kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties"
        done
    ;;
    "stop")
        hosts=`cat /etc/hosts | grep bd | awk '{print $2}'`
        for host in $hosts
        do
            echo "+----+----$host Kafka服务正在停止...----+----+"
            ssh $host "source /etc/profile;kafka-server-stop.sh -daemon $KAFKA_HOME/config/server.properties"
            ssh $host "$JAVA_HOME/bin/jps | grep Kafka | awk '{print$1}' | xargs kill -l 9"
        done
    ;;
    # 当输入的参数不为start或stop时执行
    *)
        echo "Input Args Error..."
    ;;
    esac
    */

    // 6、显示当前所有机器的所有Java进程及其pid
    /*
    #!/bin/bash
    hosts=`cat /etc/hosts | grep bd | awk '{print $2}'`
    for host in $hosts
    do
        echo "+-------+-------$host-------+-------+"
        ssh $host "$JAVA_HOME/bin/jps | grep -v Jps"
    done
    */

}
