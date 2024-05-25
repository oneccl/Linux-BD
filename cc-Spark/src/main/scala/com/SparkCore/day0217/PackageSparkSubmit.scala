package com.SparkCore.day0217

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/17
 * Time: 19:15
 * Description:
 */
// Spark运行模式
/*
1、Local模式：本机提供资源，Spark提供计算
在IDEA中运行代码的环境，标明Master:local[*]，用于调试
2、Cluster(Standalone独立部署)模式：Spark提供资源，Spark提供计算
指定Master的地址和端口，连接到Spark集群
由Spark自身提供计算资源，无需其它框架，降低耦合度，独立性强，可靠性低
3、Yarn模式
Yarn中的NM没有Spark执行的相关类库，需要一台SparkNode向Yarn传递所需Spark环境
*/
object PackageSparkSubmit {

  // Spark任务提交（SparkSubmit）在集群中执行Spark任务
  // Scala项目打包：
  // 1）添加Scala项目打包依赖（不用设置Main-Class）
  /*
  SparkSubmit集群执行常用命令：
  spark-submit \
  --class Driver驱动程序全类名 \    应用程序入口
  --master spark://bd91:7077 \   指定集群Master的URL，还支持local、yarn集群
  --deploy-mode cluster \        client(客户端/本地)或cluster模式（默认client）
                                 集群模式下，不会输出日志和系统输出，只能在Hadoop集群上查看
  --executor-memory 1G \         指定每个Executor可用内存为1G（默认1G）
  --name WordCount               应用名称（可在程序设置）
  --total-executor-cores 2 \     指定每个Executor使用的CPU核数为2（可在程序设置）
  jar所在路径  参数1(args(0))  参数2(args(1))
  */
  // 2）启动HDFS集群，启动Spark集群，执行语句：
  /*
  spark-submit \
  --class com.SparkCore.day0217.PackageSparkSubmit \
  --master spark://bd91:7077 \
  --deploy-mode cluster \
  --executor-memory 1G \
  --total-executor-cores 2 \
  /opt/jars/cc-Spark-1.0-SNAPSHOT.jar \
  hdfs://bd91:8020/Harry.txt  hdfs://bd91:8020/sparkOut
  */
  // 3）在HDFS上查看结果

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)
    sc.textFile(args(0))
      .flatMap(_.toLowerCase.replaceAll("\\W+", " ").split("\\s+"))
      .filter(!_.trim.equals(""))
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false, 1)
      .saveAsTextFile(args(1))
  }

}
