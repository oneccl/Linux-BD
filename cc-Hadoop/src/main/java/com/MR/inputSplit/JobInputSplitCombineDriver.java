package com.MR.inputSplit;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/1/18
 * Time: 12:03
 * Description:
 */

// MapReduce的运行原理
/*
1、客户端submit()前，获取数据，根据配置，形成一个任务规划
2、对任务(数据)按照长度(文件大小)进行切片(任务分割/分片)，计算所需MapTask线程数
   默认按BlockSize(128M)分片，剩余的数据单独成片
   例如：file：300M，切片后为
        fileSplit1  128M  MapTask1
        fileSplit2  128M  MapTask2
        fileSplit1  44M   MapTask3
   分片的个数与MR任务启动的MapTask的线程数(并发度)对应
***** 源码(FileInputFormat类中getSplits(JobContext job)方法)分析 *****
   1)切片大小：Math.max(minSize, Math.min(maxSize, blockSize))
   2)minSize(切片最小值)：mapreduce.input.fileinputformat.split.minsize=1 默认值为1(Hadoop配置文件属性)
   3)maxSize(切片最大值)：mapreduce.input.fileinputformat.split.maxsize=Long.MAXValue 默认值Long.MAXValue
   4)综上，默认切片大小=blockSize
   5)调整分片大小：修改参数minSize和maxSize即可
*******************************************************************
3、每个MapTask(逻辑分区)默认使用TextInputFormat获取任务(数据)将清洗结果(<k,1>...)
  保存到环形缓冲区(边写边读；默认大小100M，阈值80%，到达阈值时必须读取数据)
4、分区(默认1)：Reduce执行前，MR默认使用HashPartitioner对数据按照Key的hash % reduceTask
  比较均衡的将Map输出分配给每一个reduceTask避免数据倾斜
5、排序：MR默认按照Mapper输出数据的key进行升序排序(key.compareTo()方法)；
   预聚合：Combiner是MR提供的一个Map端的预聚合机制，Combiner对每个分区的数据进行合并(一个Combiner就相当于一个Reduce)
6、ReduceTask根据分区编号读取每个分区的数据，进行归并并排序，输出结果
*/

// 由于默认TextInputFormat每个文件至少产生一个分片，如果原始数据是大量小文件会导致启动过多MapTask线程(IO次数增多)导致MR性能下降

// MR优化：
// 多个小文件(默认每个单独成片)合并到同一个切片上

// 方式1：使用MR提供的可以跨文件进行分片合并的输入格式化类CombineTextInputFormat
// 方式2：自定义输入文件合并格式化类（见FileInputMergeFormat类、JobInputSplitDriver类）
public class JobInputSplitCombineDriver {

    @Test
    public void driver() throws Exception {
        Job job = Job.getInstance();
        // 1、Mapper阶段
        // ...

        // 方式1：
        // MR提供了可以跨文件进行分片合并的输入格式化类CombineTextInputFormat
        // 1)设置多个小文件处理到同一个切片上(切片大小：128M)
        job.setInputFormatClass(CombineTextInputFormat.class);
        // 2)设置切片的最小大小
        CombineTextInputFormat.setMinInputSplitSize(job,128*1024*1024);
        // 3)设置切片的最大大小
        CombineTextInputFormat.setMaxInputSplitSize(job,128*1024*1024);

        // 2、Reduce阶段
        // ...

        // 3、Driver阶段
        job.setJarByClass(JobInputSplitCombineDriver.class);
        // 4、输入输出路径
        FileInputFormat.setInputPaths(job,new Path("C:\\Users\\cc\\Desktop\\temp\\weblogs"));
        FileOutputFormat.setOutputPath(job,new Path("C:\\Users\\cc\\Desktop\\combineMerge"));
        // 5、执行
        System.out.println(job.waitForCompletion(true));
    }

}


