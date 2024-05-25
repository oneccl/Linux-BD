package com.SparkMLlib

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql._

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/4/3
 * Time: 19:55
 * Description:
 */
object LinearRegression {

  // 线性回归和预测
  // 销量预测

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("LinearRegression").master("local[2]").getOrCreate()
    // 1、读取数据样本
    val df = spark.read.format("csv")
      .option("header","true")  // 使用首行作为表头
      .load("D:\\JavaProjects\\Linux-BD\\cc-Spark\\src\\main\\scala\\com\\SparkMLlib\\sale.csv")
    //df.show()
    // 2、数据筛选、类型转换
    import spark.implicits._
    val df1 = df.select("month", "sale")
      .map(r => (r.getString(0).toInt, r.getString(1).toDouble))
      .toDF("month", "sale")
    //df1.show()

    // 3、将特征列合并为特征向量，VectorAssembler是一个转换器
    val vector = new VectorAssembler()
      .setInputCols(Array("sale"))  // 特征列
      .setOutputCol("features")     // 特征向量列

    // String列不支持
    val df3:DataFrame = vector.transform(df1)
    df3.show()
    // 添加了特征向量(最后一列)的数据帧如下
    /*
    | month | sale | features |
    |     1 |  9.3 |    [9.3] |
    |     2 | 10.4 |   [10.4] |
    ...
    */
    // 4、将历史数据按照8:2抽取出来，构建测试集和训练集
    val trainSet = Array(0.8, 0.2)
    val testSet = 1000
    // def randomSplit(weights: Array[Double], seed: Long): Array[Dataset[T]] = {}
    val array = df3.randomSplit(trainSet, testSet)

    // 5、构建模型，设置回归参数和正则化参数
    // 线性回归参数
    val regression = new LinearRegression()
      .setLabelCol("sale")    // 标签列
      .setFeaturesCol("features")  // 特征向量列
      .setFitIntercept(true)  // 是否有w0截距(最终绘制的方程是否经过坐标轴原点，true为允许不经过原点)
      // 正则化参数
      .setTol(0.001)          // 收敛值(公差)，越小越精确，设置此值以达到均方差最小
      .setMaxIter(100)        // 迭代次数，设置此值以达到均方差最小
      .setRegParam(0.3)       // 正则因子(默认0)
      .setElasticNetParam(0.8)

    // 6、生成训练模型，并对测试集进行预测
    // 6.1、使用fit()生成训练模型
    val model = regression.fit(array(0))

    println("权重: "+model.coefficients)    // [0.8205579023771011]
    println("截距: "+model.intercept)       // 2.1562958731018345
    println("特征数: "+model.numFeatures)   // 1

    // 6.2、对测试集进行预测
    // 方式1：
    val result:DataFrame = model.transform(array(1))
    // 预测结果
    result.show()

    println("-----------------")

    // 方式2：
    val summary = model.evaluate(array(1))
    val predictions:DataFrame = summary.predictions

    // 均方差: 越小越好
    println("均方差: "+summary.meanSquaredError)        // 0.5595641159463838
    println("平均绝对值误差: "+summary.meanAbsoluteError) // 0.6091062313755066
    println("测试数据集数量: "+summary.numInstances)      // 3
    // 模型拟合度: 接近1越好
    println("模型拟合度: "+summary.r2)                   // 0.9178989722282775

    // 预测结果
    predictions.show()
    /*
    | month | sale | features |         prediction |
    |     4 | 11.8 |   [11.8] | 11.838879121151628 |
    |     8 | 15.9 |   [15.9] | 15.203166520897742 |
    |     9 | 18.1 |   [18.1] | 17.008393906127367 |
    */

    spark.stop()

  }

}
