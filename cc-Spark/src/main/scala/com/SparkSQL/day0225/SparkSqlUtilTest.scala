package com.SparkSQL.day0225

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/30
 * Time: 20:17
 * Description:
 */
object SparkSqlUtilTest {

  def main(args: Array[String]): Unit = {

    val df = SparkSqlUtil.spark.read.text("C:\\Users\\cc\\Desktop\\temp\\log-20211101.txt")
    df.createTempView("v1")

    val sql =
      """
        |select
        |arr[0] ip,
        |split(arr[3],':')[1] hour,
        |arr[8] code,
        |arr[9] upload,
        |arr[size(arr)-1] download
        |from
        |(select split(value,' ') arr from v1)
        |where arr[8]==200
        |""".stripMargin
    SparkSqlUtil.execSql(sql).show()

    val path = "D:\\JavaProjects\\Linux-BD\\cc-Hive\\sql\\day0221\\exercises1.sql"
    //SparkSqlUtil.exec(path)

  }

}
