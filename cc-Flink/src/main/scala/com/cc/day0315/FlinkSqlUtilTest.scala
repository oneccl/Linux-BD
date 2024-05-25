package com.cc.day0315


/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/17
 * Time: 11:49
 * Description:
 */
object FlinkSqlUtilTest {

  def main(args: Array[String]): Unit = {
    // 1、执行SQL
    val create =
      """
        |create table source_mysql(
        |  name string,
        |  num bigint
        |) with (
        |  'connector' = 'jdbc',
        |  'url' = 'jdbc:mysql://localhost:3306/day0316_flinksql',
        |  'table-name' = 'name_num',
        |  'username' = 'root',
        |  'password' = '123456'
        |)
        |""".stripMargin
    //FlinkSqlUtil.execSql(create).print()
    val select =
      """
        |select * from source_mysql
        |""".stripMargin
    //FlinkSqlUtil.execSql(select).print()


    // 2、读取外部.sql文件
    val path = "D:\\JavaProjects\\Linux-BD\\cc-Hive\\sql\\day0221\\exercises1.sql"
    val result1 = FlinkSqlUtil.exec(path)
    //result1.print()

  }

}
