package com.zoo.database

/**
 * Author: JMD
 * Date: 7/6/2023
 */
object TableUtils {
  def getMysqlUpdateSql(table: String, rows: Array[Array[AnyRef]], databaseColumnNames: Array[(String, Class[_])], keys: Array[String]): String = {

    rows.foreach(row => {
      assert(row.length == databaseColumnNames.length)
    })
    rows.foreach {
      row: Array[AnyRef] =>
        keys

    }
    s"""
       |update $table
       |set
       |where 1 = 1
       |""".stripMargin
  }
}
