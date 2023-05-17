package com.zoo.database

class DataBaseConfig {
  var driver = ""
  /**
   * 切记 url中不可以出现换行符，否则报错 SqlException, 不可使用
   * s"""
   * |
   * |""".stripMargin
   */
  var url = ""
  var username = ""
  var password = ""
}
