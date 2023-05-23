package com.zoo.database

/**
 * Author: JMD
 * Date: 5/19/2023
 */
object MysqlServer {
  def getLionfulServer(): IMysql = {
    new IMysql(LionfulDb)
  }
}
