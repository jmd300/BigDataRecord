package com.zoo.database

import com.zoo.database.Utils.printListArray

object LionfulDb extends DataBaseConfig {
  driver = Drivers.mysql
  url = "jdbc:mysql://172.16.51.100:3306/lionful?useServerPrepStmts=true"
  username = "root"
  password = "lionful"

  def main(args: Array[String]): Unit = {

  }
}
