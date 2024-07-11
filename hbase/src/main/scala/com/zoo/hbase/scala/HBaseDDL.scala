package com.zoo.hbase.scala

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{ColumnFamilyDescriptorBuilder, Connection, TableDescriptor, TableDescriptorBuilder}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConverters._

object HBaseDDL {
  val connection: Connection = com.zoo.hbase.HBaseDDL.connection

  def createTable(nameSpace: String, tableName: String, columnFamilies: String*): Unit = {
    val admin = connection.getAdmin

    val tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(nameSpace, tableName))

    for (columnFamily <- columnFamilies) {
      val columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily))
      columnFamilyDescriptorBuilder.setMaxVersions(5)
      // 创建添加完参数的列族描述
      tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build())
    }

    // 2.4 对应当前的列族添加参数
    // 添加版本参数

    admin.createTable(tableDescriptorBuilder.build())

    admin.close()
  }

  def main(args: Array[String]): Unit = {
    createTable("big_data", "scala", "list", "map")
  }
}
