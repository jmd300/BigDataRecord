package utils

/**
 * 读取spark-submit输入的 main 参数
 * @param args 形如：path=/home/data.csv year=2023
 * @param msg 日志中的标识字符串，如果出现找不到的 key 输出到log
 */
class ArgsMap(args: Array[String], msg: String = "") extends Serializable {
  private val argsMap: Map[String, String] = args.map(e => {
    val idx = e.indexOf('=')
    e.slice(0, idx) -> e.slice(math.min(idx + 1, e.length - 1), e.length)
  }).toMap

  def apply(key: String, log: String = ""): String = {
    if (argsMap.contains(key)) argsMap(key)
    else {
      println(msg)
      println(s"MY_LOG: ArgsMap '$key' not found ", log)
      System.exit(1)
      ""
    }
  }

  def apply(keys: List[String]): List[String] = keys.map(key => this.apply(key))

  def contains(key: String): Boolean = argsMap.contains(key)

  def getOrElse(key: String, v: String): String = argsMap.getOrElse(key, v)

  override def toString: String = argsMap.toString

  def toMap: Map[String, String] = argsMap
}