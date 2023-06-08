package com.zoo.lang.scala.test

/**
 * Author: JMD
 * Date: 6/8/2023
 *
 * object 也可以继承一个类，这时它会继承该类中的所有非私有成员（包括方法和字段），包括非静态方法。
 * 当一个 object 继承了一个类时，它本质上就是这个类的一个单例对象。也就是说，它与普通的类一样具有类型，可以被视为类的一个实例，并且可以访问该类的非私有成员方法和属性。
 */
object Boy extends Person {
  def main(args: Array[String]): Unit = {
    println(name)
  }
}
