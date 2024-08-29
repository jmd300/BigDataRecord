package com.zoo.spark.sql

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.util.control.Breaks.{break, breakable}


object SqlExample {
  val spark = SparkSession
    .builder()
    .appName(this.getClass.getSimpleName).master("local")
    .getOrCreate()

  def example1(): Unit = {
    // 1. 查询订单明细表（order_detail）中销量（下单件数）排名第二的商品id，如果不存在返回null，如果存在多个排名第二的商品则需要全部返回。
    // order_detail_id (订单明细id)	order_id (订单id)	sku_id (商品id)	create_date (下单日期)	price (商品单价)	sku_num (商品件数)
    val df1 = spark.createDataFrame(Seq(
      (1, 1, 1, "2021-09-30", 2000.00, 2),
      (2, 1, 3, "2021-09-30", 5000.00, 5),
      (22, 10, 4, "2020-10-02", 6000.00, 1),
      (23, 10, 5, "2020-10-02", 500.00, 24),
      (24, 10, 6, "2020-10-02", 2000.00, 5),
    )).toDF("order_detail_id", "order_id", "sku_id", "create_date", "price", "sku_num")
    df1.createTempView("order_detail")

    spark.read.parquet()
    val sql1 =
      s"""
         |select sku_id
         |from(
         |  select sku_id, rank() over(order by sale_num desc) as rank_id
         |  from (
         |    select sku_id, sum(sku_num) as sale_num
         |    from order_detail
         |    group by sku_id
         |  )
         |)
         |where rank_id = 2
         |""".stripMargin

    spark.sql(sql1).show()
  }
  def example2(): Unit = {
    // 2.查询至少连续三天下单的用户
    // 查询订单信息表(order_info)中最少连续3天下单的用户id，期望结果如下：
    // order_id (订单id)	user_id (用户id)	create_date (下单日期)	total_amount (订单金额)
    val df2 = spark.createDataFrame(Seq(
        (1, 101, "2021-09-30", 29000.00),
        (2, 101, "2021-10-01", 29000.00),
        (3, 101, "2021-10-02", 29000.00),
        (10, 103, "2020-10-02", 28000.00),
        (12, 104, "2020-10-03", 28000.00)
      ))
      .toDF("order_id", "user_id", "create_date", "total_amount")
    df2.createTempView("order_info2")

    spark.sql(
        s"""
           |select user_id
           |from(
           |  select user_id, date_add(create_date, 2) as add_2_date, lead(create_date, 2, null) over(partition by user_id order by create_date asc) as lead_2_date
           |  from
           |  (
           |    select user_id, create_date
           |    from order_info2
           |    group by user_id, create_date
           |  )
           |)
           |where add_2_date = lead_2_date
           |group by user_id
           |""".stripMargin)
      .show()
  }

  def example3(): Unit = {
    // 3. 查询各品类销售商品的种类数及销量最高的商品
    // order_detail（订单明细表）
    // order_detail_id (订单明细id)	order_id (订单id)	sku_id (商品id)	create_date (下单日期)	price (商品单价)	sku_num (商品件数)
    // 1	                        1	                1	              2021-09-30	          2000.00	        2
    // 2	                        1	                3	              2021-09-30	          5000.00	        5
    // 22	                        10	              4	              2020-10-02	          6000.00	        1
    // 23	                        10	              5	              2020-10-02	          500.00	        24
    // 24	                        10	              6	              2020-10-02	          2000.00	        5
    // 商品信息表：sku_info
    // sku_id (商品id)	name (商品名称)	category_id (分类id)	from_date (上架日期)	price (商品价格)
    // 1	            xiaomi        1 2020-01-01	2000
    // 6	            洗碗机	        2	2020-02-01	2000
    // 9	            自行车	        3	2020-01-01	1000
    // 商品分类信息表：category_info
    // category_id (分类id)	category_name (分类名称)
    // 1	                  数码
    // 2	                  厨卫
    // 3	                  户外
    // 期望结果如下：
    // category_id	category_name	sku_id	name	order_num	sku_cnt（所在品类中，商品的数量）
    // 1	          数码	          2	      手机壳	302	      4
    // 2	          厨卫	          8	      微波炉	253	      4
    // 3	          户外	          12	    遮阳伞	349	      4

    val d3_1 = spark.createDataFrame(Seq(
      (1,	 1,  1, "2021-09-30", 2000.00, 2),
      (2,	 1,  3, "2021-09-30", 5000.00, 5),
      (22, 10, 4, "2020-10-02", 6000.00, 1),
      (23, 10, 5, "2020-10-02", 500.00 , 24),
      (24, 10, 6, "2020-10-02", 2000.00, 5)
    ))
      .toDF("order_detail_id",	"order_id", "sku_id", "create_date",  "price", "sku_num")

    d3_1.createTempView("order_detail3")

    // sku_info
    val df3_2 = spark.createDataFrame(Seq(
      (1, "xiaomi", 1, "2020-01-01",	2000),
      (6,	"洗碗机",	2, "2020-02-01",	2000),
      (9,	"自行车",	3, "2020-01-01",	1000)
    )).toDF("sku_id", 	"name", 	"category_id",	"from_date", "price")

    df3_2.createTempView("sku_info")

    val df3_3 = spark.createDataFrame(Seq(
      (1, "数码"),
      (2, "厨卫"),
      (3, "户外")
    )).toDF("category_id", "category_name")
    df3_3.createTempView("category_info")

    spark.sql(
      s"""
         |select *
         |from(
         |  select category_id, sku_id, name, order_num, rank() over(partition by category_id order by order_num desc) as rank_id
         |  from(
         |    select category_id, sku_id, name, sum(sku_num) as order_num
         |    from(
         |      select order_detail3.sku_id, sku_num, name, category_id
         |      from order_detail3 join sku_info
         |      on order_detail3.sku_id = sku_info.sku_id
         |    )
         |    group by category_id, sku_id, name
         |  )
         |)
         |where rank_id = 1
         |""".stripMargin)
      .createTempView("tmp3")

    println("tmp3")
    spark.sql("select * from tmp3").show()

    spark.sql(
      s"""
         |select *
         |from tmp3 t1
         |join
         |(
         |  select sku_info.category_id, category_name, count(*) as sku_cnt
         |  from sku_info join category_info
         |  on sku_info.category_id = category_info.category_id
         |  group by sku_info.category_id, category_name
         |)t2
         |on t1.category_id = t2.category_id
         |""".stripMargin)
      .show()
  }
  def example4(): Unit = {
    // 查询用户的累计消费金额及VIP等级
    // 题目需求：
    //从订单信息表(order_info)中统计每个用户截止其每个下单日期的累积消费金额，以及每个用户在其每个下单日期的VIP等级。
    //用户vip等级根据累积消费金额计算，计算规则如下：
    //设累积消费总额为X，
    //若0=<X<10000,则vip等级为普通会员
    //若10000<=X<30000,则vip等级为青铜会员
    //若30000<=X<50000,则vip等级为白银会员
    //若50000<=X<80000,则vip为黄金会员
    //若80000<=X<100000,则vip等级为白金会员
    //若X>=100000,则vip等级为钻石会员
    // 期望结果如下：
    //
    //user_id (用户id)	create_date (下单日期)	sum_so_far <decimal(16,2)> (截至每个下单日期的累计下单金额)	vip_level (每个下单日期的VIP等级)
    //101	2021-09-27	29000.00	青铜会员

    // 订单信息表：order_info
    //order_id (订单id)	user_id (用户id)	create_date (下单日期)	total_amount (订单金额)
    //1	101	2021-09-30	29000.00
    //10	103	2020-10-02	28000.00
    spark.createDataFrame(Seq(
      (1,	101,	"2021-09-30",	29000.00),
      (10,	103,	"2020-10-02",	28000.00)
    ))
      .toDF("order_id", "user_id", "create_date", "total_amount")
      .createTempView("order_info")

    spark.sql(
      s"""
         |select user_id, create_date, sum_so_far,
         |               case
         |                 when sum_so_far >= 0 and sum_so_far < 10000 then '普通会员'
         |                 when sum_so_far >= 10000 and sum_so_far < 30000 then '青铜会员'
         |                 when sum_so_far >= 30000 and sum_so_far < 50000 then '白银会员'
         |                 when sum_so_far >= 50000 and sum_so_far < 80000 then '黄金会员'
         |                 when sum_so_far >= 80000 and sum_so_far < 100000 then '白金会员'
         |                 when sum_so_far >= 100000 then '钻石会员'
         |                 else ''
         |               end as vip_level
         |from(
         |  select user_id, create_date, sum(total_amount) over(partition by user_id order by create_date asc) as sum_so_far
         |  from order_info
         |)
         |""".stripMargin).show()

  }

  def example5(): Unit = {
    // 查询首次下单后第二天连续下单的用户比率
    // 从订单信息表(order_info)中查询首次下单后第二天仍然下单的用户占所有下单用户的比例，结果保留一位小数，使用百分数显示，
    // 期望结果如下：
    // percentage
    // 70.0%
    // 订单信息表：order_info
    // order_id (订单id)	user_id (用户id)	create_date (下单日期)	total_amount (订单金额)
    // 1	101	2021-09-30	29000.00
    // 2	102	2021-09-30	29000.00
    // 3	103	2021-09-30	29000.00
    // 4	104	2021-09-30	29000.00

    // 5	101	2021-09-30	29000.00
    // 6	103	2020-10-02	28000.00
    // 7	103	2020-10-02	28000.00
    spark.createDataFrame(Seq(
      (1,	101,	"2021-09-30",	100.00),
      (2,	102,	"2021-09-30",	100.00),
      (3,	103,	"2021-09-30",	100.00),
      (4,	104,	"2021-09-30",	100.00),

      (5,	101,	"2021-10-01",	100.00),
      (6,	103,	"2020-10-01",	100.00),
      (7,	105,	"2020-10-01",	100.00) ,

      (8, 101, "2021-10-02", 100.00),
      (9, 102, "2020-10-02", 100.00),
      (10, 104, "2020-10-02", 100.00),
    )).toDF("order_id", "user_id", "create_date", "total_amount")
      .createTempView("order_info")

    spark.sql(
      s"""
         |select concat(round(count(distinct t2.user_id) / count(distinct t1.user_id),1) * 100,'%') as percentage
         |from
         |order_info as t1,(
         |  select user_id
         |  from(
         |    select user_id, datediff(create_date, first_value(create_date) over(partition by user_id order by create_date asc) ) as date_diff
         |    from(
         |      select user_id, create_date
         |      from order_info
         |      group by user_id, create_date
         |    )
         |  )
         |  where date_diff = 1
         |)t2
         |""".stripMargin)
      .show()
  }
  def example6(): Unit = {
    // 从订单明细表(order_detail)统计每个商品销售首年的年份，销售数量和销售总额。
    // 期望结果如下：
    // sku_id (商品id)	year (销售首年年份)	order_num (首年销量)	order_amount <decimal(16,2)> (首年销售金额)
    // 1	2020	2	4000.00
    // 订单明细表：order_detail
    //order_detail_id(订单明细id)	order_id(订单id)	sku_id(商品id)	create_date(下单日期)	price(商品单价)	sku_num(商品件数)
    //1	1	1	2021-09-30	2000.00	2
    //2	1	3	2021-09-30	5000.00	5
    //22	10	4	2020-10-02	6000.00	1
    //23	10	5	2020-10-02	500.00	24
    spark.createDataFrame(Seq(
      (1,	 1,	 1,	"2021-09-30",	2000.00,	2),
      (2,	 1,	 3,	"2021-09-30",	5000.00,	5),
      (3,	 2,	 3,	"2021-10-30",	5000.00,	2),
      (4,	 4,	 3,	"2022-09-30",	5000.00,	5),
      (22, 10, 4,	"2020-10-02",	6000.00,	1),
      (23, 10, 5,	"2020-10-02",	500.00,	 24)
    )).toDF("order_detail_id", "order_id", "sku_id", "create_date", "price", "sku_num")
      .createTempView("order_detail")
    spark.sql(
      s"""create TEMPORARY view tmp as
         |select create_year, sku_id, price, sku_num
         |from(
         |  select sku_id, year(create_date) as create_year, price, sku_num
         |  from order_detail
         |)
         |""".stripMargin)
    spark.sql(
      s"""
         |select sku_id, create_year, sum(price * sku_num) as order_amount, sum(sku_num) as order_num
         |from(
         |  select t1.sku_id, t1.create_year, price, sku_num
         |  from tmp as t1
         |  join
         |  (
         |    select *
         |    from(
         |      select sku_id, create_year, rank() over(partition by sku_id order by create_year) as rank_id
         |      from(
         |        select sku_id, create_year
         |        from tmp
         |        group by sku_id, create_year
         |      )
         |    )
         |    where rank_id = 1
         |  )t2
         |  on t1.sku_id = t2.sku_id
         |  and t1.create_year = t2.create_year
         |)
         |group by sku_id, create_year
         |""".stripMargin
    ).show()
  }
  def example7(): Unit = {
    // 筛选去年总销量小于100的商品
    // 从订单明细表(order_detail)中筛选出去年总销量小于100的商品及其销量，假设今天的日期是2022-01-10，不考虑上架时间小于一个月的商品
    //期望结果如下：
    //sku_id （商品id）	name （商品名称）	order_num （销量）
    //1	xiaomi 10	49
    //3	apple 12	35
    //4	xiaomi 13	53
    //6	洗碗机	26
    // 商品信息表：sku_info
    //sku_id(商品id)	name(商品名称)	category_id(分类id)	from_date(上架日期)	price(商品价格)
    //1	xiaomi 10	1	2020-01-01	2000
    //6	洗碗机	2	2020-02-01	2000
    //9	自行车	3	2020-01-01	1000
    //订单明细表：order_detail
    //
    //order_detail_id(订单明细id)	order_id(订单id)	sku_id(商品id)	create_date(下单日期)	price(商品单价)	sku_num(商品件数)
    //1	1	1	2021-09-30	2000.00	2
    //2	1	3	2021-09-30	5000.00	5
    //22	10	4	2020-10-02	6000.00	1
    //23	10	5	2020-10-02	500.00	24
    //24	10	6	2020-10-02	2000.00	5
    spark.createDataFrame(Seq(
      (1,	"xiaomi", 10, "2019-01-01",	2000),
      (3,	"洗碗机",	2,  "2019-02-01",	2000),
      (9,	"自行车",	3,  "2020-01-01",	1000),
    )).toDF("sku_id", "name", "category_id", "from_date", "price")
      .createTempView("sku_info")

    spark.createDataFrame(Seq(
        (1,	 1,	 1,	"2021-09-30", 2000.00, 2),
        (2,	 1,	 3,	"2021-09-30", 5000.00, 5),
        (22, 10, 4,	"2020-10-02", 6000.00, 1),
        (23, 10, 5,	"2020-10-02", 500.00, 24),
        (24, 10, 6,	"2020-10-02", 2000.00, 5)
    )).toDF("order_detail_id", "order_id", "sku_id", "create_date", "price", "sku_num")
      .createTempView("order_detail")

    spark.sql(
      s"""
         |select
         |	t1.sku_id,
         |    si.name,
         |    t1.order_num
         |from (
         |  select
         |	sku_id,
         |    sum(sku_num) as order_num
         |  from order_detail
         |  where datediff('2022-01-10',create_date)>30 and -- 不考虑小于一个月的数据
         |  year(create_date)='2021'
         |  group by sku_id
         |  having order_num<100 -- 对统计后的数据进行筛选
         |) t1
         |left join sku_info si on t1.sku_id = si.sku_id;
         |""".stripMargin)
      .show()
  }

  def example8(): Unit = {
    // 查询每日新用户数
    // 期望结果如下：
    //login_date_first （日期）	user_count （新增用户数）
    //2021-09-21	1
    //2021-09-22	1
    //2021-09-23	1
    //2021-09-24	1
    //2021-09-25	1
    //2021-09-26	1
    //2021-09-27	1
    //2021-10-04	2
    //2021-10-06	1
    // 需要用到的表：
    //用户登录明细表：user_login_detail
    //user_id(用户id)	ip_address(ip地址)	login_ts(登录时间)	logout_ts(登出时间)
    //101	180.149.130.161	2021-09-21 08:00:00	2021-09-27 08:30:00
    //102	120.245.11.2	2021-09-22 09:00:00	2021-09-27 09:30:00
    //103	27.184.97.3	2021-09-23 10:00:00	2021-09-27 10:30:00
    spark.createDataFrame(Seq(
        (101, "180.149.130.161",	"2021-09-21 08:00:00", "2021-09-27 08:30:00"),
        (102, "120.245.11.2",	"2021-09-22 09:00:00",	"2021-09-27 09:30:00"),
        (103,	"27.184.97.3",	"2021-09-23 10:00:00",	"2021-09-27 10:30:00"),
    )).toDF("user_id", "ip_address", "login_ts", "logout_ts")
      .createTempView("user_login_detail")

    // TODO: 每个用户的第一次登录信息组成了这一天的新用户数量
    spark.sql(
      s"""
         |select login_date, count(*) as new_user_count
         |from(
         |  select user_id, date_format(first_login_ts, 'yyyy-MM-dd') as login_date
         |  from(
         |    select user_id, first_value(login_ts) over(partition by user_id order by login_ts asc) as first_login_ts
         |    from user_login_detail
         |  )
         |  group by user_id, first_login_ts
         |)
         |group by login_date
         |order by login_date
         |""".stripMargin)
      .show()
  }
  def example9(): Unit = {
    // 统计每个商品的销量最高的日期
    // 从订单明细表（order_detail）中统计出每种商品销售件数最多的日期及当日销量，如果有同一商品多日销量并列的情况，取其中的最小日期。
    //期望结果如下：
    //sku_id （商品id）	create_date （销量最高的日期）	sum_num （销量）
    //1	2021-09-30	9
    // 需要用到的表：
    //订单明细表：order_detail
    //order_detail_id(订单明细id)	order_id(订单id)	sku_id(商品id)	create_date(下单日期)	price(商品单价)	sku_num(商品件数)
    //1	1	1	2021-09-30	2000.00	2
    //2	1	3	2021-09-30	5000.00	5
    //22	10	4	2020-10-02	6000.00	1
    //23	10	5	2020-10-02	500.00	24
    //24	10	6	2020-10-02	2000.00	5
    spark.createDataFrame(Seq(
      (1,	1, 1,	"2021-09-30", 2000.00, 2),
      (2, 1, 3,	"2021-09-30",	5000.00, 5),
      (22, 10, 4, "2020-10-02",	6000.00, 1),
      (23, 10, 5, "2020-10-02",	500.00,	24),
      (24, 10, 6, "2020-10-02",	2000.00, 5),
    )).toDF("order_detail_id", "order_id", "sku_id", "create_date", "price", "sku_num")
      .createTempView("order_detail")

    spark.sql(
      s"""
         |select sku_id, create_date, sum_num
         |from(
         |  select sku_id, create_date, sum_num, rank() over(partition by sku_id order by sum_num desc, create_date asc) as rank_id
         |  from(
         |    select sku_id, create_date, sum(sku_num) as sum_num
         |    from order_detail
         |    group by sku_id, create_date
         |  )
         |)
         |where rank_id = 1
         |""".stripMargin)
      .show()
  }

  def example10(): Unit = {
    // 查询销售件数高于品类平均数的商品
    //题目需求：
    //从订单明细表（order_detail）中查询累积销售件数高于其所属品类平均数的商品
    //期望结果如下：
    //sku_id	name	sum_num	cate_avg_num
    //2	手机壳	6044	1546

    // 需要用到的表：
    //
    //商品信息表：sku_info
    //
    //sku_id(商品id)	name(商品名称)	category_id(分类id)	from_date(上架日期)	price(商品价格)
    //1	xiaomi 10	1	2020-01-01	2000
    //6	洗碗机	2	2020-02-01	2000
    //9	自行车	3	2020-01-01	1000
    //订单明细表：order_detail
    //order_detail_id(订单明细id)	order_id(订单id)	sku_id(商品id)	create_date(下单日期)	price(商品单价)	sku_num(商品件数)
    //1	1	1	2021-09-30	2000.00	2
    //2	1	3	2021-09-30	5000.00	5
    //22	10	4	2020-10-02	6000.00	1
    //23	10	5	2020-10-02	500.00	24
    spark.createDataFrame(Seq(
        (1, "xiaomi", 10, "2019-01-01", 2000),
        (3, "洗碗机", 2, "2019-02-01", 2000),
        (6, "自行车", 3, "2020-01-01", 1000),
        (7, "电动车", 3, "2020-01-02", 2000),
      )).toDF("sku_id", "name", "category_id", "from_date", "price")
      .createTempView("sku_info")

    spark.createDataFrame(Seq(
        (1, 1, 1, "2021-09-30", 2000.00, 2),
        (2, 1, 3, "2021-09-30", 5000.00, 5),
        (22, 10, 4, "2020-10-02", 6000.00, 1),
        (23, 10, 5, "2020-10-02", 500.00, 24),
        (24, 10, 6, "2020-10-02", 2000.00, 5),
        (25, 11, 7, "2020-11-02", 2000.00, 10),
      )).toDF("order_detail_id", "order_id", "sku_id", "create_date", "price", "sku_num")
      .createTempView("order_detail")
    spark.sql(
      s"""
         |create temporary view tmp as
         |select category_id, od.sku_id, name, sum(sku_num) as sum_num -- 2.统计该品类下的对应sku_id的销售件数
         |from order_detail od
         |join sku_info si
         |on od.sku_id = si.sku_id
         |group by category_id, od.sku_id, name
         |""".stripMargin)
    spark.sql(
      s"""
         |select sku_id,
         |       name,
         |       sum_num,
         |       floor(cate_avg_num) as cate_avg_num
         |from(
         |  select category_id, sku_id, name, sum_num,
         |       avg(sum_num) over(partition by category_id
         |         rows between unbounded preceding and unbounded following) as cate_avg_num
         |  from tmp
         |)
         |where sum_num > cate_avg_num
         |""".stripMargin)
      .show()

  }
  def example11(): Unit = {
    // 从用户登录明细表（user_login_detail）和订单信息表（order_info）中查询每个用户的注册日期（首次登录日期）、总登录次数以及其在2021年的登录次数、订单数和订单总额。
    // 期望结果如下：
    //user_id (用户id)	register_date (注册日期)	total_login_count (累积登录次数)	login_count_2021 (2021年登录次数)	order_count_2021 (2021年下单次数)	order_amount_2021 (2021年订单金额) <decimal(16,2)>
    //101	2021-09-21	5	5	4	143660.00
    //102	2021-09-22	4	4	4	177850.00
    // 需要用到的表：
    //
    //用户登录明细表：user_login_detail
    //
    //user_id(用户id)	ip_address(ip地址)	login_ts(登录时间)	logout_ts(登出时间)
    //101	180.149.130.161	2021-09-21 08:00:00	2021-09-27 08:30:00
    //102	120.245.11.2	2021-09-22 09:00:00	2021-09-27 09:30:00
    //103	27.184.97.3	2021-09-23 10:00:00	2021-09-27 10:30:00
    //订单信息表：order_info
    //order_id (订单id)	user_id (用户id)	create_date (下单日期)	total_amount (订单金额)
    //1	101	2021-09-30	29000.00
    //10	103	2020-10-02	28000.00
    spark.createDataFrame(Seq(
        (101, "180.149.130.161", "2021-09-21 08:00:00", "2021-09-27 08:30:00"),
        (102, "120.245.11.2", "2021-09-22 09:00:00", "2021-09-22 09:30:00"),
        (102, "120.245.11.2", "2021-09-23 09:00:00", "2021-09-23 09:30:00"),
        (103, "27.184.97.3", "2021-09-23 10:00:00", "2021-09-27 10:30:00"),
      )).toDF("user_id", "ip_address", "login_ts", "logout_ts")
      .createTempView("user_login_detail")

    spark.createDataFrame(Seq(
        (1, 101, "2021-09-30", 100.00),
        (2, 102, "2021-09-30", 100.00),
        (22, 102, "2021-08-30", 150.00),
        (3, 103, "2021-09-30", 100.00),
        (4, 104, "2021-09-30", 100.00),

        (5, 101, "2021-10-01", 100.00),
        (6, 103, "2020-10-01", 100.00),
        (7, 105, "2020-10-01", 100.00),

        (8, 101, "2021-10-02", 100.00),
        (9, 102, "2020-10-02", 100.00),
        (10, 104, "2020-10-02", 100.00),
      )).toDF("order_id", "user_id", "create_date", "total_amount")
      .createTempView("order_info")

    spark.sql(
      s"""
         |select t1.user_id, regist_date, total_login_count, login_count_2021, order_count_2021, order_amount_2021
         |from(
         |  select user_id, regist_date,
         |         count(*) as total_login_count,
         |         sum(if(year(login_date) = '2021', 1, 0)) as login_count_2021
         |  from(
         |    select user_id,
         |           date_format(first_value(login_ts) over(partition by user_id order by login_ts), 'yyyy-MM-dd') as regist_date,
         |           date_format(login_ts, 'yyyy-MM-dd') as login_date
         |    from user_login_detail
         |  )
         |  group by user_id, regist_date
         |)t1
         |join
         |(
         |  select user_id, count(*) as order_count_2021, sum(total_amount) as order_amount_2021
         |  from(
         |    select user_id, total_amount
         |    from order_info
         |    where year(create_date) = '2021'
         |  )
         |  group by user_id
         |)t2
         |on t1.user_id = t2.user_id
         |""".stripMargin)
      .show()
  }

  def example12(): Unit = {
    // SQL 十二：查询指定日期的全部商品价格
    // 查询所有商品（sku_info表）截至到2021年10月01号的最新商品价格（需要结合价格修改表进行分析）
    // 商品信息表：sku_info
    //
    //sku_id(商品id)	name(商品名称)	category_id(分类id)	from_date(上架日期)	price(商品价格)
    //1	xiaomi 10	1	2020-01-01	2000
    //2	洗碗机	2	2020-02-01	2000
    //3	自行车	3	2020-01-01	1000

    //商品价格变更明细表：sku_price_modify_detail
    //sku_id(商品id)	new_price(本次变更之后的价格)	change_date(变更日期)
    //1	1900.00	2021-09-25
    //1	2000.00	2021-09-26
    //2	80.00	2021-09-29
    //2	10.00	2021-09-30
    spark.createDataFrame(Seq(
      (1,	"xiaomi", 10,	"2020-01-01",	2000),
      (2,	"洗碗机",	2,	"2020-02-01",	2000),
      (3,	"自行车",	3,	"2020-01-01",	1000)
    )).toDF("sku_id", "name", "category_id", "from_date", "price")
      .createTempView("sku_info")
    spark.createDataFrame(Seq(
      (1,	1900.00,	"2021-09-25"),
      (1,	2000.00,	"2021-09-26"),
      (2,	80.00,	"2021-09-29"),
      (2,	10.00,	"2021-09-30")
    ))
      .toDF("sku_id", "new_price", "change_date")
      .createTempView("sku_price_modify_detail")

    spark.sql(
      s"""
         |select si.sku_id, name, price as first_price, new_price, IFNULL(new_price, price) as current_price
         |from sku_info si
         |left join(
         |  select sku_id, new_price
         |  from(
         |    select sku_id, new_price, rank() over(partition by sku_id order by change_date desc) as rank_id
         |    from sku_price_modify_detail
         |    where change_date <= '2021-10-01'
         |  )
         |  where rank_id = 1
         |)t2
         |on si.sku_id = t2.sku_id
         |""".stripMargin)
      .show()
  }

  private def example13(): Unit = {
    // 订单配送中，如果期望配送日期和下单日期相同，称为即时订单，如果期望配送日期和下单日期不同，称为计划订单。
    //请从配送信息表（delivery_info）中求出每个用户的首单（用户的第一个订单）中即时订单的比例，保留两位小数，以小数形式显示。
    // 配送信息表：delivery_info
    //
    //delivery_id （运单 id ）	order_id （订单id）	user_id （用户 id ）	order_date （下单日期）	custom_date （期望配送日期）
    //1	1	101	2021-09-27	2021-09-29
    //2	2	101	2021-09-28	2021-09-28
    //3	3	101	2021-09-29	2021-09-30
    //4	4	102	2021-09-29	2021-09-30
    //5	5	102	2021-09-30	2021-09-30
    spark.createDataFrame(Seq(
      (1,	1,	101,	"2021-09-27",	"2021-09-27"),
      (2,	2,	101,	"2021-09-28",	"2021-09-28"),
      (3,	3,	101,	"2021-09-29",	"2021-09-30"),
      (4,	4,	102,	"2021-09-29",	"2021-09-30"),
      (5,	5,	102,	"2021-09-30",	"2021-09-30")
    )).toDF("delivery_id", "order_id", "user_id", "order_date", "custom_date")
      .createTempView("delivery_info")
    spark.sql(
      s"""
         |select cast(sum(is_instant) / count(*) as decimal(16, 2)) as rate
         |from(
         |  select user_id, IF(order_date = custom_date, 1, 0) as is_instant
         |  from(
         |    select user_id, order_date, custom_date, rank() over(partition by user_id order by order_date) as rank_id
         |    from delivery_info
         |  )
         |  where rank_id = 1
         |)
         |""".stripMargin)
      .show()
  }
  def example14(): Unit = {
    // 向用户推荐朋友收藏的商品
    // 现需要请向所有用户推荐其朋友收藏但是用户自己未收藏的商品，请从好友关系表（friendship_info）和收藏表（favor_info）中查询出应向哪位用户推荐哪些商品。
    // 期望结果如下：
    //
    //user_id （用户id）	sku_id （应向该用户推荐的商品id）
    //101	2
    //101	4
    // 需要用到的表：
    //
    //好友关系表：friendship_info
    //
    //user1_id（用户1 id）	user2_id（用户2 id）
    //101	1010
    //101	108
    //101	106
    //收藏表：favor_info
    //
    //user_id(用户id)	sku_id(商品id)	create_date(收藏日期)
    //101	3	2021-09-23
    //101	12	2021-09-23
    //101	6	2021-09-25
    // 假设friendship_info中的朋友关系是双向的
    spark.createDataFrame(Seq(
      (101,	102),
      (101,	103),
      (102,	103),
      (102, 101),
      (103, 101),
      (103, 102)
    ))
      .toDF("user1_id", "user2_id")
      .createTempView("friendship_info")

    spark.createDataFrame(Seq(
        (101,	1,	"2021-09-23"),
        (101,	2,	"2021-09-23"),
        (101,	3,	"2021-09-25"),
        (102,	4,	"2021-09-25"),
        (103,	5,	"2021-09-25")
    ))
      .toDF("user_id", "sku_id", "create_date")
      .createTempView("favor_info")

    // 用 SQL 实现的话就是很多多余的计算，虽然实现简单
    spark.sql(
      s"""
         |select t1.user1_id, sku_id
         |from friendship_info t1
         |join
         |favor_info t2
         |on t1.user2_id = t2.user_id
         |where(
         |  concat(t1.user1_id, sku_id)
         |  not in (
         |    select concat(user_id, sku_id)
         |    from favor_info
         |  )
         |)
         |group by t1.user1_id, sku_id
         |""".stripMargin)
      .show()

  }

  def example15(): Unit = {
    // 查询所有用户的连续登录两天及以上的日期区间
    // 期望结果如下：
    //user_id (用户id)	start_date (开始日期)	end_date (结束日期)
    //101	2021-09-27	2021-09-30
    //102	2021-10-01	2021-10-02
    //106	2021-10-04	2021-10-05
    //107	2021-10-05	2021-10-06
    // 登录明细表：user_login_detail
    //
    //user_id(用户id)	ip_address(ip地址)	login_ts(登录时间)	logout_ts(登出时间)
    //101	180.149.130.161	2021-09-21 08:00:00	2021-09-27 08:30:00
    //101	180.149.130.161	2021-09-21 08:00:00	2021-09-27 08:30:00
    //102	120.245.11.2	2021-09-22 09:00:00	2021-09-22 09:30:00
    //102	120.245.11.2	2021-09-24 09:00:00	2021-09-27 09:30:00
    //103	27.184.97.3	2021-09-23 10:00:00	2021-09-23 10:30:00
    //103	27.184.97.3	2021-09-24 10:00:00	2021-09-24 10:30:00

    spark.createDataFrame(Seq(
      (101,	"180.149.130.161",	"2021-09-21 08:00:00",	"2021-09-27 08:30:00"),
      (101,	"180.149.130.161",	"2021-09-22 08:00:00",	"2021-09-27 08:30:00"),
      (102,	"120.245.11.2",	"2021-09-22 09:00:00",	"2021-09-22 09:30:00"),
      (102,	"120.245.11.2",	"2021-09-24 09:00:00",	"2021-09-27 09:30:00"),
      (103,	"27.184.97.3",	"2021-09-23 10:00:00",	"2021-09-23 10:30:00"),
      (103,	"27.184.97.3",	"2021-09-24 10:00:00",	"2021-09-24 10:30:00")
    )).toDF("user_id", "ip_address", "login_ts", "logout_ts")
      .createTempView("user_login_detail")

    spark.sql(
      s"""
         |select user_id, count(*) as login_count, max(login_date) as max_login_date, min(login_date) as min_login_date
         |from(
         |  select user_id,
         |         login_date,
         |         date_sub(login_date, row_number() over(partition by user_id order by login_date)) as _date
         |  from(
         |    select user_id, date_format(login_ts, 'yyyy-MM-dd') as login_date
         |    from user_login_detail
         |    group by user_id, login_ts
         |  )
         |)
         |group by user_id, _date
         |having login_count >= 2
         |""".stripMargin)
      .show()
  }

  def example16(): Unit = {
    // 男性和女性每日的购物总金额统计
    // 从订单信息表（order_info）和用户信息表（user_info）中，分别统计每天男性和女性用户的订单总金额，如果当天男性或者女性没有购物，则统计结果为0。
    // 需要用到的表：
    //
    //订单信息表：order_info
    //order_id (订单id)	user_id (用户id)	create_date (下单日期)	total_amount (订单金额)
    //1	101	2021-09-30	29000.00
    //10	103	2020-10-02	28000.00

    //用户信息表：user_info
    //user_id(用户id)	gender(性别)	birthday(生日)
    //101	男	1990-01-01
    //102	女	1991-02-01
    //103	女	1992-03-01
    //104	男	1993-04-01

    spark.createDataFrame(Seq(
      (1,	101,	"2021-09-30",	9000.00),
      (2,	102,	"2020-10-02",	8000.00),
      (3,	103,	"2020-10-02",	1000.00),
      (4,	104,	"2020-10-02",	2000.00),
      (5,	104,	"2020-11-02",	8000.00),
      (6,	101,	"2020-11-02",	1000.00)
    ))
      .toDF("order_id", "user_id", "create_date", "total_amount")
      .createTempView("order_info")

    spark.createDataFrame(Seq(
      (101, "男",	"1990-01-01"),
      (102,	"女",	"1991-02-01"),
      (103,	"女",	"1992-03-01"),
      (104,	"男",	"1993-04-01"),
    )).toDF("user_id", "gender", "birthday")
      .createTempView("user_info")

    spark.sql(
      s"""
         |select create_date,
         |       sum(if(gender = '男', total_amount, 0)) as man_total_amount,
         |       sum(if(gender = '女', total_amount, 0)) as woman_total_amount
         |from order_info t1
         |join
         |user_info t2
         |on t1.user_id = t2.user_id
         |group by create_date
         |order by create_date
         |""".stripMargin)
      .show()
  }

  def example17(): Unit = {
    // 查询截止每天的最近3天内的订单金额总和以及订单金额日平均值，保留两位小数，四舍五入。
    // 期望结果
    // create_date (日期)	total_3d <decimal(16,2)> (最近3日订单金额总和)	avg_3d <decimal(16,2)> (最近3日订单金额日平均值)
    // 2020-10-08	75970.00	75970.00
    // 2021-09-27	104970.00	52485.00
    // 订单信息表：order_info
    //order_id (订单id)	user_id (用户id)	create_date (下单日期)	total_amount (订单金额)
    //1	101	2021-09-28	9000.00
    //2	102	2021-09-29	2000.00
    //3	103	2021-09-30	1000.00
    //4	104	2021-10-01	2000.00
    //5	105	2020-10-02	100.00
    //6	106	2020-10-02	1500.00
    spark.createDataFrame(Seq(
      (1, 101, "2021-09-28", 9000.00),
      (2, 102, "2021-09-29", 2000.00),
      (3, 103, "2021-09-30", 1000.00),
      (4, 104, "2021-10-01", 2000.00),
      (5, 105, "2021-10-02", 100.00),
      (6, 106, "2021-10-02", 1500.00)
    )).toDF("order_id", "user_id", "create_date", "total_amount")
      .createTempView("order_info")

    spark.sql(
      s"""
         |create temporary view t1 as
         |select create_date, sum(total_amount) as day_total_amount
         |from order_info
         |group by create_date;
         |""".stripMargin)
    spark.sql(
      s"""
         |select t1.create_date,
         |       sum(t2.day_total_amount) as 3days_total_amount,
         |       cast(mean(t2.day_total_amount) as decimal(16,2)) as avg_3d
         |from(
         |  t1 join t1 as t2
         |  on t2.create_date >= date_sub(t1.create_date, 2)
         |  and t2.create_date <= t1.create_date
         |)
         |group by t1.create_date
         |""".stripMargin)
      .show()
  }

  def example18(): Unit = {
    // 购买过商品1和商品2但是没有购买商品3的顾客
    // 从订单明细表(order_detail)中查询出所有购买过商品1和商品2，但是没有购买过商品3的用户，
    // 需要用到的表：
    //订单信息表：order_info
    //order_id (订单id)	user_id (用户id)	create_date (下单日期)	total_amount (订单金额)
    //1	101	2021-09-30	29000.00
    //2	103	2020-10-02	28000.00

    //订单明细表：order_detail
    //order_detail_id(订单明细id)	order_id(订单id)	sku_id(商品id)	create_date(下单日期)	price(商品单价)	sku_num(商品件数)
    //1	  1	  1	2021-09-30	2000.00	2
    //2	  1	  2	2021-09-30	5000.00	5
    //3	  2	  2	2020-10-02	6000.00	1
    //4	  2	  3	2020-10-02	500.00	24
    //5	  3	  1	2020-10-02	2000.00	5

    spark.createDataFrame(Seq(
      (1, 1, 1, "2021-09-30", 2000.00,	2),
      (2, 1, 2, "2021-09-30", 2000.00,	2),
      (3, 2, 2, "2020-10-02", 6000.00,	1),
      (4, 2, 3, "2020-10-02", 500.00	,24),
      (5, 3, 1, "2020-10-02", 2000.00,	5)
    )).toDF("order_detail_id", "order_id", "sku_id", "create_date", "price", "sku_num")
      .createTempView("order_detail")

    spark.createDataFrame(Seq(
      (1, 101,	"2021-09-30", 29000.00),
      (2, 103, "2020-10-02", 28000.00)
    )).toDF("order_id", "user_id", "create_date", "total_amount")
      .createTempView("order_info")

    spark.sql(
      s"""
         |select user_id
         |from(
         |  select user_id, collect_set(sku_id) as sku_id_set
         |  from
         |  (
         |    select order_id, user_id
         |    from order_info
         |  )t1
         |  join(
         |    select order_id, sku_id
         |    from order_detail
         |  )t2
         |  on t1.order_id = t2.order_id
         |  group by user_id
         |)
         |where array_contains(sku_id_set, 1)
         |and array_contains(sku_id_set, 2)
         |and not array_contains(sku_id_set, 3)
         |""".stripMargin)
      .show()
  }
  def example19(): Unit = {
    // 从订单明细表（order_detail）中统计每天商品1和商品2销量（件数）的差值（商品1销量-商品2销量）
    //期望结果如下：
    //create_date	diff
    //2020-10-08	-24
    //2021-09-27	2
    //2021-09-30	9
    //2021-10-01	-10
    //2021-10-02	-5800
    //2021-10-03	4
    //2021-10-04	-55
    //2021-10-05	-30
    //2021-10-06	-49
    //2021-10-07	-40
    // 订单明细表：order_detail
    //order_detail_id(订单明细id)	order_id(订单id)	sku_id(商品id)	create_date(下单日期)	price(商品单价)	sku_num(商品件数)
    //1	1	1	2021-09-30	2000.00	2
    //2	1	3	2021-09-30	5000.00	5
    //22	10	4	2020-10-02	6000.00	1
    //23	10	5	2020-10-02	500.00	24
    //24	10	6	2020-10-02	2000.00	5
    spark.createDataFrame(Seq(
      (1,  1,  1, "2021-09-30", 2000.00,	2),
      (2,  1,  2, "2021-09-30", 5000.00,	5),
      (22, 10, 1, "2020-10-02", 6000.00,	1),
      (23, 10, 2, "2020-10-02", 500.00,  24),
      (24, 10, 1, "2020-10-02", 2000.00,	5),
    )).toDF("order_detail_id", "order_id", "sku_id", "create_date", "price", "sku_num")
      .createTempView("order_detail")

    spark.sql(
      s"""
         |select create_date, sum(if(sku_id = '1', price * sku_num, 0)) - sum(if(sku_id = '2', price * sku_num, 0)) as diff
         |from order_detail
         |where sku_id = 1 or sku_id = 2
         |group by create_date
         |order by create_date
         |""".stripMargin)
      .show()
  }

  def example20(): Unit = {
    // 从订单信息表（order_info）中查询出每个用户的最近三个下单日期的所有订单//
    //期望结果如下：
    //user_id	order_id	create_date
    //101	2	2021-09-28
    //101	3	2021-09-29
    // 订单信息表：order_info
    //order_id (订单id)	user_id (用户id)	create_date (下单日期)	total_amount (订单金额)
    //1	101	2021-09-30	2000.00
    //2	102	2020-10-02	5000.00
    //3	101	2021-10-05	1000.00
    //4	102	2021-10-03	2000.00
    //5	101	2021-10-01	9000.00
    //6	102	2021-11-02	8000.00
    spark.createDataFrame(Seq(
      (1, 101, "2021-09-30", 2000.00),
      (2, 102, "2020-10-02", 5000.00),
      (3, 101, "2021-10-05", 1000.00),
      (4, 102, "2021-10-03", 2000.00),
      (5, 101, "2021-10-01", 9000.00),
      (6, 102, "2021-11-02", 8000.00)
    )).toDF("order_id", "user_id", "create_date", "total_amount")
      .createTempView("order_info")

    spark.sql(
      s"""
         |select  user_id, order_id, create_date
         |from(
         |  select user_id, order_id, create_date, dense_rank() over(partition by user_id order by create_date) as dense_rank_id
         |  from order_info
         |)
         |where dense_rank_id <= 3
         |order by user_id,create_date;
         |""".stripMargin)
    .show()
  }
  def example21(): Unit = {
    // 从登录明细表（user_login_detail）中查询每个用户两个登录日期（以login_ts为准）之间的最大的空档期。统计最大空档期时，用户最后一次登录至今的空档也要考虑在内，假设今天为2021-10-10。
    // 期望结果如下：
    //user_id （用户id）	max_diff （最大空档期）
    //101	10
    //102	9
    //103	10
    // 需要用到的表：
    //用户登录明细表：user_login_detail
    //user_id(用户id)	ip_address(ip地址)	login_ts(登录时间)	logout_ts(登出时间)
    //101	180.149.130.161	2021-09-21 08:00:00	2021-09-27 08:30:00
    //101	180.149.130.161	2021-09-28 08:00:00	2021-09-27 08:30:00
    //102	120.245.11.2	  2021-09-22 09:00:00	2021-09-27 09:30:00
    //102	120.245.11.2	  2021-09-30 09:00:00	2021-09-27 09:30:00
    //103	27.184.97.3	    2021-09-23 10:00:00	2021-09-27 10:30:00
    //103	27.184.97.3	    2021-10-01 10:00:00	2021-09-27 10:30:00
    spark.createDataFrame(Seq(
      (101,	"180.149.130.161",  "2021-09-21 08:00:00", "2021-09-27 08:30:00"),
      (101,	"180.149.130.161",  "2021-09-28 08:00:00", "2021-09-27 08:30:00"),
      (102,	"120.245.11.2	  ",  "2021-09-22 09:00:00", "2021-09-27 09:30:00"),
      (102,	"120.245.11.2	  ",  "2021-09-30 09:00:00", "2021-09-27 09:30:00"),
      (103,	"27.184.97.3	   ", "2021-09-21 10:00:00", "2021-09-27 10:30:00"),
      (103,	"27.184.97.3	   ", "2021-10-01 10:00:00", "2021-09-27 10:30:00")
    ))
      .toDF("user_id", "ip_address", "login_ts", "logout_ts")
      .createTempView("user_login_detail")
    spark.sql(
      s"""
         |select user_id, max(login_diff) as max_diff
         |from(
         |  select user_id, datediff(lead(login_date, 1, '2021-10-10') over(partition by user_id order by login_date), login_date) as login_diff
         |  from(
         |    select user_id, date_format(login_ts, 'yyyy-MM-dd') as login_date
         |    from user_login_detail
         |  )
         |)
         |group by user_id
         |order by user_id
         |""".stripMargin)
      .show()
  }

  def example22(): Unit = {
    // 从登录明细表（user_login_detail）中查询在相同时间段，多地登陆（ip_address不同）的用户
    //用户登录明细表：user_login_detail
    //
    //user_id(用户id)	ip_address(ip地址)	login_ts(登录时间)	logout_ts(登出时间)
    //101	180.149.130.161	2021-09-21 08:00:00	2021-09-27 08:30:00
    //102	120.245.11.2	2021-09-22 09:00:00	2021-09-27 09:30:00
    //103	27.184.97.3	2021-09-23 10:00:00	2021-09-27 10:30:00

    spark.createDataFrame(Seq(
      (101,	"180.149.130.161", "2021-09-21 08:00:00", "2021-09-27 08:30:00"),
      (101,	"180.149.130.163", "2021-09-25 08:00:00", "2021-09-27 08:30:00"),
      (102,	"120.245.11.2	  ", "2021-09-22 09:00:00", "2021-09-23 09:30:00"),
      (102,	"120.245.12.2	  ", "2021-09-24 09:00:00", "2021-09-27 09:30:00"),
      (103,	"27.184.97.3	  ", "2021-09-23 10:00:00", "2021-09-27 10:30:00")
    )).toDF("user_id", "ip_address", "login_ts", "logout_ts")
      .createTempView("user_login_detail")

    spark.sql(
      s"""
         |select distinct user_id
         |from(
         |  select user_id,
         |       ip_address,
         |       lead(login_ts, 1, '9999-12-31') over(partition by user_id order by login_ts) as lead_login_ts,
         |       lead(ip_address, 1, '') over(partition by user_id order by login_ts) as lead_ip_address,
         |       logout_ts
         |  from user_login_detail
         |)
         |where lead_login_ts < logout_ts
         |and lead_ip_address != ip_address
         |""".stripMargin)
      .show()
  }

  def example23(): Unit = {
    // 题目需求
    //商家要求每个商品每个月需要售卖出一定的销售总额
    //假设1号商品销售总额大于21000，2号商品销售总额大于10000，其余商品没有要求
    //请写出SQL从订单详情表中（order_detail）查询连续两个月销售总额大于等于任务总额的商品

    // 需要用到的表：
    //订单明细表：order_detail
    //order_detail_id(订单明细id)	order_id(订单id)	sku_id(商品id)	create_date(下单日期)	price(商品单价)	sku_num(商品件数)
    spark.createDataFrame(Seq(
      (1,	1,	1,	"2020-09-25",	2000.00,	2),
      (2,	2,	2,	"2020-09-30",	5000.00,	5),
      (3,	3,	1,	"2020-09-30",	6000.00,	3),
      (6,	6,	1,	"2020-10-02",	6000.00,	5),
      (4,	4,	2,	"2020-10-02",	500.00 ,  2),
      (5,	5,	1,	"2020-10-02",	2000.00,	5)
    ))
      .toDF("order_detail_id", "order_id", "sku_id", "create_date", "price", "sku_num")
      .createTempView("order_detail")
    spark.sql(
      s"""
         |select sku_id, count(*) as count
         |from(
         |  select sku_id, acc_month - row_number() over(partition by sku_id order by acc_month) as _month
         |  from(
         |    select sku_id, year(create_date) * 12 + month(create_date) as acc_month, sum(price * sku_num) as month_total_amount
         |    from order_detail
         |    group by sku_id, year(create_date), month(create_date)
         |  )
         |  where ((sku_id = 1 and month_total_amount > 21000) or (sku_id = 2 and month_total_amount > 10000))
         |)
         |group by sku_id, _month
         |having count >= 2
         |""".stripMargin)
      .show()

  }
  def example24(): Unit = {
    // 从订单详情表中（order_detail）对销售件数对商品进行分类，0-5000为冷门商品，5001-19999位一般商品，20000往上为热门商品，并求出不同类别商品的数量
    //结果如下：
    // category （类型）	cn （数量）
    // 一般商品	1
    // 冷门商品	10
    // 订单明细表：order_detail
    //
    //order_detail_id(订单明细id)	order_id(订单id)	sku_id(商品id)	create_date(下单日期)	price(商品单价)	sku_num(商品件数)
    //1	  1	  1	2021-09-30	2000.00	2000
    //2	  1	  2	2021-09-30	5000.00	2500
    //3	  10	3	2020-10-02	6000.00	10
    //4	  10	1	2020-10-02	500.00	5002
    //5	  10	2	2020-10-02	2000.00 2500
    spark.createDataFrame(Seq(
      (1, 1	, 1, "2021-09-30", 2000.00, 	19000),
      (2, 1	, 2, "2021-09-30", 5000.00, 	2500),
      (3, 10,	3, "2020-10-02", 6000.00, 	10),
      (4, 10,	1, "2020-10-02", 500.00	, 5002),
      (5, 10,	2, "2020-10-02", 2000.00,  2502)
    ))
      .toDF("order_detail_id", "order_id", "sku_id",  "create_date", "price", "sku_num")
      .createTempView("order_detail")

    spark.sql(
      s"""
         |select category, count(*) as count
         |from
         |(
         |  select sku_id,
         |         case
         |           when total_count <= 5000 then '冷门商品'
         |           when total_count >= 20000 then '热门商品'
         |           else '一般商品'
         |         end as category
         |  from(
         |    select sku_id, sum(sku_num) as total_count
         |    from order_detail
         |    group by sku_id
         |  )
         |)group by category
         |""".stripMargin)
      .show()
  }

  def example25(): Unit = {
    // 从订单详情表中（order_detail）和商品（sku_info）中查询各个品类销售数量前三的商品。如果该品类小于三个商品，则输出所有的商品销量。
    // order_detail_id(订单明细id)	order_id(订单id)	sku_id(商品id)	create_date(下单日期)	price(商品单价)	sku_num(商品件数)
    //1	1	1	2021-09-30	2000.00	2
    //2	1	3	2021-09-30	5000.00	5
    //22	10	4	2020-10-02	6000.00	1
    //23	10	5	2020-10-02	500.00	24
    //24	10	6	2020-10-02	2000.00	5

    //商品信息表：sku_info
    //sku_id(商品id)	name(商品名称)	category_id(分类id)	from_date(上架日期)	price(商品价格)
    //1	xiaomi 10	1	2020-01-01	2000
    //6	洗碗机	2	2020-02-01	2000
    //9	自行车	3	2020-01-01	1000
    spark.createDataFrame(Seq(
        (1, 1, 1, "2021-09-30", 2000.00, 19000),
        (2, 1, 2, "2021-09-30", 5000.00, 2500),
        (3, 10, 3, "2020-10-02", 6000.00, 10),
        (4, 10, 1, "2020-10-02", 500.00, 5002),
        (5, 10, 2, "2020-10-02", 2000.00, 2502)
      ))
      .toDF("order_detail_id", "order_id", "sku_id", "create_date", "price", "sku_num")
      .createTempView("order_detail")

    spark.createDataFrame(Seq(
        (1, "xiaomi", 10, "2020-01-01", 2000),
        (2, "洗碗机", 2, "2020-02-01", 2000),
        (3, "自行车", 3, "2020-01-01", 1000)
      )).toDF("sku_id", "name", "category_id", "from_date", "price")
      .createTempView("sku_info")

    spark.sql(
      s"""
         |select *
         |from(
         |  select sku_id, category_id, total_amount, rank() over(partition by sku_id order by total_amount desc) as rank_id
         |  from(
         |    select sku_id, category_id, sum(sku_num) as total_amount
         |    from(
         |      select t1.sku_id, category_id, sku_num
         |      from order_detail t1
         |      join sku_info t2
         |      on t1.sku_id = t2.sku_id
         |    )
         |    group by sku_id, category_id
         |  )
         |)
         |where rank_id <= 3
         |""".stripMargin)
      .show()
  }

  def example26(): Unit = {
    // 各品类中商品价格的中位数
    spark.createDataFrame(Seq(
        (1, 1, 1, "2021-09-30", 2000.00, 19000),
        (2, 1, 2, "2021-09-30", 5000.00, 2500),
        (3, 10, 3, "2020-10-02", 6000.00, 10),
        (4, 10, 1, "2020-10-02", 500.00, 5002),
        (5, 10, 2, "2020-10-02", 2000.00, 2502)
      ))
      .toDF("order_detail_id", "order_id", "sku_id", "create_date", "price", "sku_num")
      .createTempView("order_detail")

    spark.createDataFrame(Seq(
        (0, "xiaomi", 2, "2020-01-01", 3000),
        (1, "xiaomi", 2, "2020-01-01", 2000),
        (2, "洗碗机", 2, "2020-02-01", 2000),
        (3, "自行车", 3, "2020-01-01", 1000)
      )).toDF("sku_id", "name", "category_id", "from_date", "price")
      .createTempView("sku_info")
    spark.sql(
      s"""
         |select
         |	category_id,
         |    cast(price as decimal(16,2)) as medprice
         |from (
         |  select
         |    category_id,
         |    sku_id,
         |    price,
         |    row_number() over(partition by category_id order by price) as rn,
         |  	count(*) over(partition by category_id) as cnt
         |  from
         |      sku_info
         |) t1
         |where rn = ceil(cnt/2) or rn = ceil((cnt+1)/2)
         |""".stripMargin)
      .show()

    spark.sql(
      s"""
         |select category_id, percentile(price, 0.5) as ret
         |from sku_info
         |group by category_id
         |""".stripMargin)
      .show()

    // 看下效果
    spark.sql(
        s"""
           |select category_id, percentile(price, 0.5) over(partition by category_id order by price) as ret
           |from sku_info
           |""".stripMargin)
      .show()
  }

  def example27(): Unit = {
    // 找出销售额连续3天超过10000的商品
    spark.createDataFrame(Seq(
        (1, 1, 1, "2021-09-30", 2000.00, 6),
        (2, 1, 2, "2021-09-30", 5000.00, 5),
        (2, 1, 1, "2021-10-01", 5000.00, 5),
        (2, 1, 2, "2021-10-01", 5000.00, 1),
        (3, 10, 1, "2021-10-02", 6000.00, 10),

        (5, 10, 3, "2021-10-02", 2000.00, 10)
      ))
      .toDF("order_detail_id", "order_id", "sku_id", "create_date", "price", "sku_num")
      .createTempView("order_detail")

    spark.sql(
      s"""
         |select sku_id,
         |create_date,
         |         date_add(create_date, 2) as add_3_date,
         |         lead(create_date, 2) over(partition by sku_id order by create_date) as lead_3_date
         |  from(
         |      select create_date, sku_id, sum(price * sku_num) as total_amount
         |      from order_detail
         |      group by create_date, sku_id
         |      having total_amount > 10000
         |  )
         |""".stripMargin).show()
    spark.sql(
      s"""
         |select sku_id
         |from(
         |  select sku_id,
         |         date_add(create_date, 2) as add_3_date,
         |         lead(create_date, 2) over(partition by sku_id order by create_date) as lead_3_date
         |  from(
         |      select create_date, sku_id, sum(price * sku_num) as total_amount
         |      from order_detail
         |      group by create_date, sku_id
         |      having total_amount > 10000
         |  )
         |)
         |where add_3_date = lead_3_date
         |group by sku_id
         |""".stripMargin)
      .show()
  }

  def text(): Unit = {
    spark.createDataFrame(Seq(
      (1, "1"),
      (1, "1"),
      (6, "6")
    )).toDF("col1", "col2")
      .createTempView("test")


  }
  def main(args: Array[String]): Unit = {

    // example27()
  }
}
