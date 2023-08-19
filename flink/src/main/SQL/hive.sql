create catalog myhive with ('type' = 'hive', 'hive-conf-dir' = '/opt/module/hive-3.1.2/conf/');


CREATE CATALOG myHiveCatalog WITH (
'type'='hive',
'default-database'='default',
'hive-conf-dir'='/opt/module/hive-3.1.2/conf/'
);

CREATE CATALOG myHiveCatalog WITH (
    'type'='hive',
    'hive-conf-dir'='/opt/module/hive-3.1.2/conf/',
    'hadoop-conf-dir' = '/opt/module/hadoop-3.1.3/etc/hadoop/'
);

CREATE TABLE datagen (
                         id STRING,
                         name STRING,
                         age INT
) WITH (
      'connector' = 'datagen',
      'rows-per-second' = '5', -- 每秒生成的数据行数据
      'fields.id.length' = '5', --字段长度限制
      'fields.name.length'='3',
      'fields.age.min' ='1', -- 最小值
      'fields.age.max'='100' -- 最大值
      );

CREATE TABLE print_show (
                               age INT,
                               num BIGINT,
                               PRIMARY KEY (age) NOT ENFORCED
) WITH (
      'connector' = 'print'
      );
CREATE TABLE ResultTable (
                             user STRING,
                             cnt BIGINT
                                 WITH (
                                 'connector' = 'print'
                                 );

CREATE TABLE age_num_mysql (
                               age INT,
                               num BIGINT,
                               PRIMARY KEY (age) NOT ENFORCED
) WITH (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://hadoop102:3306/big_data_example?useUnicode=true&characterEncoding=UTF-8',
      'table-name' = 'age_num', -- 需要手动到数据库中创建表
      'username' = 'root',
      'password' = '1234'
      );

insert into age_num_mysql select age,count(1) as num from datagen  group by age;
insert into print_show select age,count(1) as num from datagen  group by age;