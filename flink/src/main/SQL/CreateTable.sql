./bin/yarn-session.sh -s 2 -jm 1024 -tm 2048 -nm test1 -d
./bin/sql-client.sh embedded -s yarn-session

-- 1. 使用print创建数据表（作为sink）
CREATE TABLE SinkTable1 (
    age INT,
    num Int
)
WITH (
    'connector' = 'print'
);

-- 2. 使用内置的 datagen connector创建数据源表
CREATE TABLE SourceTable1 (
                     id STRING,
                     name STRING,
                     age INT
)
WITH (
    'connector' = 'datagen',
    'rows-per-second' = '5', -- 每秒生成的数据行数据
    'fields.id.length' = '5', --字段长度限制
    'fields.name.length'='3',
    'fields.age.min' ='1', -- 最小值
    'fields.age.max'='100' -- 最大值
);

-- 创建和Mysql连接的表

-- 创建和hive连接的表

-- 创建和kafka连接的表

-- 创建和Elasticsearch连接的表

-- 创建和HBase连接的表

-- 创建和本地文件连接的表
CREATE TABLE EventTable(
    user STRING,
    url STRING,
    `timestamp` BIGINT
)
WITH (
    'connector' = 'filesystem',
    'path' = 'events.csv',
    'format' = 'csv'
);

-- 创建和Hdfs文件连接的表,fs.defaultFS(namenode 地址)
CREATE TABLE SourceTable(
                           user STRING,
                           url STRING,
                           `timestamp` BIGINT
)
WITH (
    'connector' = 'filesystem',
    'path' = 'hdfs:///input/events.csv',
    'format' = 'csv'
);


-- 创建和



















