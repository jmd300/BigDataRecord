SET 'execution.runtime-mode' = 'streaming';
SET 'sql-client.execution.result-mode' = 'table';
SET 'table.exec.state.ttl' = '1000';

CREATE TABLE EventTable(
    url STRING,
    `user` STRING,
    `timestamp` BIGINT
) WITH (
    'connector' = 'filesystem',
    'path' = 'hdfs://hadoop102:8020/input/events.csv',
    'format' = 'csv'
);
CREATE TABLE ResultTable (
    `user` STRING,
    cnt BIGINT
) WITH (
   'connector' = 'print'
);

INSERT INTO ResultTable
SELECT `user`, COUNT(url) as cnt
FROM EventTable
GROUP BY `user`;