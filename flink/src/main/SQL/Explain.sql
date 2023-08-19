-- 1. SQL 客户端的执行结果模式
SET 'sql-client.execution.result-mode' = 'table';
SET 'sql-client.execution.result-mode' = 'changelog';
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'sql-client.execution.result-mode' = 'sink';
SET 'sql-client.execution.result-mode' = 'retrieve';

-- 2. 空闲状态生存时间（TTL）
SET 'table.exec.state.ttl' = '1000s';  -- 单位有 ms, s, min, h, d

