WITH instance_resource_stats_agg /* resource usage snapshot aggregated across all resource pools */
AS
(
SELECT snapshot_time,
       duration_ms,
       instance_vcores,
       SUM(delta_cpu_usage_ms) AS delta_cpu_usage_ms,
       SUM(delta_log_bytes_used) AS delta_log_bytes_used,
       SUM(delta_read_io_completed) AS delta_read_io_completed,
       SUM(delta_write_io_completed) AS delta_write_io_completed,
       SUM(delta_read_bytes) AS delta_read_bytes,
       SUM(delta_write_bytes) AS delta_write_bytes,
       SUM(active_worker_count) AS active_worker_count,
       SUM(active_session_count) AS active_session_count,
       SUM(IIF(name = 'SloHkPool', used_memory_kb, 0)) AS xtp_used_memory_kb,
       SUM(IIF(name = 'SloHkPool', max_memory_kb, 0)) AS xtp_max_memory_kb
FROM sys.dm_resource_governor_resource_pools_history_ex
GROUP BY snapshot_time,
         /* assumption: duration_ms and instance_vcores is constant for every snapshot */
         duration_ms,
         instance_vcores
)
SELECT rs.snapshot_time,
       /* cap all metrics at 100% */
       LEAST(CAST(rs.delta_cpu_usage_ms * 1. / rs.duration_ms / rs.instance_vcores * 100 AS decimal(5, 2)), 100) AS avg_instance_cpu_percent,
       LEAST(CAST(rs.delta_log_bytes_used * 1. / rs.duration_ms * 1000 / irg.instance_max_log_rate * 100 as decimal(5, 2)), 100) AS avg_instance_log_write_percent,
       LEAST(CAST(rs.active_worker_count * 1. / irg.instance_max_worker_threads * 100 AS decimal(5,2)), 100) AS instance_worker_percent,
       LEAST(CAST(rs.active_session_count * 1. / 30000 * 100 AS decimal(5,2)), 100) AS instance_session_percent,
       LEAST(CAST(rs.xtp_used_memory_kb * 1. / rs.xtp_max_memory_kb * 100 AS decimal(5,2)), 100) AS xtp_memory_percent,
       /* Instance data IO cap is not defined on MI GP, thus we cannot calculate an unambiguous percentage. Return absolute IOPS/throughput values for reads and writes instead. */
       CAST(delta_read_io_completed * 1. / (rs.duration_ms / 1000.) AS decimal(12,2)) AS avg_read_iops,
       CAST(delta_write_io_completed * 1. / (rs.duration_ms / 1000.) AS decimal(12,2)) AS avg_write_iops,
       CAST(delta_read_bytes / 1024. / 1024 / (rs.duration_ms / 1000.) AS decimal(14,4)) AS avg_read_throughput_mbps,
       CAST(delta_write_bytes / 1024. / 1024 / (rs.duration_ms / 1000.) AS decimal(14,4)) AS avg_write_throughput_mbps
FROM instance_resource_stats_agg AS rs
CROSS JOIN sys.dm_instance_resource_governance AS irg
ORDER BY snapshot_time DESC;
