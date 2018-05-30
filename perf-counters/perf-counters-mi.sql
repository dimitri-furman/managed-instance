/*
DESCRIPTION:
This script provides SQL performance counter values based on data in sys.dm_os_performance_counters DMV.
It is intended to be used for Azure SQL Database Managed Instance performance monitoring and diagnostic data collection,
where the traditional methods of collecting PerfMon data are not available.
The script will execute for the specified number of iterations, which can be set to a large number for 
a quasi-indefinite capture similar to default PerfMon behavior. At the beginning of each iteration, a
snapshot of cumulative counters is taken, followed by a wait interval, and then by a second snapshot.
At that time, counter values are recorded in the dbo.perf_counter_log table, and a new iteration starts.

USAGE:
1. Set the @SnapshotIntervalSeconds and @MaxIterationCount parameters. Optionally, edit the script to define the counters to be collected.
2. Run the script.
3. Collected data will be found in dbo.perf_counter_log table in the current database.
*/

-- External parameters
DECLARE @SnapshotIntervalSeconds int = 10; -- The interval between first and second snapshot during each iteration
DECLARE @MaxIterationCount int = 100000; -- The number of iterations. Use a large number for indefinite capture duration.
DECLARE @SourceCounter table (
                             object_name nvarchar(128) not null,
                             counter_name nvarchar(128) not null,
                             instance_name nvarchar(128) not null,
                             base_counter_name nvarchar(128) null,
                             PRIMARY KEY (object_name, counter_name, instance_name)
                             ); -- The set of collected counters, to be defined below
-- Internal variables
DECLARE @Delay char(8);
DECLARE @IterationNumber int = 1;
DECLARE @FirstSnapshotCounter table (
                                    object_name nchar(128) not null,
                                    counter_name nchar(128) not null,
                                    instance_name nchar(128) not null,
                                    cntr_value bigint not null,
                                    base_cntr_value bigint null
                                    );
IF NOT (@SnapshotIntervalSeconds BETWEEN 1 AND 86399) -- 1 second to 23h 59m 59s
    THROW 50001, 'Snapshot interval duration is outside of supported range.', 1;
SET NOCOUNT ON;
-- Define the counters to be collected. Edit the statement below to add/remove counters as needed.
-- Two special cases for instance_name: 
-- 1. <guid> matches any instance name that starts with a GUID, which typically refers to a physical database name (physical_database_name in sys.databases)
-- 2. <* !_Total> matches any instance name other than "_Total"
INSERT INTO @SourceCounter (object_name, counter_name, instance_name, base_counter_name)
SELECT 'Access Methods' AS object_name, 'Forwarded Records/sec' AS counter_name, '' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Access Methods' AS object_name, 'Full Scans/sec' AS counter_name, '' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Access Methods' AS object_name, 'Page Splits/sec' AS counter_name, '' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Access Methods' AS object_name, 'Pages Allocated/sec' AS counter_name, '' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Access Methods' AS object_name, 'Table Lock Escalations/sec' AS counter_name, '' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=000000ms & <000001ms' AS counter_name, 'CPU Time:Requests' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=000000ms & <000001ms' AS counter_name, 'CPU Time:Total(ms)' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=000000ms & <000001ms' AS counter_name, 'Elapsed Time:Requests' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=000000ms & <000001ms' AS counter_name, 'Elapsed Time:Total(ms)' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=000001ms & <000002ms' AS counter_name, 'CPU Time:Requests' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=000001ms & <000002ms' AS counter_name, 'CPU Time:Total(ms)' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=000001ms & <000002ms' AS counter_name, 'Elapsed Time:Requests' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=000001ms & <000002ms' AS counter_name, 'Elapsed Time:Total(ms)' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=000002ms & <000005ms' AS counter_name, 'CPU Time:Requests' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=000002ms & <000005ms' AS counter_name, 'CPU Time:Total(ms)' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=000002ms & <000005ms' AS counter_name, 'Elapsed Time:Requests' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=000002ms & <000005ms' AS counter_name, 'Elapsed Time:Total(ms)' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=000005ms & <000010ms' AS counter_name, 'CPU Time:Requests' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=000005ms & <000010ms' AS counter_name, 'CPU Time:Total(ms)' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=000005ms & <000010ms' AS counter_name, 'Elapsed Time:Requests' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=000005ms & <000010ms' AS counter_name, 'Elapsed Time:Total(ms)' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=000010ms & <000020ms' AS counter_name, 'CPU Time:Requests' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=000010ms & <000020ms' AS counter_name, 'CPU Time:Total(ms)' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=000010ms & <000020ms' AS counter_name, 'Elappsed Time:Requests' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=000010ms & <000020ms' AS counter_name, 'Elapsed Time:Total(ms)' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=000020ms & <000050ms' AS counter_name, 'CPU Time:Requests' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=000020ms & <000050ms' AS counter_name, 'CPU Time:Total(ms)' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=000020ms & <000050ms' AS counter_name, 'Elapsed Time:Requests' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=000020ms & <000050ms' AS counter_name, 'Elapsed Time:Total(ms)' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=000050ms & <000100ms' AS counter_name, 'CPU Time:Requests' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=000050ms & <000100ms' AS counter_name, 'CPU Time:Total(ms)' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=000050ms & <000100ms' AS counter_name, 'Elapsed Time:Requests' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=000050ms & <000100ms' AS counter_name, 'Elapsed Time:Total(ms)' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=000100ms & <000200ms' AS counter_name, 'CPU Time:Requests' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=000100ms & <000200ms' AS counter_name, 'CPU Time:Total(ms)' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=000100ms & <000200ms' AS counter_name, 'Elapsed Time:Requests' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=000100ms & <000200ms' AS counter_name, 'Elapsed Time:Total(ms)' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=000200ms & <000500ms' AS counter_name, 'CPU Time:Requests' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=000200ms & <000500ms' AS counter_name, 'CPU Time:Total(ms)' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=000200ms & <000500ms' AS counter_name, 'Elapsed Time:Requests' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=000200ms & <000500ms' AS counter_name, 'Elapsed Time:Total(ms)' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=000500ms & <001000ms' AS counter_name, 'CPU Time:Requests' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=000500ms & <001000ms' AS counter_name, 'CPU Time:Total(ms)' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=000500ms & <001000ms' AS counter_name, 'Elapsed Time:Requests' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=000500ms & <001000ms' AS counter_name, 'Elapsed Time:Total(ms)' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=001000ms & <002000ms' AS counter_name, 'CPU Time:Requests' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=001000ms & <002000ms' AS counter_name, 'CPU Time:Total(ms)' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=001000ms & <002000ms' AS counter_name, 'Elapsed Time:Requests' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=001000ms & <002000ms' AS counter_name, 'Elapsed Time:Total(ms)' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=002000ms & <005000ms' AS counter_name, 'CPU Time:Requests' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=002000ms & <005000ms' AS counter_name, 'CPU Time:Total(ms)' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=002000ms & <005000ms' AS counter_name, 'Elapsed Time:Requests' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=002000ms & <005000ms' AS counter_name, 'Elapsed Time:Total(ms)' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=005000ms & <010000ms' AS counter_name, 'CPU Time:Requests' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=005000ms & <010000ms' AS counter_name, 'CPU Time:Total(ms)' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=005000ms & <010000ms' AS counter_name, 'Elapsed Time:Requests' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=005000ms & <010000ms' AS counter_name, 'Elapsed Time:Total(ms)' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=010000ms & <020000ms' AS counter_name, 'CPU Time:Requests' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=010000ms & <020000ms' AS counter_name, 'CPU Time:Total(ms)' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=010000ms & <020000ms' AS counter_name, 'Elapsed Time:Requests' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=010000ms & <020000ms' AS counter_name, 'Elapsed Time:Total(ms)' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=020000ms & <050000ms' AS counter_name, 'CPU Time:Requests' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=020000ms & <050000ms' AS counter_name, 'CPU Time:Total(ms)' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=020000ms & <050000ms' AS counter_name, 'Elapsed Time:Requests' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=020000ms & <050000ms' AS counter_name, 'Elapsed Time:Total(ms)' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=050000ms & <100000ms' AS counter_name, 'CPU Time:Requests' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=050000ms & <100000ms' AS counter_name, 'CPU Time:Total(ms)' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=050000ms & <100000ms' AS counter_name, 'Elapsed Time:Requests' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=050000ms & <100000ms' AS counter_name, 'Elapsed Time:Total(ms)' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=100000ms' AS counter_name, 'CPU Time:Requests' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=100000ms' AS counter_name, 'CPU Time:Total(ms)' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=100000ms' AS counter_name, 'Elapsed Time:Requests' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Batch Resp Statistics' AS object_name, 'Batches >=100000ms' AS counter_name, 'Elapsed Time:Total(ms)' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Buffer Manager' AS object_name, 'Background writer pages/sec' AS counter_name, '' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Buffer Manager' AS object_name, 'Buffer cache hit ratio' AS counter_name, '' AS instance_name, 'Buffer cache hit ratio base' AS base_counter_name UNION ALL
SELECT 'Buffer Manager' AS object_name, 'Buffer cache hit ratio base' AS counter_name, '' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Buffer Manager' AS object_name, 'Checkpoint pages/sec' AS counter_name, '' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Buffer Manager' AS object_name, 'Lazy writes/sec' AS counter_name, '' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Buffer Manager' AS object_name, 'Page life expectancy' AS counter_name, '' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Database Replica' AS object_name, 'File Bytes Received/sec' AS counter_name, '<guid>' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Database Replica' AS object_name, 'Log Bytes Received/sec' AS counter_name, '<guid>' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Database Replica' AS object_name, 'Log remaining for undo' AS counter_name, '<guid>' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Database Replica' AS object_name, 'Log Send Queue' AS counter_name, '<guid>' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Database Replica' AS object_name, 'Mirrored Write Transactions/sec' AS counter_name, '<guid>' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Database Replica' AS object_name, 'Recovery Queue' AS counter_name, '<guid>' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Database Replica' AS object_name, 'Redo blocked/sec' AS counter_name, '<guid>' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Database Replica' AS object_name, 'Redo Bytes Remaining' AS counter_name, '<guid>' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Database Replica' AS object_name, 'Redone Bytes/sec' AS counter_name, '<guid>' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Database Replica' AS object_name, 'Total Log requiring undo' AS counter_name, '<guid>' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Database Replica' AS object_name, 'Transaction Delay' AS counter_name, '<guid>' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Databases' AS object_name, 'Checkpoint duration' AS counter_name, '<guid>' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Databases' AS object_name, 'Checkpoint duration' AS counter_name, 'tempdb' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Databases' AS object_name, 'Group Commit Time/sec' AS counter_name, '<guid>' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Databases' AS object_name, 'Group Commit Time/sec' AS counter_name, 'tempdb' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Databases' AS object_name, 'Log Bytes Flushed/sec' AS counter_name, '<guid>' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Databases' AS object_name, 'Log Bytes Flushed/sec' AS counter_name, 'tempdb' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Databases' AS object_name, 'Log Flush Waits/sec' AS counter_name, '<guid>' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Databases' AS object_name, 'Log Flush Waits/sec' AS counter_name, 'tempdb' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Databases' AS object_name, 'Log Flushes/sec' AS counter_name, '<guid>' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Databases' AS object_name, 'Log Flushes/sec' AS counter_name, 'tempdb' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Databases' AS object_name, 'Log Growths' AS counter_name, '<guid>' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Databases' AS object_name, 'Log Growths' AS counter_name, 'tempdb' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Databases' AS object_name, 'Percent Log Used' AS counter_name, '<guid>' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Databases' AS object_name, 'Percent Log Used' AS counter_name, 'tempdb' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Databases' AS object_name, 'Transactions/sec' AS counter_name, '<guid>' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Databases' AS object_name, 'Transactions/sec' AS counter_name, 'tempdb' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Databases' AS object_name, 'Write Transactions/sec' AS counter_name, '<guid>' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Databases' AS object_name, 'Write Transactions/sec' AS counter_namme, 'tempdb' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'General Statistics' AS object_name, 'Active Temp Tables' AS counter_name, '' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'General Statistics' AS object_name, 'Logical Connections' AS counter_name, '' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'General Statistics' AS object_name, 'Logins/sec' AS counter_name, '' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'General Statistics' AS object_name, 'Logouts/sec' AS counter_name, '' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'General Statistics' AS object_name, 'Processes blocked' AS counter_name, '' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'General Statistics' AS object_name, 'User Connections' AS counter_name, '' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Locks' AS object_name, 'Average Wait Time (ms)' AS counter_name, '_Total' AS instance_name, 'Average Wait Time Base' AS base_counter_name UNION ALL
SELECT 'Locks' AS object_name, 'Average Wait Time Base' AS counter_name, '_Total' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Locks' AS object_name, 'Lock Timeouts (timeout > 0)/sec' AS counter_name, '_Total' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Locks' AS object_name, 'Number of Deadlocks/sec' AS counter_name, '_Total' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Memory Manager' AS object_name, 'Memory Grants Outstanding' AS counter_name, '' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Memory Manager' AS object_name, 'Memory Grants Pending' AS counter_name, '' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Memory Manager' AS object_name, 'SQL Cache Memory (KB)' AS counter_name, '' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Memory Manager' AS object_name, 'Stolen Server Memory (KB)' AS counter_name, '' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Memory Manager' AS object_name, 'Target Server Memory (KB)' AS counter_name, '' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Memory Manager' AS object_name, 'Total Server Memory (KB)' AS counter_name, '' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Plan Cache' AS object_name, 'Cache Hit Ratio' AS counter_name, '_Total' AS instance_name, 'Cache Hit Ratio Base' AS base_counter_name UNION ALL
SELECT 'Plan Cache' AS object_name, 'Cache Hit Ratio Base' AS counter_name, '_Total' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Plan Cache' AS object_name, 'Cache Object Counts' AS counter_name, '_Total' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Resource Pool Stats' AS object_name, 'Active memory grant amount (KB)' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Resource Pool Stats' AS object_name, 'Active memory grants count' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Resource Pool Stats' AS object_name, 'Avg Disk Read IO (ms)' AS counter_name, 'default' AS instance_name, 'Avg Disk Read IO (ms) Base' AS base_counter_name UNION ALL
SELECT 'Resource Pool Stats' AS object_name, 'Avg Disk Read IO (ms) Base' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Resource Pool Stats' AS object_name, 'Avg Disk Write IO (ms)' AS counter_name, 'default' AS instance_name, 'Avg Disk Write IO (ms) Base' AS base_counter_name UNION ALL
SELECT 'Resource Pool Stats' AS object_name, 'Avg Disk Write IO (ms) Base' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Resource Pool Stats' AS object_name, 'Cache memory target (KB)' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Resource Pool Stats' AS object_name, 'Compile memory target (KB)' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Resource Pool Stats' AS object_name, 'CPU control effect %' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Resource Pool Stats' AS object_name, 'CPU delayed %' AS counter_name, 'default' AS instance_name, 'CPU delayed % base' AS base_counter_name UNION ALL
SELECT 'Resource Pool Stats' AS object_name, 'CPU delayed % base' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Resource Pool Stats' AS object_name, 'CPU effective %' AS counter_name, 'default' AS instance_name, 'CPU effective % base' AS base_counter_name UNION ALL
SELECT 'Resource Pool Stats' AS object_name, 'CPU effective % base' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Resource Pool Stats' AS object_name, 'CPU usage %' AS counter_name, 'default' AS instance_name, 'CPU usage % base' AS base_counter_name UNION ALL
SELECT 'Resource Pool Stats' AS object_name, 'CPU usage % base' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Resource Pool Stats' AS object_name, 'CPU usage target %' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Resource Pool Stats' AS object_name, 'CPU violated %' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Resource Pool Stats' AS object_name, 'Disk Read Bytes/sec' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Resource Pool Stats' AS object_name, 'Disk Read IO Throttled/sec' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Resource Pool Stats' AS object_name, 'Disk Read IO/sec' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Resource Pool Stats' AS object_name, 'Disk Write Bytes/sec' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Resource Pool Stats' AS object_name, 'Disk Write IO Throttled/sec' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Resource Pool Stats' AS object_name, 'Disk Write IO/sec' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Resource Pool Stats' AS object_name, 'Max memory (KB)' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Resource Pool Stats' AS object_name, 'Memory grant timeouts/sec' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Resource Pool Stats' AS object_name, 'Memory grants/sec' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Resource Pool Stats' AS object_name, 'Pending memory grants count' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Resource Pool Stats' AS object_name, 'Query exec memory target (KB)' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Resource Pool Stats' AS object_name, 'Target memory (KB)' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Resource Pool Stats' AS object_name, 'Used memory (KB)' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'SQL Errors' AS object_name, 'Errors/sec' AS counter_name, 'DB Offline Errors' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'SQL Errors' AS object_name, 'Errors/sec' AS counter_name, 'Kill Connection Errors' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'SQL Errors' AS object_name, 'Errors/sec' AS counter_name, 'User Errors' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'SQL Statistics' AS object_name, 'Batch Requests/sec' AS counter_name, '' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'SQL Statistics' AS object_name, 'Failed Auto-Params/sec' AS counter_name, '' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'SQL Statistics' AS object_name, 'SQL Attention rate' AS counter_name, '' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'SQL Statistics' AS object_name, 'SQL Compilations/sec' AS counter_name, '' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'SQL Statistics' AS object_name, 'SQL Re-Compilations/sec' AS counter_name, '' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Transactions' AS object_name, 'Longest Transaction Running Time' AS counter_name, '' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Transactions' AS object_name, 'Version Cleanup rate (KB/s)' AS counter_name, '' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Transactions' AS object_name, 'Version Generation rate (KB/s)' AS counter_name, '' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Wait Statistics' AS object_name, 'Lock waits' AS counter_name, 'Cumulative wait time (ms) per second' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Wait Statistics' AS object_name, 'Memory grant queue waits' AS counter_name, 'Cumulative wait time (ms) per second' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Wait Statistics' AS object_name, 'Network IO waits' AS counter_name, 'Cumulative wait time (ms) per second' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Wait Statistics' AS object_name, 'Non-Page latch waits' AS counter_name, 'Cumulative wait time (ms) per second' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Wait Statistics' AS object_name, 'Page IO latch waits' AS counter_name, 'Cumulative wait time (ms) per second' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Wait Statistics' AS object_name, 'Page latch waits' AS counter_name, 'Cumulative wait time (ms) per second' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Wait Statistics' AS object_name, 'Wait for the worker' AS counter_name, 'Cumulative wait time (ms) per second' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Workload Group Stats' AS object_name, 'Active parallel threads' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Workload Group Stats' AS object_name, 'Active requests' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Workload Group Stats' AS object_name, 'Avg Disk msec/Read' AS counter_name, 'default' AS instance_name, 'Disk msec/Read Base' AS base_counter_name UNION ALL
SELECT 'Workload Group Stats' AS object_name, 'Avg Disk msec/Write' AS counter_name, 'default' AS instance_name, 'Disk msec/Write Base' AS base_counter_name UNION ALL
SELECT 'Workload Group Stats' AS object_name, 'Blocked tasks' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Workload Group Stats' AS object_name, 'CPU delayed %' AS counter_name, 'default' AS instance_name, 'CPU delayed % base' AS base_counter_name UNION ALL
SELECT 'Workload Group Stats' AS object_name, 'CPU delayed % base' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Workload Group Stats' AS object_name, 'CPU effective %' AS counter_name, 'default' AS instance_name, 'CPU effective % base' AS base_counter_name UNION ALL
SELECT 'Workload Group Stats' AS object_name, 'CPU effective % base' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Workload Group Stats' AS object_name, 'CPU usage %' AS counter_name, 'default' AS instance_name, 'CPU usage % base' AS base_counter_name UNION ALL
SELECT 'Workload Group Stats' AS object_name, 'CPU usage % base' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Workload Group Stats' AS object_name, 'CPU violated %' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Workload Group Stats' AS object_name, 'Disk Read Bytes/sec' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Workload Group Stats' AS object_name, 'Disk Reads/sec' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Workload Group Stats' AS object_name, 'Disk Violations/sec' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Workload Group Stats' AS object_name, 'Disk Write Bytes/sec' AS coounter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Workload Group Stats' AS object_name, 'Disk Writes/sec' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Workload Group Stats' AS object_name, 'Max request cpu time (ms)' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Workload Group Stats' AS object_name, 'Max request memory grant (KB)' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Workload Group Stats' AS object_name, 'Query optimizations/sec' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Workload Group Stats' AS object_name, 'Queued requests' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Workload Group Stats' AS object_name, 'Reduced memory grants/sec' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Workload Group Stats' AS object_name, 'Requests completed/sec' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Workload Group Stats' AS object_name, 'Suboptimal plans/sec' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Workload Group Stats' AS object_name, 'Disk msec/Read Base' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Workload Group Stats' AS object_name, 'Disk msec/Write Base' AS counter_name, 'default' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Availability Replica' AS object_name, 'Bytes Received from Replica/sec' AS counter_name, '<guid>' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'Availability Replica' AS object_name, 'Bytes Sent to Replica/sec' AS counter_name, '<guid>' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'HTTP Storage' AS object_name, 'Avg. Bytes/Read' AS counter_name, '<* !_Total>' AS instance_name, 'Avg. Bytes/Read BASE' AS base_counter_name UNION ALL
SELECT 'HTTP Storage' AS object_name, 'Avg. Bytes/Read BASE' AS counter_name, '<* !_Total>' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'HTTP Storage' AS object_name, 'Avg. Bytes/Transfer' AS counter_name, '<* !_Total>' AS instance_name, 'Avg. Bytes/Transfer BASE' AS base_counter_name UNION ALL
SELECT 'HTTP Storage' AS object_name, 'Avg. Bytes/Transfer BASE' AS counter_name, '<* !_Total>' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'HTTP Storage' AS object_name, 'Avg. Bytes/Write' AS counter_name, '<* !_Total>' AS instance_name, 'Avg. Bytes/Write BASE' AS base_counter_name UNION ALL
SELECT 'HTTP Storage' AS object_name, 'Avg. Bytes/Write BASE' AS counter_name, '<* !_Total>' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'HTTP Storage' AS object_name, 'Avg. microsec/Read' AS counter_name, '<* !_Total>' AS instance_name, 'Avg. microsec/Read BASE' AS base_counter_name UNION ALL
SELECT 'HTTP Storage' AS object_name, 'Avg. microsec/Read BASE' AS counter_name, '<* !_Total>' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'HTTP Storage' AS object_name, 'Avg. microsec/Read Comp' AS counter_name, '<* !_Total>' AS instance_name, 'Avg. microsec/Read Comp BASE' AS base_counter_name UNION ALL
SELECT 'HTTP Storage' AS object_name, 'Avg. microsec/Read Comp BASE' AS counter_name, '<* !_Total>' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'HTTP Storage' AS object_name, 'Avg. microsec/Transfer' AS counter_name, '<* !_Total>' AS instance_name, 'Avg. microsec/Transfer BASE' AS base_counter_name UNION ALL
SELECT 'HTTP Storage' AS object_name, 'Avg. microsec/Transfer BASE' AS counter_name, '<* !_Total>' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'HTTP Storage' AS object_name, 'Avg. microsec/Write' AS counter_name, '<* !_Total>' AS instance_name, 'Avg. microsec/Write BASE' AS base_counter_name UNION ALL
SELECT 'HTTP Storage' AS object_name, 'Avg. microsec/Write BASE' AS counter_name, '<* !_Total>' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'HTTP Storage' AS object_name, 'Avg. microsec/Write Comp' AS counter_name, '<* !_Total>' AS instance_name, 'Avg. microsec/Write Comp BASE' AS base_counter_name UNION ALL
SELECT 'HTTP Storage' AS object_name, 'Avg. microsec/Write Comp BASE' AS counter_name, '<* !_Total>' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'HTTP Storage' AS object_name, 'HTTP Storage IO failed/sec' AS counter_name, '<* !_Total>' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'HTTP Storage' AS object_name, 'HTTP Storage IO retry/sec' AS counter_name, '<* !_Total>' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'HTTP Storage' AS object_name, 'Outstanding HTTP Storage IO' AS counter_name, '<* !_Total>' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'HTTP Storage' AS object_name, 'Read Bytes/Sec' AS counter_name, '<* !_Total>' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'HTTP Storage' AS object_name, 'Reads/Sec' AS counter_name, '<* !_Total>' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'HTTP Storage' AS object_name, 'Total Bytes/Sec' AS counter_name, '<* !_Total>' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'HTTP Storage' AS object_name, 'Transfers/Sec' AS counter_name, '<* !_Total>' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'HTTP Storage' AS object_name, 'Write Bytes/Sec' AS counter_name, '<* !_Total>' AS instance_name, NULL AS base_counter_name UNION ALL
SELECT 'HTTP Storage' AS object_name, 'Writes/Sec' AS counter_name, '<* !_Total>' AS instance_name, NULL AS base_counter_name
;
IF NOT EXISTS (
              SELECT 1
              FROM sys.tables AS t
              WHERE t.name = 'perf_counter_log'
                    AND
                    SCHEMA_NAME(t.schema_id) = 'dbo'
              )
BEGIN
    CREATE TABLE dbo.perf_counter_log
    (
    iteration_number int NOT NULL,
    collection_time datetimeoffset NOT NULL CONSTRAINT df_perf_counter_log_collection_time DEFAULT (SYSDATETIMEOFFSET()),
    object_name nvarchar(128) NOT NULL,
    counter_name nvarchar(128) NOT NULL,
    instance_name nvarchar(128) NOT NULL,
    counter_value float NOT NULL,
    CONSTRAINT pk_perf_counter_log PRIMARY KEY (collection_time, object_name, counter_name, instance_name)
    );
END;
WHILE @IterationNumber <= @MaxIterationCount
BEGIN
    PRINT CAST(SYSDATETIMEOFFSET() AS nvarchar(40)) + ': Starting iteration ' + CAST(@IterationNumber AS varchar(11));
    -- Get the first snapshot of cumulative counters
    INSERT INTO @FirstSnapshotCounter (object_name, counter_name, instance_name, cntr_value, base_cntr_value)
    SELECT pc.object_name, pc.counter_name, pc.instance_name, pc.cntr_value, bc.cntr_value AS base_cntr_value
    FROM sys.dm_os_performance_counters AS pc
    INNER JOIN @SourceCounter AS sc
    ON UPPER(RTRIM(pc.object_name)) LIKE '%' + UPPER(sc.object_name)
       AND
       UPPER(RTRIM(pc.counter_name)) = UPPER(sc.counter_name)
       AND
       (
       UPPER(RTRIM(pc.instance_name)) = UPPER(sc.instance_name)
       OR
       (sc.instance_name = '<guid>' AND TRY_CONVERT(uniqueidentifier, pc.instance_name) IS NOT NULL)
       OR
       (sc.instance_name = '<* !_Total>' AND pc.instance_name <> '_Total')
       )
    OUTER APPLY (
                SELECT pc2.cntr_value
                FROM sys.dm_os_performance_counters AS pc2
                WHERE pc2.cntr_type = 1073939712
                      AND
                      pc2.object_name = pc.object_name
                      AND
                      pc2.instance_name = pc.instance_name
                      AND
                      UPPER(RTRIM(pc2.counter_name)) = UPPER(sc.base_counter_name)
                ) AS bc
    WHERE pc.cntr_type IN (272696576,1073874176)
          OR
          sc.object_name = 'Batch Resp Statistics'
    OPTION (RECOMPILE)
    ;
    -- Wait for specified interval
    SELECT @Delay = CONVERT(char(8), DATEADD(second, @SnapshotIntervalSeconds, 0), 114);
    WAITFOR DELAY @Delay;
    -- Get the second snapshot and record it in the log,
    -- using point-in-time counters as is, 
    -- and calculating the values for other counter types 
    -- based on the first snapshot and current base counter values.
    INSERT INTO dbo.perf_counter_log
    (
    iteration_number,
    object_name,
    counter_name,
    instance_name,
    counter_value
    )
    SELECT @IterationNumber AS iteration_number,
           pc.object_name,
           pc.counter_name,
           ISNULL(pc.instance_name, '') AS instance_name,
           ROUND(
                CASE WHEN sc.object_name = 'Batch Resp Statistics' THEN CAST((pc.cntr_value - fsc.cntr_value) AS float) -- Delta absolute
                     WHEN pc.cntr_type = 65792 THEN pc.cntr_value -- Point-in-time
                     WHEN pc.cntr_type = 272696576 THEN (pc.cntr_value - fsc.cntr_value) / CAST(@SnapshotIntervalSeconds AS float) -- Delta rate
                     WHEN pc.cntr_type = 537003264 THEN CAST(100 AS float) * pc.cntr_value / NULLIF(bc.cntr_value, 0) -- Ratio
                     WHEN pc.cntr_type = 1073874176 THEN ISNULL((pc.cntr_value - fsc.cntr_value) / NULLIF(bc.cntr_value - fsc.base_cntr_value, 0) / CAST(@SnapshotIntervalSeconds AS float), 0) -- Delta ratio
                END, 3)
                AS cntr_value
    FROM sys.dm_os_performance_counters AS pc
    INNER JOIN @SourceCounter AS sc
    ON UPPER(RTRIM(pc.object_name)) LIKE '%' + UPPER(sc.object_name)
       AND
       UPPER(RTRIM(pc.counter_name)) = UPPER(sc.counter_name)
       AND
       (
       UPPER(RTRIM(pc.instance_name)) = UPPER(sc.instance_name)
       OR
       (sc.instance_name = '<guid>' AND TRY_CONVERT(uniqueidentifier, pc.instance_name) IS NOT NULL)
       OR
       (sc.instance_name = '<* !_Total>' AND pc.instance_name <> '_Total')
       )
    OUTER APPLY (
                SELECT TOP (1) fsc.cntr_value,
                               fsc.base_cntr_value
                FROM @FirstSnapshotCounter AS fsc
                WHERE fsc.object_name = pc.object_name
                      AND
                      fsc.counter_name = pc.counter_name
                      AND
                      fsc.instance_name = pc.instance_name
                ) AS fsc
    OUTER APPLY (
                SELECT TOP (1) pc2.cntr_value
                FROM sys.dm_os_performance_counters AS pc2 
                WHERE pc2.cntr_type = 1073939712
                      AND
                      pc2.object_name = pc.object_name
                      AND
                      pc2.instance_name = pc.instance_name
                      AND
                      UPPER(RTRIM(pc2.counter_name)) = UPPER(sc.base_counter_name)
                ) AS bc
    WHERE -- Exclude base counters
          pc.cntr_type IN (65792,272696576,537003264,1073874176)
    OPTION (RECOMPILE)
    ;
    -- Reset for next iteration
    DELETE 
    FROM @FirstSnapshotCounter;
    SELECT @IterationNumber += 1;
END;
