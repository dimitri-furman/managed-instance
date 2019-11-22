/*
On Azure SQL Database Managed Instance, the sys.server_resource_stats view is populated 
via a monitoring pipeline that has typical data latency in a single minutes range. Therefore, 
data in this view often appears delayed. 

As a workaround, the dbo.server_resource_stats view defined below can be used to obtain resource 
stats in real time as they are emitted by the instance using the managed_instance_resource_stats
extended event.

The script creates an event session that starts automatically on instance startup and collects
the managed_instance_resource_stats event in a memory ring buffer. Each event is represented by an
XML node in the ring buffer data. The view shreds and parses XML to provide a relational rowset view
of the ring buffer, and then unions this set with the existing sys.server_resource_stats view to 
provide both current and historical resource stats.
*/

USE master;

IF NOT EXISTS (
              SELECT 1
              FROM sys.dm_xe_sessions
              WHERE name = 'current_resource_stats'
              )
BEGIN
    -- Create an extended events session capturing resource stats events in a ring buffer
    CREATE EVENT SESSION current_resource_stats ON SERVER
    ADD EVENT sqlazure_min.managed_instance_resource_stats
    ADD TARGET package0.ring_buffer(SET max_events_limit=50) -- 50 events cover the last 125 minutes. Can be increased in case of very high data latency in sys.server_resource_stats.
    WITH (EVENT_RETENTION_MODE=ALLOW_SINGLE_EVENT_LOSS,MAX_DISPATCH_LATENCY=10 SECONDS,STARTUP_STATE=ON);

    ALTER EVENT SESSION current_resource_stats ON SERVER STATE = START;
END;
GO

CREATE OR ALTER VIEW dbo.server_resource_stats
AS

WITH
RingBuffer AS
(
SELECT CAST(xst.target_data AS xml) AS TargetData
FROM sys.dm_xe_session_targets AS xst
INNER JOIN sys.dm_xe_sessions AS xs
ON xst.event_session_address = xs.address
WHERE xs.name = N'current_resource_stats'
),
EventNode AS
(
SELECT CAST(NodeData.query('.') AS xml) AS EventInfo
FROM RingBuffer AS rb
CROSS APPLY rb.TargetData.nodes('/RingBufferTarget/event') AS n(NodeData)
)
SELECT EventInfo.value('(//event/data[@name="start_time"]/value)[1]','datetime') AS start_time,
       EventInfo.value('(//event/data[@name="end_time"]/value)[1]','datetime') AS end_time,
       N'SQL managed instance' AS resource_type,
       EventInfo.value('(//event/data[@name="server_name"]/value)[1]','nvarchar(256)') AS resource_name,
       EventInfo.value('(//event/data[@name="sku"]/value)[1]','nvarchar(256)') AS sku,
       EventInfo.value('(//event/data[@name="hardware_generation"]/value)[1]','nvarchar(256)') AS hardware_generation,
       EventInfo.value('(//event/data[@name="virtual_core_count"]/value)[1]','int') AS virtual_core_count,
       EventInfo.value('(//event/data[@name="avg_cpu_percent"]/value)[1]','decimal(5,2)') AS avg_cpu_percent,
       EventInfo.value('(//event/data[@name="reserved_storage_mb"]/value)[1]','bigint') AS reserved_storage_mb,
       EventInfo.value('(//event/data[@name="storage_space_used_mb"]/value)[1]','decimal(18,2)') AS storage_space_used_mb,
       EventInfo.value('(//event/data[@name="io_requests"]/value)[1]','bigint') AS io_requests,
       EventInfo.value('(//event/data[@name="io_bytes_read"]/value)[1]','bigint') AS io_bytes_read,
       EventInfo.value('(//event/data[@name="io_bytes_written"]/value)[1]','bigint') AS io_bytes_written
FROM EventNode
UNION
SELECT start_time,
       end_time,
       resource_type,
       resource_name,
       sku,
       hardware_generation,
       virtual_core_count,
       avg_cpu_percent,
       reserved_storage_mb,
       storage_space_used_mb,
       io_requests,
       io_bytes_read,
       io_bytes_written
FROM master.sys.server_resource_stats;
GO

SELECT *
FROM dbo.server_resource_stats
ORDER BY start_time DESC;