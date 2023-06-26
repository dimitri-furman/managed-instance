/*
This script is intended to be executed on Azure SQL Database Managed Instance (General Purpose)
to determine if the IOPS/throughput seen against each database file in remote storage during script 
execution are near Azure Premium Storage limits for the blob corresponding to the file.

The script helps in determining if using larger files/blobs with higher limits
would be beneficial for improving workload performance.

NOTE: This script reports IOPS as they are measured by SQL Server. Azure Premium Storage measures 
them differently. For IOs up to 256 KB, both measurements match. For larger IOs, Azure Premium Storage 
breaks each IO into 256 KB chunks, and counts each chunk as an IO. Therefore, if SQL Server issues 
IOs larger than 256 KB, e.g. during backup/restore, then IOPS reported by this script will be lower 
than the IOPS measured by Azure Premium Storage. In this case, IOPS-based throttling could be 
occurring even if not reported in the script output.
*/

SET NOCOUNT ON;

BEGIN TRY

-- Begin parameters section

-- Change sampling loop duration to collect data over a representative time interval
DECLARE @LoopDurationSeconds int = 30;

-- Change the length of the interval between samplings of sys.dm_io_virtual_file_stats() for more or less granular sampling
DECLARE @IntervalLengthMilliseconds int = 1000;

-- End parameters section

IF @IntervalLengthMilliseconds < 100
    THROW 50001, 'The minimum supported sampling interval duration is 100 ms.', 1;

DECLARE @StartDateTime datetime2(2) = SYSDATETIME();
DECLARE @DelayInterval varchar(12) = DATEADD(millisecond, @IntervalLengthMilliseconds, CAST('00:00:00' AS time(3)));
DECLARE @VFSSample TABLE (
                         SampleMs bigint NOT NULL,
                         DatabaseID smallint NOT NULL,
                         FileID smallint NOT NULL,
                         TransferCount bigint NOT NULL,
                         ByteCount bigint NOT NULL,
                         PRIMARY KEY (SampleMs, DatabaseID, FileID)
                         );

-- Collect samples of virtual file stats for the specified duration
WHILE SYSDATETIME() < DATEADD(second, @LoopDurationSeconds, @StartDateTime)
BEGIN
    INSERT INTO @VFSSample
    (
    SampleMs,
    DatabaseID,
    FileID,
    TransferCount,
    ByteCount
    )
    SELECT vfs.sample_ms AS SampleMs,
           vfs.database_id AS DatabaseID,
           vfs.file_id AS FileID,
           vfs.num_of_reads + vfs.num_of_writes AS TransferCount,
           vfs.num_of_bytes_read + vfs.num_of_bytes_written AS ByteCount
    FROM sys.dm_io_virtual_file_stats(default, default) AS vfs
    WHERE vfs.database_id NOT IN (2,32760,32761,32762,32763) -- Exclude databases on local storage
    ;

    WAITFOR DELAY @DelayInterval;
END;

-- Return result set. 
-- Each row represents a database file, and includes max IOPS/throughput seen against the file, 
-- as well as counters showing how many times file IOPS/throughput were near Premium Storage limits during sampling loop execution.
WITH 
-- Define Azure Premium Storage limits (https://docs.microsoft.com/en-us/azure/virtual-machines/windows/premium-storage#premium-storage-disk-limits)
BlobLimit AS
(
SELECT 129 AS BlobSizeGB, 500 AS IOPSLimit, 100 AS ThroughputLimit
UNION
SELECT 513, 2300, 150
UNION
SELECT 1025, 5000, 200
UNION
SELECT 2049, 7500, 250
UNION
SELECT 4097, 7500, 250
UNION
SELECT 8192, 12500, 480
),
-- Calculate IOPS/throughput per file for each sampling interval,
-- by subtracting the cumulative stats of the previous sample 
-- from the cumulative stats of the next sample.
IntervalPerfMeasure AS
(
SELECT s.DatabaseID,
       s.FileID,
       s.SampleMs,
       (LEAD(s.TransferCount, 1) OVER (PARTITION BY s.DatabaseID, s.FileID ORDER BY s.SampleMs) - s.TransferCount)
       *
       (1000. / (LEAD(s.SampleMs, 1) OVER (PARTITION BY s.DatabaseID, s.FileID ORDER BY s.SampleMs) - s.SampleMs))
       AS IntervalIOPS,
       (
       (LEAD(s.ByteCount, 1) OVER (PARTITION BY s.DatabaseID, s.FileID ORDER BY s.SampleMs) - s.ByteCount)
       /
       ((LEAD(s.SampleMs, 1) OVER (PARTITION BY s.DatabaseID, s.FileID ORDER BY s.SampleMs) - s.SampleMs) * 0.001)
       )
       / 1024 / 1024 
       AS IntervalThroughput -- In MB/s
FROM @VFSSample AS s
),
-- Add columns for database name, file names, and file size
FilePerfMeasure AS
(
SELECT DB_NAME(mf.database_id) AS DatabaseName,
       mf.name AS FileLogicalName,
       mf.physical_name AS FilePhysicalName,
       CAST(mf.size * 8. / 1024 / 1024 AS decimal(12,4)) AS FileSizeGB,
       ipm.SampleMs,
       CAST(ipm.IntervalIOPS AS decimal(12,2)) AS IntervalIOPS,
       CAST(ipm.IntervalThroughput AS decimal(12,2)) AS IntervalThroughput
FROM IntervalPerfMeasure AS ipm
INNER JOIN sys.master_files AS mf
ON ipm.DatabaseID = mf.database_id
   AND
   ipm.FileID = mf.file_id
WHERE -- Remove rows without corresponding next sample
      ipm.IntervalIOPS IS NOT NULL
      AND
      ipm.IntervalThroughput IS NOT NULL
)
SELECT fpm.DatabaseName,
       fpm.FileLogicalName,
       fpm.FilePhysicalName,
       fpm.FileSizeGB,
       bl.BlobSizeGB,
       bl.IOPSLimit,
       MAX(fpm.IntervalIOPS) AS MaxIOPS,
       SUM(IIF(fpm.IntervalIOPS >= bl.IOPSLimit * 0.9, 1, 0)) AS IOPSNearLimitCount,
       bl.ThroughputLimit AS ThroughputLimitMBPS,
       MAX(fpm.IntervalThroughput) AS MaxThroughputMBPS,
       SUM(IIF(fpm.IntervalThroughput >= bl.ThroughputLimit * 0.9, 1, 0)) AS ThroughputNearLimitCount
FROM FilePerfMeasure AS fpm
CROSS APPLY (
            SELECT TOP (1) bl.BlobSizeGB,
                           bl.IOPSLimit,
                           bl.ThroughputLimit
            FROM BlobLimit AS bl
            WHERE bl.BlobSizeGB >= fpm.FileSizeGB
            ORDER BY bl.BlobSizeGB
            ) AS bl
GROUP BY fpm.DatabaseName,
         fpm.FileLogicalName,
         fpm.FilePhysicalName,
         fpm.FileSizeGB,
         bl.BlobSizeGB,
         bl.IOPSLimit,
         bl.ThroughputLimit
;

END TRY
BEGIN CATCH
    THROW;
END CATCH;
