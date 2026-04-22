package com.tech.service;

import com.tech.util.TimeUtil;
import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * table1 的 Parquet 导出与查询。
 * <ul>
 *   <li>{@code exportTable1*}：IoTDB → Parquet（本地或 MinIO）</li>
 *   <li>{@code queryTable1From*}：DuckDB 读取 Parquet（本地文件 / MinIO Hive 通配 / 指定对象 key）</li>
 * </ul>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ParquetService {

    private static final String IOTDB_JDBC_URL = "jdbc:iotdb://8.138.14.210:6667/database1?sql_dialect=table";
    private static final String IOTDB_USER = "root";
    private static final String IOTDB_PASSWORD = "root";
    private static final String MINIO_ENDPOINT = "http://127.0.0.1:9000";
    private static final String MINIO_ACCESS_KEY = "minioadmin";
    private static final String MINIO_SECRET_KEY = "minioadmin";
    private static final String DEFAULT_BUCKET = "parquet";
    private static final Path DIR;
    private static final long ONE_HOUR_MS = 3_600_000L;
    private static final long EAST8_OFFSET_MS = 8 * ONE_HOUR_MS;
    private final DataSource dataSource;

    /**
     * DuckDB 读 MinIO 上 table1 的 Hive 布局通配路径。
     */
    private static final String TABLE1_MINIO_HIVE_GLOB = "s3://%s/date=*/hour=*/*.parquet";

    private static final String TABLE1_AVRO_SCHEMA = """
            {
              "type": "record",
              "name": "table1",
              "fields": [
                {"name": "time",        "type": "long"},
                {"name": "device_id",   "type": ["null","string"], "default": null},
                {"name": "temperature", "type": ["null","double"], "default": null},
                {"name": "humidity",    "type": ["null","double"], "default": null}
              ]
            }
            """;
    private static final Schema TABLE1_SCHEMA = new Schema.Parser().parse(TABLE1_AVRO_SCHEMA);
    private static final int PARQUET_ZSTD_LEVEL = 12;
    private static final int PARQUET_PAGE_SIZE = 64 * 1024;
    private static final int PARQUET_DICT_PAGE_SIZE = 512 * 1024;
    private static final long PARQUET_ROW_GROUP_SIZE = 8L * 1024 * 1024;
    private static final long SYNTHETIC_PACKET_INTERVAL_MS = 500L;
    private static final long DEFAULT_SYNTHETIC_DURATION_MS = ONE_HOUR_MS;

    /**
     * {@code INSTALL httpfs} 写入本机 DuckDB 扩展目录，同一 JVM 内只需尝试一次（升级 DuckDB 版本后需重新拉取）。
     * {@code LOAD httpfs} 载入当前连接对应的数据库，每个新 Connection 仍要执行。
     */
    private static final Object HTTPFS_INSTALL_LOCK = new Object();
    private static volatile boolean httpfsJvmInstallAttempted;

    static {
        DIR = Path.of("./data/parquet");
        try {
            Files.createDirectories(DIR);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 将 IoTDB table1 导出为本地 Parquet（使用内置默认时间范围）。
     */
    public void exportTable1ToLocalParquet(long startTime, long endTime) {
        MillisRange range = MillisRange.ordered(startTime, endTime);
        int generated = exportTable1ToLocalPartitionFiles(range);
        log.info("本地分区 Parquet 导出完成: count={}, range=[{}, {}]", generated, startTime, endTime);
    }

    /**
     * 本地造数并同时导出为 Parquet 与普通二进制文件，再比较二者文件大小。
     * <p>
     * 造数规则：
     * <ul>
     *   <li>不查询 IoTDB</li>
     *   <li>0.5 秒一个包，时长由 {@code durationMs} 指定</li>
     * </ul>
     */
    public void exportTable1ToLocalBinaryAndCompare(long durationMs) {
        if (durationMs <= 0) {
            throw new IllegalArgumentException("durationMs must be > 0");
        }
        long alignedStart = floorToEast8Hour(System.currentTimeMillis());
        long syntheticEndExclusive = alignedStart + durationMs;
        MillisRange range = new MillisRange(alignedStart, syntheticEndExclusive);
        List<Table1Row> syntheticRows = buildSyntheticRows(alignedStart, durationMs);
        ExportSizeStats stats = exportRowsToLocalParquetAndBinary(range, syntheticRows);
        log.info(
                "本地造数参数: start={}, endExclusive={}, rows={}, intervalMs={}, durationMs={}",
                alignedStart,
                syntheticEndExclusive,
                syntheticRows.size(),
                SYNTHETIC_PACKET_INTERVAL_MS,
                durationMs
        );
        if (stats.binaryBytes() <= 0) {
            log.info(
                    "本地导出完成: parquetFiles={}, parquetBytes={}, binaryFiles={}, binaryBytes={}",
                    stats.parquetFileCount(),
                    stats.parquetBytes(),
                    stats.binaryFileCount(),
                    stats.binaryBytes()
            );
            return;
        }
        double parquetVsBinary = (double) stats.parquetBytes() / stats.binaryBytes();
        log.info(
                "本地导出完成(Parquet vs Binary): parquetFiles={}, parquetBytes={}, binaryFiles={}, binaryBytes={}, parquet/binary={}",
                stats.parquetFileCount(),
                stats.parquetBytes(),
                stats.binaryFileCount(),
                stats.binaryBytes(),
                parquetVsBinary
        );
    }

    public void exportTable1ToLocalBinaryAndCompare() {
        exportTable1ToLocalBinaryAndCompare(DEFAULT_SYNTHETIC_DURATION_MS);
    }

    public String exportTable1ToMinio(long startTime, long endTime) {
        return exportTable1ToMinio(startTime, endTime, DEFAULT_BUCKET);
    }

    /**
     * 将 IoTDB table1 导出为 Parquet 并上传到 MinIO（内存写入，不落本地文件）。
     */
    public String exportTable1ToMinio(long startTime, long endTime, String bucketName) {
        MillisRange range = MillisRange.ordered(startTime, endTime);
        try {
            MinioClient client = createMinioClient();
            ensureBucketExists(client, bucketName);
            int uploaded = exportTable1ToMinioPartitionFiles(client, bucketName, range);
            String prefixUrl = "%s/%s/".formatted(MINIO_ENDPOINT, bucketName);
            log.info("分区 Parquet 上传 MinIO 完成: count={}, prefix={}", uploaded, prefixUrl);
            return prefixUrl;
        } catch (Exception e) {
            log.error("Parquet 上传 MinIO 异常", e);
            return null;
        }
    }

    /**
     * 一次性查询范围内 table1 数据（避免按小时多次请求 IoTDB）。
     */
    private List<Table1Row> fetchTable1Rows(Connection conn, MillisRange range) throws SQLException {
        String sql = """
                    SELECT time, device_id, temperature, humidity
                    FROM table1
                    WHERE time >= %d AND time < %d
                    ORDER BY time
                """.formatted(range.lo(), range.hi());
        ArrayList<Table1Row> rows = new ArrayList<>();
        try (Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            stmt.setFetchSize(1000);
            try (ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    long time = rs.getLong("time");
                    String deviceId = rs.getString("device_id");

                    double temp = rs.getDouble("temperature");
                    Double temperature = rs.wasNull() ? null : temp;

                    double hum = rs.getDouble("humidity");
                    Double humidity = rs.wasNull() ? null : hum;

                    rows.add(new Table1Row(time, deviceId, temperature, humidity));
                }
            }
        }
        return rows;
    }

    private int writeRowsToParquet(OutputFile outputFile, List<Table1Row> rows) {
        if (rows.isEmpty()) {
            return 0;
        }
        Configuration conf = new Configuration(false);
        conf.setInt("parquet.compression.codec.zstd.level", PARQUET_ZSTD_LEVEL);
        try (ParquetWriter<GenericRecord> writer =
                     AvroParquetWriter.<GenericRecord>builder(outputFile)
                             .withSchema(TABLE1_SCHEMA)
                             .withConf(conf)
                             .withCompressionCodec(CompressionCodecName.ZSTD)
                             .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
                             .withDictionaryEncoding(true)
                             .withPageSize(PARQUET_PAGE_SIZE)
                             .withDictionaryPageSize(PARQUET_DICT_PAGE_SIZE)
                             .withRowGroupSize(PARQUET_ROW_GROUP_SIZE)
                             .build()) {
            for (Table1Row row : rows) {
                GenericRecord record = new GenericData.Record(TABLE1_SCHEMA);
                record.put("time", row.time());
                record.put("device_id", row.deviceId());
                record.put("temperature", row.temperature());
                record.put("humidity", row.humidity());
                writer.write(record);
            }
            return rows.size();
        } catch (Exception e) {
            log.error("Parquet 生成异常", e);
            return -1;
        }
    }

    private static MinioClient createMinioClient() {
        return MinioClient.builder()
                .endpoint(MINIO_ENDPOINT)
                .credentials(MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
                .build();
    }

    private static void ensureBucketExists(MinioClient minioClient, String bucketName) throws Exception {
        boolean exists = minioClient.bucketExists(
                BucketExistsArgs.builder().bucket(bucketName).build()
        );
        if (!exists) {
            minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
        }
    }

    private int exportTable1ToLocalPartitionFiles(MillisRange range) {
        int exported = 0;
        try (Connection conn = openIotDbConnection()) {
            List<Table1Row> allRows = fetchTable1Rows(conn, range);
            Map<Long, List<Table1Row>> rowsByHour = partitionRowsByEast8Hour(allRows);
            for (Map.Entry<Long, List<Table1Row>> entry : rowsByHour.entrySet()) {
                long hourStartMs = entry.getKey();
                long hourEndMs = hourStartMs + ONE_HOUR_MS;
                String shardStart = TimeUtil.toCompactTimestamp(hourStartMs);
                String shardEnd = TimeUtil.toCompactTimestamp(hourEndMs);
                String date = TimeUtil.hiveDateValue(hourStartMs);
                int hour = TimeUtil.hiveHourValue(hourStartMs);
                Path partitionDir = DIR.resolve(buildPartitionDirForTable1(date, hour));
                try {
                    Files.createDirectories(partitionDir);
                    Path parquetPath = partitionDir.resolve(buildTable1ParquetFilename(shardStart, shardEnd));
                    int rowCount = writeRowsToParquet(new LocalOutputFile(parquetPath), entry.getValue());
                    if (rowCount > 0) {
                        exported++;
                    } else if (rowCount == 0) {
                        Files.deleteIfExists(parquetPath);
                    }
                } catch (IOException e) {
                    log.error("创建本地分区目录失败: {}", partitionDir, e);
                }
            }
        } catch (Exception e) {
            log.error("本地分区 Parquet 导出异常", e);
        }
        return exported;
    }

    private ExportSizeStats exportRowsToLocalParquetAndBinary(MillisRange range, List<Table1Row> rows) {
        int parquetFileCount = 0;
        int binaryFileCount = 0;
        long parquetBytes = 0L;
        long binaryBytes = 0L;
        Map<Long, List<Table1Row>> rowsByHour = partitionRowsByEast8Hour(rows);
        for (Map.Entry<Long, List<Table1Row>> entry : rowsByHour.entrySet()) {
            long hourStartMs = entry.getKey();
            long hourEndMs = hourStartMs + ONE_HOUR_MS;
            String shardStart = TimeUtil.toCompactTimestamp(hourStartMs);
            String shardEnd = TimeUtil.toCompactTimestamp(hourEndMs);
            String date = TimeUtil.hiveDateValue(hourStartMs);
            int hour = TimeUtil.hiveHourValue(hourStartMs);
            Path partitionDir = DIR.resolve(buildPartitionDirForTable1(date, hour));
            List<Table1Row> bucketRows = entry.getValue();
            try {
                Files.createDirectories(partitionDir);
                Path parquetPath = partitionDir.resolve(buildTable1ParquetFilename(shardStart, shardEnd));
                int parquetRows = writeRowsToParquet(new LocalOutputFile(parquetPath), bucketRows);
                if (parquetRows > 0) {
                    parquetFileCount++;
                    parquetBytes += Files.size(parquetPath);
                } else if (parquetRows == 0) {
                    Files.deleteIfExists(parquetPath);
                }

                Path binaryPath = partitionDir.resolve(buildTable1BinaryFilename(shardStart, shardEnd));
                int binaryRows = writeRowsToBinary(binaryPath, bucketRows);
                if (binaryRows > 0) {
                    binaryFileCount++;
                    binaryBytes += Files.size(binaryPath);
                } else if (binaryRows == 0) {
                    Files.deleteIfExists(binaryPath);
                }
            } catch (IOException e) {
                log.error("创建本地分区目录失败: {}", partitionDir, e);
            }
        }
        log.debug("导出区间(用于目录命名/统计): [{} - {})", range.lo(), range.hi());
        return new ExportSizeStats(parquetFileCount, binaryFileCount, parquetBytes, binaryBytes);
    }

    private List<Table1Row> buildSyntheticRows(long startTime, long durationMs) {
        int packets = (int) (durationMs / SYNTHETIC_PACKET_INTERVAL_MS);
        ArrayList<Table1Row> rows = new ArrayList<>(packets);
        for (int i = 0; i < packets; i++) {
            long ts = startTime + i * SYNTHETIC_PACKET_INTERVAL_MS;
            String deviceIdPayload = "device-" + (i % 256);
            double temperature = 20.0 + ((i % 400) * 0.01);
            double humidity = 40.0 + ((i % 600) * 0.01);
            rows.add(new Table1Row(ts, deviceIdPayload, temperature, humidity));
        }
        return rows;
    }

    private int writeRowsToBinary(Path path, List<Table1Row> rows) {
        if (rows.isEmpty()) {
            return 0;
        }
        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(path)))) {
            for (Table1Row row : rows) {
                out.writeLong(row.time());
                byte[] deviceIdBytes = row.deviceId().getBytes();
                out.write(deviceIdBytes);
                out.writeDouble(row.temperature());
                out.writeDouble(row.humidity());
            }
            return rows.size();
        } catch (IOException e) {
            log.error("二进制文件生成异常: {}", path, e);
            return -1;
        }
    }

    private int exportTable1ToMinioPartitionFiles(MinioClient client, String bucketName, MillisRange range) {
        int uploaded = 0;
        try (Connection conn = openIotDbConnection()) {
            List<Table1Row> allRows = fetchTable1Rows(conn, range);
            Map<Long, List<Table1Row>> rowsByHour = partitionRowsByEast8Hour(allRows);
            for (Map.Entry<Long, List<Table1Row>> entry : rowsByHour.entrySet()) {
                long hourStartMs = entry.getKey();
                long hourEndMs = hourStartMs + ONE_HOUR_MS;
                String shardStart = TimeUtil.toCompactTimestamp(hourStartMs);
                String shardEnd = TimeUtil.toCompactTimestamp(hourEndMs);
                InMemoryOutputFile outputFile = new InMemoryOutputFile();
                int rowCount = writeRowsToParquet(outputFile, entry.getValue());
                if (rowCount <= 0) {
                    log.debug("分片无数据，跳过上传: [{} - {})", shardStart, shardEnd);
                    continue;
                }
                byte[] bytes = outputFile.toByteArray();
                String objectKey = buildMinioHiveObjectKeyForTable1(shardStart, shardEnd, hourStartMs);
                try (ByteArrayInputStream in = new ByteArrayInputStream(bytes)) {
                    client.putObject(
                            PutObjectArgs.builder()
                                    .bucket(bucketName)
                                    .object(objectKey)
                                    .contentType("application/octet-stream")
                                    .stream(in, bytes.length, -1)
                                    .build()
                    );
                    uploaded++;
                } catch (Exception e) {
                    log.error("上传分区 Parquet 到 MinIO 失败: {}", objectKey, e);
                }
            }
        } catch (Exception e) {
            log.error("分区 Parquet 上传前查询 IoTDB 异常", e);
        }
        return uploaded;
    }

    /**
     * 从本地目录读取 table1 Parquet（文件名由紧凑时间拼接）。
     */
    public void queryTable1FromLocalParquet(long startTime, long endTime) {
        MillisRange range = MillisRange.ordered(startTime, endTime);
        queryTable1FromLocalHivePartitions(range);
    }

    public void queryTable1FromMinioHive(long startTime, long endTime) {
        queryTable1FromMinioHive(startTime, endTime, DEFAULT_BUCKET);
    }

    /**
     * 从 MinIO Hive 分区路径通配读取，使用分区列 {@code date}/{@code hour} + {@code time} 过滤。
     */
    public void queryTable1FromMinioHive(long startTime, long endTime, String bucketName) {
        MillisRange range = MillisRange.ordered(startTime, endTime);
        queryTable1FromMinioHiveGlob(bucketName, range);
    }

    /**
     * 执行 MinIO Hive 查询的执行计划（EXPLAIN / EXPLAIN ANALYZE），用于确认是否触发分区裁剪。
     *
     * @return 执行计划文本行
     */
    public List<String> explainQueryTable1FromMinioHive(
            long startTime,
            long endTime,
            String bucketName,
            boolean analyze
    ) {
        MillisRange range = MillisRange.ordered(startTime, endTime);
        return explainMinioHiveQueryPlan(bucketName, range, analyze);
    }

    public List<String> explainQueryTable1FromMinioHive(long startTime, long endTime) {
        return explainQueryTable1FromMinioHive(startTime, endTime, DEFAULT_BUCKET, false);
    }

    private void queryTable1FromMinioHiveGlob(String bucketName, MillisRange range) {
        String s3Glob = TABLE1_MINIO_HIVE_GLOB.formatted(bucketName);
        String sql = buildMinioHiveTable1QuerySql("");

        try (Connection conn = openDuckDbWithMinioS3()) {
            log.info("Parquet 从 MinIO 直连读取 (hive glob): {}", s3Glob);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                bindMinioHiveTable1QueryParams(ps, s3Glob, range);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        logTable1Row(rs);
                    }
                }
            }
        } catch (Exception e) {
            log.error("从 MinIO 读取 Parquet 异常", e);
        }
    }

    private List<String> explainMinioHiveQueryPlan(String bucketName, MillisRange range, boolean analyze) {
        String s3Glob = TABLE1_MINIO_HIVE_GLOB.formatted(bucketName);
        String explainPrefix = analyze ? "EXPLAIN ANALYZE " : "EXPLAIN ";
        String sql = buildMinioHiveTable1QuerySql(explainPrefix);
        List<String> planLines = new ArrayList<>();
        try (Connection conn = openDuckDbWithMinioS3()) {
            try (Statement stmt = conn.createStatement()) {
                // 返回更完整的 explain 结构，便于观察是否发生 partition pruning。
                stmt.execute("SET explain_output = 'all'");
            }
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                bindMinioHiveTable1QueryParams(ps, s3Glob, range);
                try (ResultSet rs = ps.executeQuery()) {
                    int colCount = rs.getMetaData().getColumnCount();
                    while (rs.next()) {
                        String key = rs.getString(1);
                        String value = colCount >= 2 ? rs.getString(2) : null;
                        String line = value != null && !value.isBlank()
                                ? "%s:%n%s".formatted(key, value)
                                : key;
                        if (line != null && !line.isBlank()) {
                            planLines.add(line);
                            log.info("EXPLAIN: {}", line);
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("执行 MinIO Hive 查询 EXPLAIN 异常", e);
        }
        return planLines;
    }

    private static String buildMinioHiveTable1QuerySql(String prefix) {
        return """
                %sSELECT time, device_id, temperature, humidity
                FROM read_parquet(?, hive_partitioning = 1)
                WHERE "date" BETWEEN ? AND ?
                  AND time BETWEEN ? AND ?
                  AND ("date" > ? OR CAST("hour" AS INTEGER) >= ?)
                  AND ("date" < ? OR CAST("hour" AS INTEGER) <= ?)
                ORDER BY time
                """.formatted(prefix);
    }

    private static void bindMinioHiveTable1QueryParams(PreparedStatement ps, String s3Glob, MillisRange range) throws SQLException {
        String startDate = TimeUtil.hiveDateValue(range.lo());
        String endDate = TimeUtil.hiveDateValue(range.hi());
        int startHour = TimeUtil.hiveHourValue(range.lo());
        int endHour = TimeUtil.hiveHourValue(range.hi());
        ps.setString(1, s3Glob);
        ps.setString(2, startDate);
        ps.setString(3, endDate);
        ps.setLong(4, range.lo());
        ps.setLong(5, range.hi());
        ps.setString(6, startDate);
        ps.setInt(7, startHour);
        ps.setString(8, endDate);
        ps.setInt(9, endHour);
    }

    /**
     * MinIO 对象 key：{@code parquet/date=.../hour=.../table1_...parquet}
     */
    private static String buildMinioHiveObjectKeyForTable1(
            String exportStartCompact,
            String exportEndCompact,
            long exportStartEpochMs
    ) {
        String date = TimeUtil.hiveDateValue(exportStartEpochMs);
        int hour = TimeUtil.hiveHourValue(exportStartEpochMs);
        return new StringBuilder(96)
                .append("date=")
                .append(date)
                .append("/hour=")
                .append(hour)
                .append("/")
                .append(buildTable1ParquetFilename(exportStartCompact, exportEndCompact))
                .toString();
    }

    private static String buildPartitionDirForTable1(String date, int hour) {
        return new StringBuilder(32)
                .append("date=")
                .append(date)
                .append("/hour=")
                .append(hour)
                .toString();
    }

    private static String buildTable1ParquetFilename(String startCompact, String endCompact) {
        return new StringBuilder(48)
                .append("table1_")
                .append(startCompact)
                .append("_")
                .append(endCompact)
                .append(".parquet")
                .toString();
    }

    private static String buildTable1BinaryFilename(String startCompact, String endCompact) {
        return new StringBuilder(48)
                .append("table1_")
                .append(startCompact)
                .append("_")
                .append(endCompact)
                .append(".bin")
                .toString();
    }

    /**
     * 按东八区整点将数据分桶：同一桶的记录都属于同一 date/hour 分区。
     */
    private static Map<Long, List<Table1Row>> partitionRowsByEast8Hour(List<Table1Row> rows) {
        Map<Long, List<Table1Row>> buckets = new TreeMap<>();
        for (Table1Row row : rows) {
            long hourStart = floorToEast8Hour(row.time());
            buckets.computeIfAbsent(hourStart, ignored -> new ArrayList<>()).add(row);
        }
        return buckets;
    }

    private static long floorToEast8Hour(long epochMilli) {
        long shifted = epochMilli + EAST8_OFFSET_MS;
        long floored = (shifted / ONE_HOUR_MS) * ONE_HOUR_MS;
        return floored - EAST8_OFFSET_MS;
    }

    private static void logTable1Row(ResultSet rs) throws SQLException {
        long time = rs.getLong("time");
        String deviceId = rs.getString("device_id");
        double temp = rs.getDouble("temperature");
        double hum = rs.getDouble("humidity");
        log.info("time={}, device={}, temp={}, hum={}", time, deviceId, temp, hum);
    }

    private Connection openDuckDbWithMinioS3() throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        ensureHttpfsLoaded(conn);
        applyMinioS3Settings(conn);
        return conn;
    }

    private record MillisRange(long lo, long hi) {
        static MillisRange ordered(long a, long b) {
            return a <= b ? new MillisRange(a, b) : new MillisRange(b, a);
        }
    }

    private record Table1Row(long time, String deviceId, Double temperature, Double humidity) {
    }

    private record ExportSizeStats(int parquetFileCount, int binaryFileCount, long parquetBytes, long binaryBytes) {
    }

    /**
     * 每个连接执行 {@code LOAD httpfs}；仅在首次加载失败时对本 JVM 同步执行一次 {@code INSTALL httpfs}。
     * DuckDB 在某次 SQL 失败时可能关闭当前 {@link Statement}，失败后必须换新的 Statement。
     */
    private static void ensureHttpfsLoaded(Connection conn) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("LOAD httpfs");
            return;
        } catch (SQLException ignored) {
            // 扩展未装进本机目录或未载入当前库
        }
        synchronized (HTTPFS_INSTALL_LOCK) {
            if (!httpfsJvmInstallAttempted) {
                httpfsJvmInstallAttempted = true;
                try (Statement s = conn.createStatement()) {
                    try {
                        s.execute("INSTALL httpfs");
                    } catch (SQLException ignored) {
                        // 已安装或离线；扩展目录已有文件时后续 LOAD 仍可能成功
                    }
                }
            }
        }
        try (Statement s = conn.createStatement()) {
            s.execute("LOAD httpfs");
        }
    }

    /**
     * 将 {@link #MINIO_ENDPOINT} 解析为 DuckDB S3 设置（endpoint 不含协议，与 MinIO 文档一致）。
     */
    private static void applyMinioS3Settings(Connection conn) throws SQLException {
        URI uri = URI.create(MINIO_ENDPOINT);
        boolean useSsl = "https".equalsIgnoreCase(uri.getScheme());
        String host = uri.getHost();
        if (host == null) {
            throw new IllegalStateException("无效的 MinIO endpoint: " + MINIO_ENDPOINT);
        }
        int port = uri.getPort();
        String endpoint = port > 0 ? host + ":" + port : host;

        try (Statement stmt = conn.createStatement()) {
            stmt.execute("SET s3_region = 'us-east-1'");
            stmt.execute("SET s3_url_style = 'path'");
            stmt.execute("SET s3_endpoint = '%s'".formatted(sqlStringLiteral(endpoint)));
            stmt.execute("SET s3_access_key_id = '%s'".formatted(sqlStringLiteral(MINIO_ACCESS_KEY)));
            stmt.execute("SET s3_secret_access_key = '%s'".formatted(sqlStringLiteral(MINIO_SECRET_KEY)));
            stmt.execute(useSsl ? "SET s3_use_ssl = true" : "SET s3_use_ssl = false");
        }
    }

    private static String sqlStringLiteral(String s) {
        return s.replace("'", "''");
    }

    private Connection openIotDbConnection() throws SQLException {
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            // 回退到直连方式，便于在 DataSource 配置异常时快速定位问题。
            log.warn("通过 Spring DataSource 获取 IoTDB 连接失败，回退 DriverManager 直连: {}", e.getMessage());
            return DriverManager.getConnection(IOTDB_JDBC_URL, IOTDB_USER, IOTDB_PASSWORD);
        }
    }

    private void queryTable1FromLocalHivePartitions(MillisRange range) {
        String baseDir = DIR.toAbsolutePath().toString().replace("\\", "/");
        String localGlob = baseDir + "/date=*/hour=*/*.parquet";
        String startDate = TimeUtil.hiveDateValue(range.lo());
        String endDate = TimeUtil.hiveDateValue(range.hi());
        int startHour = TimeUtil.hiveHourValue(range.lo());
        int endHour = TimeUtil.hiveHourValue(range.hi());

        String sql = """
                    SELECT time, device_id, temperature, humidity
                    FROM read_parquet(?, hive_partitioning = 1)
                    WHERE "date" BETWEEN ? AND ?
                      AND time BETWEEN ? AND ?
                      AND ("date" > ? OR CAST("hour" AS INTEGER) >= ?)
                      AND ("date" < ? OR CAST("hour" AS INTEGER) <= ?)
                    LIMIT 10
                """;

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, localGlob);
            ps.setString(2, startDate);
            ps.setString(3, endDate);
            ps.setLong(4, range.lo());
            ps.setLong(5, range.hi());
            ps.setString(6, startDate);
            ps.setInt(7, startHour);
            ps.setString(8, endDate);
            ps.setInt(9, endHour);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    logTable1Row(rs);
                }
            }
        } catch (Exception e) {
            log.error("读取本地分区 Parquet 异常", e);
        }
    }

    /**
     * 内存版 Parquet {@link OutputFile}：顺序写入 {@link ByteArrayOutputStream}，供 {@link AvroParquetWriter} 使用。
     * Parquet 写入是顺序的（只追加、用 getPos 记录偏移，不回跳），所以无需真正可寻址的流。
     */
    private static final class InMemoryOutputFile implements OutputFile {
        private final ByteArrayOutputStream buffer = new ByteArrayOutputStream(64 * 1024);

        @Override
        public PositionOutputStream create(long blockSizeHint) {
            return newStream();
        }

        @Override
        public PositionOutputStream createOrOverwrite(long blockSizeHint) {
            buffer.reset();
            return newStream();
        }

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return 0;
        }

        byte[] toByteArray() {
            return buffer.toByteArray();
        }

        private PositionOutputStream newStream() {
            return new PositionOutputStream() {
                private long pos;

                @Override
                public long getPos() {
                    return pos;
                }

                @Override
                public void write(int b) {
                    buffer.write(b);
                    pos++;
                }

                @Override
                public void write(byte[] b, int off, int len) {
                    buffer.write(b, off, len);
                    pos += len;
                }

                @Override
                public void flush() {
                    // ByteArrayOutputStream is in-memory, no-op
                }
            };
        }
    }
}
