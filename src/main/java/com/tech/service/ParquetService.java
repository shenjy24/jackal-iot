package com.tech.service;

import com.tech.util.TimeUtil;
import io.minio.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;

@Slf4j
@Service
public class ParquetService {

    private static final String JDBC_URL = "jdbc:iotdb://8.138.14.210:6667/database1?sql_dialect=table";
    private static final String USER = "root";
    private static final String PASSWORD = "root";
    private static final String MINIO_ENDPOINT = "http://127.0.0.1:9000";
    private static final String MINIO_ACCESS_KEY = "minioadmin";
    private static final String MINIO_SECRET_KEY = "minioadmin";
    private static final String DEFAULT_BUCKET = "parquet";
    private static final Path DIR;

    /** MinIO Hive key: parquet/date=yyyy-MM-dd/hour=H/table1_{exportStart}_{exportEnd}.parquet */
    private static final String TABLE1_HIVE_GLOB = "s3://%s/parquet/date=*/hour=*/*.parquet";

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

    public void toParquet() {
        String startTime = TimeUtil.toCompactTimestamp(TimeUtil.getTimestamp("2025-08-20 00:00:00"));
        String endTime = TimeUtil.toCompactTimestamp(System.currentTimeMillis());
        long startEpochMilli = TimeUtil.parseCompactTimestamp(startTime);
        long endEpochMilli = TimeUtil.parseCompactTimestamp(endTime);

        String fileName = buildTable1FileName(startTime, endTime);
        Path parquetPath = DIR.resolve(fileName);

        if (writeParquet(new LocalOutputFile(parquetPath), startEpochMilli, endEpochMilli)) {
            log.info("Parquet 生成完成: {}", parquetPath.toAbsolutePath());
        }
    }

    public String toParquetAndUploadToMinio(String startTime, String endTime) {
        return toParquetAndUploadToMinio(startTime, endTime, DEFAULT_BUCKET);
    }

    public String toParquetAndUploadToMinio(long startTime, long endTime) {
        return toParquetAndUploadToMinio(
                TimeUtil.toCompactTimestamp(startTime),
                TimeUtil.toCompactTimestamp(endTime),
                DEFAULT_BUCKET
        );
    }

    /**
     * 直接把 Parquet 写到内存缓冲再上传到 MinIO，不生成本地文件。
     */
    public String toParquetAndUploadToMinio(String startTime, String endTime, String bucketName) {
        long startEpochMilli = TimeUtil.parseCompactTimestamp(startTime);
        long endEpochMilli = TimeUtil.parseCompactTimestamp(endTime);
        String objectName = table1HiveObjectKey(startTime, endTime, startEpochMilli);

        InMemoryOutputFile outputFile = new InMemoryOutputFile();
        if (!writeParquet(outputFile, startEpochMilli, endEpochMilli)) {
            return null;
        }
        byte[] bytes = outputFile.toByteArray();

        try {
            MinioClient minioClient = minioClient();
            ensureBucket(minioClient, bucketName);

            try (ByteArrayInputStream in = new ByteArrayInputStream(bytes)) {
                minioClient.putObject(
                        PutObjectArgs.builder()
                                .bucket(bucketName)
                                .object(objectName)
                                .contentType("application/octet-stream")
                                .stream(in, bytes.length, -1)
                                .build()
                );
            }

            String objectUrl = "%s/%s/%s".formatted(MINIO_ENDPOINT, bucketName, objectName);
            log.info("Parquet 上传 MinIO 完成 (size={} bytes): {}", bytes.length, objectUrl);
            return objectUrl;
        } catch (Exception e) {
            log.error("Parquet 上传 MinIO 异常", e);
            return null;
        }
    }

    public String toParquetAndUploadToMinio(long startTime, long endTime, String bucketName) {
        return toParquetAndUploadToMinio(
                TimeUtil.toCompactTimestamp(startTime),
                TimeUtil.toCompactTimestamp(endTime),
                bucketName
        );
    }

    /**
     * 从 IoTDB 查询 {@code [startTime, endTime)} 的 table1 数据写入指定 {@link OutputFile}。
     */
    private boolean writeParquet(OutputFile outputFile, long startTime, long endTime) {
        Schema schema = new Schema.Parser().parse(TABLE1_AVRO_SCHEMA);
        // IoTDB JDBC 2.0.6 不支持 prepareStatement；时间范围为 long，直接拼入 SQL。
        String sql = """
                    SELECT time, device_id, temperature, humidity
                    FROM table1
                    WHERE time >= %d AND time < %d
                    ORDER BY time
                """.formatted(startTime, endTime);

        try (Connection conn = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
             Statement stmt = conn.createStatement(
                     ResultSet.TYPE_FORWARD_ONLY,
                     ResultSet.CONCUR_READ_ONLY
             );
             ParquetWriter<GenericRecord> writer =
                     AvroParquetWriter.<GenericRecord>builder(outputFile)
                             .withSchema(schema)
                             .withCompressionCodec(CompressionCodecName.ZSTD)
                             .build()
        ) {
            stmt.setFetchSize(1000);
            try (ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    GenericRecord record = new GenericData.Record(schema);
                    record.put("time", rs.getLong("time"));
                    record.put("device_id", rs.getString("device_id"));

                    double temp = rs.getDouble("temperature");
                    record.put("temperature", rs.wasNull() ? null : temp);

                    double hum = rs.getDouble("humidity");
                    record.put("humidity", rs.wasNull() ? null : hum);

                    writer.write(record);
                }
            }
            return true;
        } catch (Exception e) {
            log.error("Parquet 生成异常", e);
            return false;
        }
    }

    private MinioClient minioClient() {
        return MinioClient.builder()
                .endpoint(MINIO_ENDPOINT)
                .credentials(MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
                .build();
    }

    private void ensureBucket(MinioClient minioClient, String bucketName) throws Exception {
        boolean exists = minioClient.bucketExists(
                BucketExistsArgs.builder().bucket(bucketName).build()
        );
        if (!exists) {
            minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
        }
    }

    public void readParquet(String startTime, String endTime) {
        long startEpochMilli = TimeUtil.parseCompactTimestamp(startTime);
        long endEpochMilli = TimeUtil.parseCompactTimestamp(endTime);
        Path parquetPath = DIR.resolve(buildTable1FileName(startTime, endTime));
        readParquetFile(parquetPath, startEpochMilli, endEpochMilli);
    }

    public void readParquet(long startTime, long endTime) {
        readParquet(TimeUtil.toCompactTimestamp(startTime), TimeUtil.toCompactTimestamp(endTime));
    }

    public void readParquetFromMinio(String startTime, String endTime) {
        readParquetFromMinio(startTime, endTime, DEFAULT_BUCKET);
    }

    public void readParquetFromMinio(long startTime, long endTime) {
        readParquetFromMinio(
                TimeUtil.toCompactTimestamp(startTime),
                TimeUtil.toCompactTimestamp(endTime),
                DEFAULT_BUCKET
        );
    }

    /**
     * 直接读取 Hive 分区目录并使用分区列 {@code date}/{@code hour} + 列值 {@code time} 过滤。
     * 不再先 list 对象，依赖 DuckDB 的 hive partition pruning。
     */
    public void readParquetFromMinio(String startTime, String endTime, String bucketName) {
        long lo = Math.min(TimeUtil.parseCompactTimestamp(startTime), TimeUtil.parseCompactTimestamp(endTime));
        long hi = Math.max(TimeUtil.parseCompactTimestamp(startTime), TimeUtil.parseCompactTimestamp(endTime));
        readParquetFromMinioByHivePartition(bucketName, lo, hi);
    }

    public void readParquetFromMinio(long startTime, long endTime, String bucketName) {
        readParquetFromMinio(
                TimeUtil.toCompactTimestamp(startTime),
                TimeUtil.toCompactTimestamp(endTime),
                bucketName
        );
    }

    /**
     * 指定精确对象名读取（不做区间匹配，仅在 SQL 里做 {@code time BETWEEN}）。
     */
    public void readParquetFromMinio(String bucketName, String objectName, String startTime, String endTime) {
        long lo = Math.min(TimeUtil.parseCompactTimestamp(startTime), TimeUtil.parseCompactTimestamp(endTime));
        long hi = Math.max(TimeUtil.parseCompactTimestamp(startTime), TimeUtil.parseCompactTimestamp(endTime));
        readParquetFromSingleObject(bucketName, objectName, lo, hi);
    }

    public void readParquetFromMinio(String bucketName, String objectName, long startTime, long endTime) {
        readParquetFromMinio(
                bucketName,
                objectName,
                TimeUtil.toCompactTimestamp(startTime),
                TimeUtil.toCompactTimestamp(endTime)
        );
    }

    /**
     * 按 Hive 分区路径读取（parquet/date=.../hour=...），并直接在 SQL 中按分区列做裁剪。
     */
    private void readParquetFromMinioByHivePartition(String bucketName, long queryLo, long queryHi) {
        String s3Glob = TABLE1_HIVE_GLOB.formatted(bucketName);
        String startDate = TimeUtil.hiveDateValue(queryLo);
        String endDate = TimeUtil.hiveDateValue(queryHi);
        int startHour = TimeUtil.hiveHourValue(queryLo);
        int endHour = TimeUtil.hiveHourValue(queryHi);
        String sql = """
                SELECT time, device_id, temperature, humidity
                FROM read_parquet(?, hive_partitioning = 1)
                WHERE "date" BETWEEN ? AND ?
                  AND time BETWEEN ? AND ?
                  AND ("date" > ? OR CAST("hour" AS INTEGER) >= ?)
                  AND ("date" < ? OR CAST("hour" AS INTEGER) <= ?)
                ORDER BY time
                """;

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
            ensureHttpfsLoaded(conn);
            applyMinioS3Settings(conn);

            log.info("Parquet 从 MinIO 直连读取 (hive glob): {}", s3Glob);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, s3Glob);
                ps.setString(2, startDate);
                ps.setString(3, endDate);
                ps.setLong(4, queryLo);
                ps.setLong(5, queryHi);
                ps.setString(6, startDate);
                ps.setInt(7, startHour);
                ps.setString(8, endDate);
                ps.setInt(9, endHour);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        long time = rs.getLong("time");
                        String deviceId = rs.getString("device_id");
                        double temp = rs.getDouble("temperature");
                        double hum = rs.getDouble("humidity");

                        log.info("time={}, device={}, temp={}, hum={}", time, deviceId, temp, hum);
                    }
                }
            }
        } catch (Exception e) {
            log.error("从 MinIO 读取 Parquet 异常", e);
        }
    }

    /**
     * 指定精确对象 key 读取（不依赖 hive 分区裁剪）。
     */
    private void readParquetFromSingleObject(String bucketName, String objectName, long queryLo, long queryHi) {
        String s3Uri = "s3://%s/%s".formatted(bucketName, objectName);
        String sql = """
                SELECT time, device_id, temperature, humidity
                FROM read_parquet(?, hive_partitioning = 1)
                WHERE time BETWEEN ? AND ?
                ORDER BY time
                """;
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
            ensureHttpfsLoaded(conn);
            applyMinioS3Settings(conn);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, s3Uri);
                ps.setLong(2, queryLo);
                ps.setLong(3, queryHi);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        long time = rs.getLong("time");
                        String deviceId = rs.getString("device_id");
                        double temp = rs.getDouble("temperature");
                        double hum = rs.getDouble("humidity");
                        log.info("time={}, device={}, temp={}, hum={}", time, deviceId, temp, hum);
                    }
                }
            }
        } catch (Exception e) {
            log.error("从 MinIO 读取单对象 Parquet 异常", e);
        }
    }

    private static String table1HiveObjectKey(String exportStart, String exportEnd, long exportStartEpochMilli) {
        String date = TimeUtil.hiveDateValue(exportStartEpochMilli);
        int hour = TimeUtil.hiveHourValue(exportStartEpochMilli);
        StringBuilder sb = new StringBuilder(96);
        sb.append("parquet/date=")
                .append(date)
                .append("/hour=")
                .append(hour)
                .append("/")
                .append(buildTable1FileName(exportStart, exportEnd));
        return sb.toString();
    }

    private static String buildTable1FileName(String startTime, String endTime) {
        StringBuilder sb = new StringBuilder(48);
        sb.append("table1_")
                .append(startTime)
                .append("_")
                .append(endTime)
                .append(".parquet");
        return sb.toString();
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

    private void readParquetFile(Path parquetPath, long startTime, long endTime) {
        String filePath = parquetPath.toAbsolutePath().toString().replace("\\", "/");
        long lo = Math.min(startTime, endTime);
        long hi = Math.max(startTime, endTime);

        String sql = """
                    SELECT time, device_id, temperature, humidity
                    FROM read_parquet(?)
                    WHERE time BETWEEN ? AND ?
                    LIMIT 10
                """;

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, filePath);
            ps.setLong(2, lo);
            ps.setLong(3, hi);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long time = rs.getLong("time");
                    String deviceId = rs.getString("device_id");
                    double temp = rs.getDouble("temperature");
                    double hum = rs.getDouble("humidity");

                    log.info("time={}, device={}, temp={}, hum={}", time, deviceId, temp, hum);
                }
            }
        } catch (Exception e) {
            log.error("读取 Parquet 文件异常", e);
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
