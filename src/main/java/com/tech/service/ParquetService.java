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
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

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
        generateParquetFile();
    }

    public String toParquetAndUploadToMinio() {
        return toParquetAndUploadToMinio(DEFAULT_BUCKET);
    }

    public String toParquetAndUploadToMinio(String bucketName) {
        Path parquetFile = generateParquetFile();
        if (parquetFile == null) {
            return null;
        }

        String objectName = parquetFile.getFileName().toString();

        try {
            MinioClient minioClient = MinioClient.builder()
                    .endpoint(MINIO_ENDPOINT)
                    .credentials(MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
                    .build();

            boolean bucketExists = minioClient.bucketExists(
                    BucketExistsArgs.builder().bucket(bucketName).build()
            );
            if (!bucketExists) {
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
            }

            minioClient.uploadObject(
                    UploadObjectArgs.builder()
                            .bucket(bucketName)
                            .object(objectName)
                            .filename(parquetFile.toAbsolutePath().toString())
                            .contentType("application/octet-stream")
                            .build()
            );

            String objectUrl = "%s/%s/%s".formatted(MINIO_ENDPOINT, bucketName, objectName);
            log.info("Parquet 上传 MinIO 完成: {}", objectUrl);
            return objectUrl;
        } catch (Exception e) {
            log.error("Parquet 上传 MinIO 异常", e);
            return null;
        }
    }

    private Path generateParquetFile() {
        Schema schema = new Schema.Parser().parse("""
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
                """);

        long startTime = TimeUtil.getTimestamp("2025-08-20 00:00:00");
        long endTime = System.currentTimeMillis();

        String sql = """
                    SELECT time, device_id, temperature, humidity
                    FROM table1
                    WHERE time >= %d AND time < %d
                    ORDER BY time
                """.formatted(startTime, endTime);

        String fileName = "table1_%d_%d.parquet".formatted(startTime, endTime);
        Path parquetPath = DIR.resolve(fileName);
        OutputFile outputFile = new LocalOutputFile(parquetPath);

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
            ResultSet rs = stmt.executeQuery(sql);

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

            log.info("Parquet 生成完成: {}", parquetPath.toAbsolutePath());
            return parquetPath;
        } catch (Exception e) {
            log.error("Parquet 生成异常", e);
            return null;
        }
    }

    public void readParquet(long startTime, long endTime) {
        Path parquetPath = DIR.resolve("table1_%d_%d.parquet".formatted(startTime, endTime));
        readParquetFile(parquetPath, startTime, endTime);
    }

    public void readParquetFromMinio(long startTime, long endTime) {
        readParquetFromMinio(startTime, endTime, DEFAULT_BUCKET);
    }

    public void readParquetFromMinio(long startTime, long endTime, String bucketName) {
        String objectName = "table1_%d_%d.parquet".formatted(startTime, endTime);
        readParquetFromMinio(bucketName, objectName, startTime, endTime);
    }

    /**
     * 通过 DuckDB {@code httpfs} 以 S3 兼容协议直连 MinIO 读取 Parquet（按需 Range 请求），不落盘整文件。
     */
    public void readParquetFromMinio(String bucketName, String objectName, long startTime, long endTime) {
        String s3Uri = "s3://%s/%s".formatted(bucketName, objectName);
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
            ensureHttpfsLoaded(conn);
            applyMinioS3Settings(conn);

            String escapedUri = sqlStringLiteral(s3Uri);
            String query = """
                        SELECT time, device_id, temperature, humidity
                        FROM read_parquet('%s')
                        WHERE time BETWEEN %d AND %d
                        LIMIT 10
                    """.formatted(escapedUri, startTime, endTime);

            log.info("Parquet 从 MinIO 直连读取: {}", s3Uri);
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(query)) {
                while (rs.next()) {
                    long time = rs.getLong("time");
                    String deviceId = rs.getString("device_id");
                    double temp = rs.getDouble("temperature");
                    double hum = rs.getDouble("humidity");

                    log.info("time={}, device={}, temp={}, hum={}", time, deviceId, temp, hum);
                }
            }
        } catch (Exception e) {
            log.error("从 MinIO 读取 Parquet 异常", e);
        }
    }

    /**
     * 每个连接执行 {@code LOAD httpfs}；仅在首次加载失败时对本 JVM 同步执行一次 {@code INSTALL httpfs}（见类字段说明）。
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
                        // 已安装、版本已存在或离线；扩展目录已有文件时后续 LOAD 仍可能成功
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
        String scheme = uri.getScheme();
        boolean useSsl = "https".equalsIgnoreCase(scheme);
        int port = uri.getPort();
        String host = uri.getHost();
        if (host == null) {
            throw new IllegalStateException("无效的 MinIO endpoint: " + MINIO_ENDPOINT);
        }
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

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("""
                        SELECT time, device_id, temperature, humidity
                        FROM read_parquet('%s')
                        WHERE time BETWEEN %d AND %d
                        LIMIT 10
                    """.formatted(filePath, startTime, endTime));

            while (rs.next()) {
                long time = rs.getLong("time");
                String deviceId = rs.getString("device_id");
                double temp = rs.getDouble("temperature");
                double hum = rs.getDouble("humidity");

                log.info("time={}, device={}, temp={}, hum={}", time, deviceId, temp, hum);
            }
        } catch (Exception e) {
            log.error("读取 Parquet 文件异常", e);
        }
    }
}
