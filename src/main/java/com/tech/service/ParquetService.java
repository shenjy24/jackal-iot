package com.tech.service;

import com.tech.util.TimeUtil;
import io.minio.*;
import io.minio.messages.Item;
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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    private static final String TABLE1_OBJECT_PREFIX = "table1_";
    /** 对应 {@link #writeParquet} 命名：{@code table1_{exportStart}_{exportEnd}.parquet}，数据区间 {@code [exportStart, exportEnd)}。 */
    private static final Pattern TABLE1_OBJECT_PATTERN = Pattern.compile("^table1_(\\d+)_(\\d+)\\.parquet$");

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
        long startTime = TimeUtil.getTimestamp("2025-08-20 00:00:00");
        long endTime = System.currentTimeMillis();

        String fileName = "table1_%d_%d.parquet".formatted(startTime, endTime);
        Path parquetPath = DIR.resolve(fileName);

        if (writeParquet(new LocalOutputFile(parquetPath), startTime, endTime)) {
            log.info("Parquet 生成完成: {}", parquetPath.toAbsolutePath());
        }
    }

    public String toParquetAndUploadToMinio(long startTime, long endTime) {
        return toParquetAndUploadToMinio(startTime, endTime, DEFAULT_BUCKET);
    }

    /**
     * 直接把 Parquet 写到内存缓冲再上传到 MinIO，不生成本地文件。
     */
    public String toParquetAndUploadToMinio(long startTime, long endTime, String bucketName) {
        String objectName = "table1_%d_%d.parquet".formatted(startTime, endTime);

        InMemoryOutputFile outputFile = new InMemoryOutputFile();
        if (!writeParquet(outputFile, startTime, endTime)) {
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

    /**
     * 从 IoTDB 查询 {@code [startTime, endTime)} 的 table1 数据写入指定 {@link OutputFile}。
     */
    private boolean writeParquet(OutputFile outputFile, long startTime, long endTime) {
        Schema schema = new Schema.Parser().parse(TABLE1_AVRO_SCHEMA);
        String sql = """
                    SELECT time, device_id, temperature, humidity
                    FROM table1
                    WHERE time >= ? AND time < ?
                    ORDER BY time
                """;

        try (Connection conn = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
             PreparedStatement ps = conn.prepareStatement(
                     sql,
                     ResultSet.TYPE_FORWARD_ONLY,
                     ResultSet.CONCUR_READ_ONLY
             );
             ParquetWriter<GenericRecord> writer =
                     AvroParquetWriter.<GenericRecord>builder(outputFile)
                             .withSchema(schema)
                             .withCompressionCodec(CompressionCodecName.ZSTD)
                             .build()
        ) {
            ps.setLong(1, startTime);
            ps.setLong(2, endTime);
            ps.setFetchSize(1000);
            try (ResultSet rs = ps.executeQuery()) {
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

    public void readParquet(long startTime, long endTime) {
        Path parquetPath = DIR.resolve("table1_%d_%d.parquet".formatted(startTime, endTime));
        readParquetFile(parquetPath, startTime, endTime);
    }

    public void readParquetFromMinio(long startTime, long endTime) {
        readParquetFromMinio(startTime, endTime, DEFAULT_BUCKET);
    }

    /**
     * 在 bucket 中列出所有符合 {@code table1_<start>_<end>.parquet} 的对象，
     * 以文件名编码的导出区间 {@code [exportStart, exportEnd)} 与查询区间 {@code [startTime, endTime]} 做重叠匹配，
     * 命中对象交给 DuckDB 一次读入后按 {@code time BETWEEN} 过滤。
     */
    public void readParquetFromMinio(long startTime, long endTime, String bucketName) {
        long lo = Math.min(startTime, endTime);
        long hi = Math.max(startTime, endTime);
        try {
            List<String> objects = listMinioObjectsOverlapping(bucketName, lo, hi);
            if (objects.isEmpty()) {
                log.warn("MinIO bucket [{}] 未找到与 [{}, {}] 重叠的 table1 Parquet 对象", bucketName, lo, hi);
                return;
            }
            log.info("MinIO 匹配到 {} 个对象: {}", objects.size(), objects);
            readParquetFromMinioObjects(bucketName, objects, lo, hi);
        } catch (Exception e) {
            log.error("按时间从 MinIO 解析 Parquet 失败", e);
        }
    }

    /**
     * 指定精确对象名读取（不做区间匹配，仅在 SQL 里做 {@code time BETWEEN}）。
     */
    public void readParquetFromMinio(String bucketName, String objectName, long startTime, long endTime) {
        long lo = Math.min(startTime, endTime);
        long hi = Math.max(startTime, endTime);
        readParquetFromMinioObjects(bucketName, List.of(objectName), lo, hi);
    }

    /**
     * 通过 DuckDB {@code httpfs} 以 S3 兼容协议直连 MinIO 读取一个或多个 Parquet（按需 Range 请求，不整文件落盘）。
     */
    private void readParquetFromMinioObjects(String bucketName, List<String> objectNames, long queryLo, long queryHi) {
        List<String> s3Uris = objectNames.stream()
                .map(name -> "s3://%s/%s".formatted(bucketName, name))
                .toList();
        String sql = buildSelectFromParquetSql(s3Uris);

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
            ensureHttpfsLoaded(conn);
            applyMinioS3Settings(conn);

            log.info("Parquet 从 MinIO 直连读取: {}", s3Uris);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                int p = 1;
                if (s3Uris.size() == 1) {
                    ps.setString(p++, s3Uris.getFirst());
                }
                ps.setLong(p++, queryLo);
                ps.setLong(p, queryHi);
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
     * 导出文件区间 {@code [fileStart, fileEnd)} 与查询闭区间 {@code [queryLo, queryHi]} 存在重叠。
     */
    private static boolean exportOverlapsQuery(long fileStart, long fileEnd, long queryLo, long queryHi) {
        return fileStart < fileEnd && fileStart <= queryHi && fileEnd > queryLo;
    }

    private static long[] parseTable1ObjectRange(String objectName) {
        Matcher m = TABLE1_OBJECT_PATTERN.matcher(objectName);
        if (!m.matches()) {
            return null;
        }
        try {
            return new long[]{Long.parseLong(m.group(1)), Long.parseLong(m.group(2))};
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private List<String> listMinioObjectsOverlapping(String bucket, long queryLo, long queryHi) throws Exception {
        List<String> names = new ArrayList<>();
        Iterable<Result<Item>> results = minioClient().listObjects(
                ListObjectsArgs.builder().bucket(bucket).prefix(TABLE1_OBJECT_PREFIX).build()
        );
        for (Result<Item> r : results) {
            Item item = r.get();
            if (item.isDir()) {
                continue;
            }
            String objectName = item.objectName();
            long[] range = parseTable1ObjectRange(objectName);
            if (range != null && exportOverlapsQuery(range[0], range[1], queryLo, queryHi)) {
                names.add(objectName);
            }
        }
        names.sort(Comparator.comparingLong(n -> {
            long[] r = parseTable1ObjectRange(n);
            return r != null ? r[0] : Long.MAX_VALUE;
        }));
        return names;
    }

    /**
     * 单文件时用 {@code read_parquet(?)} 占位符；多文件时 DuckDB 需字面量列表，时间条件仍用 {@code ?}。
     */
    private static String buildSelectFromParquetSql(List<String> s3Uris) {
        String from;
        if (s3Uris.size() == 1) {
            from = "read_parquet(?)";
        } else {
            from = readParquetSqlExpression(s3Uris);
        }
        return """
                SELECT time, device_id, temperature, humidity
                FROM %s
                WHERE time BETWEEN ? AND ?
                ORDER BY time
                """.formatted(from);
    }

    private static String readParquetSqlExpression(List<String> locations) {
        if (locations.size() == 1) {
            return "read_parquet('%s')".formatted(sqlStringLiteral(locations.getFirst()));
        }
        StringBuilder sb = new StringBuilder("read_parquet([");
        for (int i = 0; i < locations.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append("'").append(sqlStringLiteral(locations.get(i))).append("'");
        }
        sb.append("])");
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
