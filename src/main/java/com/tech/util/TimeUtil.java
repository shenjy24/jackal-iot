package com.tech.util;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class TimeUtil {
    private static final ZoneId ZONE_EAST_8 = ZoneId.of("Asia/Shanghai");
    private static final DateTimeFormatter FORMAT_DASHED = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    /** 24 小时制；{@code hh} 为 12 小时制且需上午/下午，无则解析失败。 */
    private static final DateTimeFormatter FORMAT_COMPACT = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS");
    private static final DateTimeFormatter FORMAT_HIVE_DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private TimeUtil() {
    }

    public static long getTimestamp(String time) {
        LocalDateTime localDateTime = LocalDateTime.parse(time, FORMAT_DASHED);
        return localDateTime.atZone(ZONE_EAST_8).toInstant().toEpochMilli();
    }

    public static String toCompactTimestamp(long epochMilli) {
        return FORMAT_COMPACT.format(instantAtEast8(epochMilli));
    }

    public static long parseCompactTimestamp(String timestamp) {
        LocalDateTime localDateTime = LocalDateTime.parse(timestamp, FORMAT_COMPACT);
        return localDateTime.atZone(ZONE_EAST_8).toInstant().toEpochMilli();
    }

    public static String hiveDateValue(long epochMilli) {
        return FORMAT_HIVE_DATE.format(instantAtEast8(epochMilli));
    }

    public static int hiveHourValue(long epochMilli) {
        return instantAtEast8(epochMilli).getHour();
    }

    private static ZonedDateTime instantAtEast8(long epochMilli) {
        return ZonedDateTime.ofInstant(java.time.Instant.ofEpochMilli(epochMilli), ZONE_EAST_8);
    }
}
