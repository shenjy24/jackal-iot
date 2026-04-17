package com.tech.util;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class TimeUtil {
    private static final DateTimeFormatter FORMAT1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static long getTimestamp(String time){
        LocalDateTime localDateTime = LocalDateTime.parse(time, FORMAT1);
        return localDateTime.toInstant(ZoneOffset.ofHours(8)).toEpochMilli();
    }
}
