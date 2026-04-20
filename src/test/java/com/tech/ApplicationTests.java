package com.tech;

import com.tech.repository.iotdb.entity.Table1;
import com.tech.repository.iotdb.service.Table1Service;
import com.tech.service.ParquetService;
import com.tech.util.TimeUtil;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

@Slf4j
@SpringBootTest
class ApplicationTests {

    @Autowired
    private Table1Service table1Service;

    @Autowired
    private ParquetService parquetService;

    @Test
    void testList() {
        List<Table1> table1s = table1Service.list();
        log.info("table1s: {}", table1s);
    }

    @Test
    void testBetweenTime() {
        String startTime = "2025-07-20 00:00:00";
        String endTime = "2027-04-20 00:00:00";
        List<Table1> table1s = table1Service.listBetweenTime(TimeUtil.getTimestamp(startTime), TimeUtil.getTimestamp(endTime));
        log.info("table1s: {}", table1s);
    }

    @Test
    void testSave() {
        Table1 table1 = new Table1();
        table1.setTime(TimeUtil.getTimestamp("2026-04-20 13:50:55"));
        table1.setRegion("Hamburg");
        table1.setPlantId("1003");
        table1.setDeviceId("100");
        table1.setTemperature(80.0);
        table1.setHumidity(40.5);
        table1.setStatus(true);
        table1.setArrivalTime(System.currentTimeMillis());
        if (table1Service.save(table1)) {
            log.info("table1: {}", table1);
        }
    }

    @Test
    void testUpdate() {
        List<Table1> table1s = table1Service.listByTime(1776394997739L);
        for (Table1 table1 : table1s) {
            table1.setStatus(false);
        }
        table1Service.saveBatch(table1s);
    }

    @Test
    void testToParquet() {
        long startTime = TimeUtil.getTimestamp("2025-07-20 00:00:00");
        long endTime = TimeUtil.getTimestamp("2027-04-20 00:00:00");
        parquetService.exportTable1ToLocalParquet(startTime, endTime);
    }

    @Test
    void testReadParquet() {
        long startTime = TimeUtil.getTimestamp("2025-07-20 00:00:00");
        long endTime = TimeUtil.getTimestamp("2027-04-20 00:00:00");
        parquetService.queryTable1FromLocalParquet(startTime, endTime);
    }

    @Test
    void testToMinio() {
        long startTime = TimeUtil.getTimestamp("2026-04-20 00:00:00");
        long endTime = TimeUtil.getTimestamp("2026-04-21 00:00:00");
        String url = parquetService.exportTable1ToMinio(startTime, endTime);
        log.info("url: {}", url);
    }

    @Test
    void testReadMinio() {
        long startTime = TimeUtil.getTimestamp("2026-04-20 14:00:00");
        long endTime = TimeUtil.getTimestamp("2026-04-20 14:20:00");
        parquetService.queryTable1FromMinioHive(startTime, endTime);
    }

    @Test
    void testExplain() {
        long startTime = TimeUtil.getTimestamp("2026-04-20 14:00:00");
        long endTime = TimeUtil.getTimestamp("2026-04-20 14:20:00");
        parquetService.explainQueryTable1FromMinioHive(startTime, endTime);
    }
}
