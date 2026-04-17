package com.tech.repository.iotdb.service;

import com.tech.repository.iotdb.entity.Table1;
import com.baomidou.mybatisplus.extension.service.IService;

import java.util.List;

/**
 * <p>
 * table1 服务类
 * </p>
 *
 * @author IoTDB
 * @since 2026-04-17
 */
public interface Table1Service extends IService<Table1> {

    List<Table1> listBetweenTime(Long startTime, Long endTime);

    List<Table1> listByTime(Long time);
}
