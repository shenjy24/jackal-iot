package com.tech.repository.iotdb.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.tech.repository.iotdb.entity.Table1;
import com.tech.repository.iotdb.mapper.Table1Mapper;
import com.tech.repository.iotdb.service.Table1Service;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * <p>
 * table1 服务实现类
 * </p>
 *
 * @author IoTDB
 * @since 2026-04-17
 */
@Service
public class Table1ServiceImpl extends ServiceImpl<Table1Mapper, Table1> implements Table1Service {

    @Override
    public List<Table1> listBetweenTime(Long startTime, Long endTime) {
        LambdaQueryWrapper<Table1> queryWrapper = Wrappers.lambdaQuery();
        queryWrapper.ge(Table1::getTime, startTime)
                .le(Table1::getTime, endTime);
        return baseMapper.selectList(queryWrapper);
    }

    @Override
    public List<Table1> listByTime(Long time) {
        LambdaQueryWrapper<Table1> queryWrapper = Wrappers.lambdaQuery();
        queryWrapper.eq(Table1::getTime, time);
        return baseMapper.selectList(queryWrapper);
    }
}
