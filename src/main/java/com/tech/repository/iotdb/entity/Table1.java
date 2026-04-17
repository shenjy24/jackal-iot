package com.tech.repository.iotdb.entity;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

/**
 * <p>
 * table1
 * </p>
 *
 * @author IoTDB
 * @since 2026-04-17
 */
@Getter
@Setter
@ToString
public class Table1 implements Serializable {

    private static final long serialVersionUID = 1L;

    private Long time;

    private String region;

    private String plantId;

    private String deviceId;

    private String modelId;

    /**
     * maintenance
     */
    private String maintenance;

    /**
     * temperature
     */
    private Double temperature;

    /**
     * humidity
     */
    private Double humidity;

    /**
     * status
     */
    private Boolean status;

    /**
     * arrival_time
     */
    private Long arrivalTime;
}
