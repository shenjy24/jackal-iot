package com.tech.util;


import com.baomidou.mybatisplus.generator.FastAutoGenerator;
import com.baomidou.mybatisplus.generator.config.DataSourceConfig;
import com.baomidou.mybatisplus.generator.config.OutputFile;
import com.baomidou.mybatisplus.generator.config.rules.DateType;
import com.baomidou.mybatisplus.generator.config.rules.DbColumnType;
import org.apache.iotdb.jdbc.IoTDBDataSource;

import java.sql.Types;
import java.util.Collections;

public class MyBatisPlusGenerator {
    public static void main(String[] args) {
        IoTDBDataSource dataSource = new IoTDBDataSource();
        dataSource.setUrl("jdbc:iotdb://8.138.14.210:6667/database1?sql_dialect=table");
        dataSource.setUser("root");
        dataSource.setPassword("root");
        FastAutoGenerator generator = FastAutoGenerator.create(new DataSourceConfig.Builder(dataSource)
                .driverClassName("org.apache.iotdb.jdbc.IoTDBDriver"));
        generator
                .globalConfig(builder -> {
                    builder.author("IoTDB")
                            .enableSwagger()
                            .dateType(DateType.ONLY_DATE)
                            .outputDir("src/main/java");
                })
                .packageConfig(builder -> {
                    builder.parent("com.tech.repository.iotdb")
                            .mapper("mapper")
                            .pathInfo(Collections.singletonMap(OutputFile.xml, "src/main/java/com/tech/repository/iotdb/xml"));
                })
                .dataSourceConfig(builder -> {
                    builder.typeConvertHandler((globalConfig, typeRegistry, metaInfo) -> {
                        int typeCode = metaInfo.getJdbcType().TYPE_CODE;
                        switch (typeCode) {
                            case Types.FLOAT:
                                return DbColumnType.FLOAT;
                            default:
                                return typeRegistry.getColumnType(metaInfo);
                        }
                    });
                })
                .strategyConfig(builder -> {
                    builder.addInclude("table1");
                    builder.entityBuilder()
                            .enableLombok()
//                            .addIgnoreColumns("create_time")
                            .enableFileOverride();
                    builder.serviceBuilder()
                            .formatServiceFileName("%sService")
                            .formatServiceImplFileName("%sServiceImpl")
                            .convertServiceFileName((entityName -> entityName + "Service"))
                            .enableFileOverride();
                    builder.controllerBuilder()
                            .enableRestStyle()
                            .enableFileOverride();
                })
                .execute();
    }
}
