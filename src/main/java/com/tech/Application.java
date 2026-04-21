package com.tech;

import lombok.extern.slf4j.Slf4j;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.io.ClassPathResource;
import org.springframework.util.StreamUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;

@Slf4j
@SpringBootApplication
@MapperScan("com.tech.repository.iotdb.mapper")
public class Application {

	public static void main(String[] args) {
		// 在 Spring 启动前，初始化 SSL 证书
		initIoTDBTrustStore();

		SpringApplication.run(Application.class, args);
	}

	public static void initIoTDBTrustStore() {
		try {
			// 读取 resources 目录下的证书文件流
			ClassPathResource resource = new ClassPathResource("iotdb_truststore.jks");
			if (!resource.exists()) {
				System.err.println("未找到 IoTDB 信任库证书！");
				return;
			}

			// 在操作系统临时目录创建一个临时文件
			File tempTrustStore = File.createTempFile("iotdb_truststore_", ".jks");
			// 确保 JVM 退出时自动清理该临时文件
			tempTrustStore.deleteOnExit();

			// 将内部流拷贝到外部物理文件
			try (InputStream is = resource.getInputStream();
                 FileOutputStream fos = new FileOutputStream(tempTrustStore)) {
				StreamUtils.copy(is, fos);
			}

			// 将绝对路径动态注入到当前 JVM 环境中
			System.setProperty("javax.net.ssl.trustStore", tempTrustStore.getAbsolutePath());
			System.setProperty("javax.net.ssl.trustStorePassword", "123456"); // 你的密码
			System.setProperty("javax.net.ssl.trustStoreType", "JKS");

            log.info("IoTDB SSL 证书已成功加载至临时路径: {}", tempTrustStore.getAbsolutePath());

		} catch (Exception e) {
			throw new RuntimeException("初始化 IoTDB SSL 信任库失败", e);
		}
	}
}
