package com.xxl.job.admin;

import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author xuxueli 2018-10-28 00:38:13
 */
@SpringBootApplication
public class XxlJobAdminApplication {
	public static void main(String[] args) {
		Logger logger = LoggerFactory.getLogger("XxlJobAdminApplication");
		logger.info("Setting default TimeZone to PST");	
		TimeZone.setDefault(TimeZone.getTimeZone("PST"));
        SpringApplication.run(XxlJobAdminApplication.class, args);
	}

}