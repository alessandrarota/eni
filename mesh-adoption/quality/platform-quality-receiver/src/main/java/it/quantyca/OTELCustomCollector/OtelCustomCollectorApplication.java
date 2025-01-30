package it.quantyca.OTELCustomCollector;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class OtelCustomCollectorApplication {

	public static void main(String[] args) throws Exception {
		SpringApplication.run(OtelCustomCollectorApplication.class, args);
	}

}