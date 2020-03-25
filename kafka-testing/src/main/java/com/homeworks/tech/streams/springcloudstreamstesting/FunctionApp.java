package com.homeworks.tech.streams.springcloudstreamstesting;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.function.Function;

@SpringBootApplication
@Slf4j
public class FunctionApp {

	public static void main(String[] args) {
		SpringApplication.run(FunctionApp.class, args);
	}

	@Bean
	public Function<String, String> process() {
		return str -> {
			log.info("Received new message {}", str);
			String output = str.toUpperCase();
			log.info("Sending out new message {}", output);
			return output;
		};
	}

}
