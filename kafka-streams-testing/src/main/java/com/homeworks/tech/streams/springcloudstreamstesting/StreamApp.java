package com.homeworks.tech.streams.springcloudstreamstesting;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.function.Function;

@SpringBootApplication
@Slf4j
public class StreamApp {

	public static void main(String[] args) {
		SpringApplication.run(StreamApp.class, args);
	}

	@Bean
	public Function<KStream<String, String>, KStream<String, String>> process() {
		return stream -> stream
				.peek((key, payload) -> log.info("Message {} with payload {}", key, payload))
				.mapValues((ValueMapper<String, String>) String::toUpperCase)
				.peek((key, payload) -> log.info("Outgoing message {} with payload {}", key, payload));
	}

}
