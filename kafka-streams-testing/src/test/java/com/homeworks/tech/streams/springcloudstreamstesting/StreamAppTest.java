package com.homeworks.tech.streams.springcloudstreamstesting;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.ClassRule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class StreamAppTest {

    private static final String IN_TOPIC = "words_unique";
    private static final String OUT_TOPIC = "capitalized_unique";

    @ClassRule
    private static EmbeddedKafkaRule embeddedKafkaRule = new EmbeddedKafkaRule(1, true);
    private static EmbeddedKafkaBroker embeddedKafka = embeddedKafkaRule.getEmbeddedKafka();

    private static Consumer<String, String> consumer;
    private static KafkaTemplate<String, String> producer;

    @BeforeAll
    public static void setUp() {
        embeddedKafka.afterPropertiesSet();
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testConsumerGroup", "false",
                embeddedKafka);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
        consumer = cf.createConsumer();

        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafka);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(producerProps);
        producer = new KafkaTemplate<>(pf, true);
    }

    @AfterAll
    public static void tearDown() {
        consumer.close();
    }

    @Test
    public void shouldUppercaseIncomingWords() {
        String newTopic = IN_TOPIC + "shouldUppercaseIncomingWords";
        String newOutTopic = OUT_TOPIC + "shouldUppercaseIncomingWords";
        embeddedKafka.addTopics(newTopic, newOutTopic);
        embeddedKafka.consumeFromAnEmbeddedTopic(consumer, newOutTopic);
        SpringApplication app = new SpringApplication(StreamApp.class);
        app.setWebApplicationType(WebApplicationType.NONE);

        try (ConfigurableApplicationContext context = app.run(
                "--spring.cloud.stream.bindings.process-in-0.destination=" + newTopic,
                "--spring.cloud.stream.bindings.process-out-0.destination=" + newOutTopic,
                "--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString())) {

            producer.send(newTopic, "uppercase");
            ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(consumer, newOutTopic);
            assertThat(record.value()).isEqualTo("UPPERCASE");

        }
    }

    @Test
    public void shouldBeAbleToRunEachTestInIsolation() {
        String newTopic = IN_TOPIC + "shouldBeAbleToRunEachTestInIsolation";
        String newOutTopic = OUT_TOPIC + "shouldBeAbleToRunEachTestInIsolation";
        embeddedKafka.addTopics(newTopic, newOutTopic);
        embeddedKafka.consumeFromAnEmbeddedTopic(consumer, newOutTopic);
        SpringApplication app = new SpringApplication(StreamApp.class);
        app.setWebApplicationType(WebApplicationType.NONE);

        try (ConfigurableApplicationContext context = app.run(
                "--spring.cloud.stream.bindings.process-in-0.destination=" + newTopic,
                "--spring.cloud.stream.bindings.process-out-0.destination=" + newOutTopic,
                "--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString())) {
            producer.send(newTopic, "test");
            ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(consumer, newOutTopic);
            assertThat(record.value()).isEqualTo("TEST");

        }
    }
}
