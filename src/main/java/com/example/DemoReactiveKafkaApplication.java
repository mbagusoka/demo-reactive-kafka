package com.example;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

@SpringBootApplication
@ConfigurationPropertiesScan
public class DemoReactiveKafkaApplication {

    public static void main(String[] args) {
        SpringApplication.run(DemoReactiveKafkaApplication.class, args);
    }

}

@RestController
@RequiredArgsConstructor
class DummyController {

    private final MessageProducer producer;

    @GetMapping("/fill/{count}")
    public Flux<String> fillMessage(@PathVariable int count) {
        return producer.produce(count).map(RecordMetadata::toString);
    }

    @GetMapping(value = "/fill/sse/{count}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> fillMessageSse(@PathVariable int count) {
        return producer.produce(count)
            .map(RecordMetadata::toString)
            .delayElements(Duration.ofSeconds(1));
    }

    @GetMapping(value = "fill/token/{token}")
    public Mono<String> fillToken(@PathVariable String token) {
        return Mono.just(token)
            .flatMap(producer::produce)
            .map(rec -> token);
    }
}

@Component
@RequiredArgsConstructor
@Slf4j
class Initializer implements CommandLineRunner {

    private final MessageProducer producer;

    @Override
    public void run(String... args) {
        producer.produce(10).subscribe();
    }
}

@Service
@RequiredArgsConstructor
@Slf4j
class MessageProducer {

    private final KafkaSender<String, String> sender;

    private final KafkaProperties properties;

    public Flux<RecordMetadata> produce(int count) {
        String topic = properties.getTopics().get(0);
        Flux<SenderRecord<String, String, String>> recordFlux = getRecordFlux(count, topic);

        return sender.send(recordFlux)
            .map(SenderResult::recordMetadata)
            .doOnError(e -> log.error("Send failed", e));
    }

    public Mono<RecordMetadata> produce(String token) {
        String topic = properties.getTopics().get(0);
        Mono<SenderRecord<String, String, String>> recordMono =
            Mono.just(SenderRecord.create(
                new ProducerRecord<>(topic, token),
                UUID.randomUUID().toString()
            ));

        return sender.send(recordMono)
            .map(SenderResult::recordMetadata)
            .single();
    }

    private Flux<SenderRecord<String, String, String>> getRecordFlux(
        int count, String topic
    ) {
        return Flux.range(1, count)
            .map(String::valueOf)
            .<SenderRecord<String, String, String>>map(i -> SenderRecord.create(
                new ProducerRecord<>(topic, UUID.randomUUID().toString()), i
            ))
            .doOnNext(rec -> log.info("Produced message {}", rec.value()));
    }
}

@Service
@RequiredArgsConstructor
@Slf4j
class Listener {

    private final KafkaReceiver<String, String> kafkaReceiver;

    private Disposable recordFlux;

    @PostConstruct
    public void listen() {
        recordFlux = kafkaReceiver.receive()
            .map(ReceiverRecord::value)
            .doOnNext(v -> log.info("Consumed with value {}", v))
            .subscribe();
    }

    @PreDestroy
    public void close() {
        recordFlux.dispose();
    }
}

@ConfigurationProperties(prefix = "kafka")
@Value
class KafkaProperties {

    String bootstrapServers = "localhost:9092";

    String clientId = "sample-consumer";

    String groupId = "sample-group";

    List<String> topics = Collections.singletonList("demo-topic");
}

@Configuration
@Slf4j
class KafkaConsumerConfiguration {

    @Bean
    public ReceiverOptions<String, String> receiverOptions(KafkaProperties properties) {
        return ReceiverOptions.<String, String>create()
            .consumerProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                properties.getBootstrapServers()
            )
            .consumerProperty(CLIENT_ID_CONFIG, properties.getClientId())
            .consumerProperty(GROUP_ID_CONFIG, properties.getGroupId())
            .consumerProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
            .consumerProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
            .consumerProperty(AUTO_OFFSET_RESET_CONFIG, "latest")
            .subscription(properties.getTopics())
            .addAssignListener(partitions -> log.debug("Assigned: {}", partitions))
            .addRevokeListener(partitions -> log.debug("Revoked: {}", partitions));
    }

    @Bean
    public KafkaReceiver<String, String> kafkaReceiver(ReceiverOptions<String, String> options) {
        return KafkaReceiver.create(options);
    }
}

@Configuration
class KafkaProducerConfiguration {

    @Bean
    public SenderOptions<String, String> senderOptions(KafkaProperties properties) {
        return SenderOptions.<String, String>create()
            .producerProperty(BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers())
            .producerProperty(ACKS_CONFIG, "all")
            .producerProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
            .producerProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    }

    @Bean
    public KafkaSender<String, String> kafkaSender(SenderOptions<String, String> options) {
        return KafkaSender.create(options);
    }
}