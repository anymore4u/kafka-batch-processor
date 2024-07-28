package com.santander.kafkabatchprocessor;

import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaItemWriter implements ItemWriter<String> {

    private static final Logger logger = LoggerFactory.getLogger(KafkaItemWriter.class);
    private final KafkaProducer<String, String> producer;

    public KafkaItemWriter() {
        Vertx vertx = Vertx.vertx();
        Map<String, String> config = new HashMap<>();
        config.put("bootstrap.servers", "localhost:9092");
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = KafkaProducer.create(vertx, config);
    }

    @Override
    public void write(Chunk<? extends String> chunk) throws Exception {
        for (String item : chunk.getItems()) {
            KafkaProducerRecord<String, String> record = KafkaProducerRecord.create("your_topic", item);
            producer.send(record, result -> {
                if (result.failed()) {
                    logger.error("Failed to send item to Kafka: {}", item, result.cause());
                }
            });
        }
    }
}
