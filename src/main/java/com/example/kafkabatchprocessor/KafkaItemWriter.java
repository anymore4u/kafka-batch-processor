package com.example.kafkabatchprocessor;

import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
        config.put("bootstrap.servers", "kafka:9092");
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = KafkaProducer.create(vertx, config);
    }

    @Override
    public void write(List<? extends String> items) throws Exception {
/*        long startTime = System.currentTimeMillis();
        logger.info("Starting to write chunk at {}", startTime);
        logger.info("Items to write: {}", items.size());*/

        for (String item : items) {
            //logger.info("Writing item to Kafka: {}", item);
            KafkaProducerRecord<String, String> record = KafkaProducerRecord.create("your_topic", item);
            producer.send(record, result -> {
                if (result.failed()) {
                    logger.error("Failed to send item to Kafka: {}", item, result.cause());
                }
            });
        }

        /*long endTime = System.currentTimeMillis();
        logger.info("Finished writing chunk at {}", endTime);
        logger.info("Chunk processing time: {} ms", (endTime - startTime));*/
    }
}
