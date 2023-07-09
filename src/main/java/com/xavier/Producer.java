package com.xavier;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(Producer.class);

    public static void main(String[] args) {
        log.info("I'm Kafka Producer");


        String bootstrapServers = "localhost:9092";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create a producer
        try  {
            KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

            for (int i = 0; i < 10; i++) {
                String key = "id_" + i;

                // create a producer record
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>("demo_java", key, "oi - "+i);

                producer.send(producerRecord, new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        // executes every time a record is successfully sent or an exception is thrown
                        if (e == null) {
                            // the record was successfully sent
                            log.info("Received new metadata. \n" +
                                    "Topic:" + recordMetadata.topic() + "\n" +
                                    "Key:" + producerRecord.key() + "\n" +
                                    "Partition: " + recordMetadata.partition() + "\n" +
                                    "Offset: " + recordMetadata.offset() + "\n" +
                                    "Timestamp: " + recordMetadata.timestamp());
                        } else {
                            log.error("Error while producing", e);
                        }
                    }
                });

                // to observe the round-robin feature of Kafka, we can add a Thread.sleep(1000) in between each
                // iteration of the loop, which will force the batch to be sent and a new batch to be created for a
                // different partition.
                // ** without sleep, it was not possible to see the round-robin in action whitin this test because: **
                // since Kafka v2.4.0, the partitioner is a Sticky Partitioner, which means the producer that
                // receives messages sent in time close to each other will try to fill a batch into ONE partition
                // before switching to creating a batch for another partition.
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        } catch (Exception e) {
            log.error("Error while creating a producer", e);
        }


    }

    @Override
    public void close() throws RuntimeException {
        log.info("Closing producer with close() method from Autocloseable interface");
    }
}