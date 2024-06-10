package io.confluent.csta.timestamp.avro;

import io.confluent.csta.timestamp.Customer;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Properties;

public class AvroProducer {
    private static final Logger log = LoggerFactory.getLogger(AvroProducer.class);
    private static boolean running = true;
    private static final long SLEEP_TIME_MS = 500;
    private static final String TOPIC = "customers";

    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        properties.load(AvroProducer.class.getResourceAsStream("/configuration.properties"));

        Producer<String, Customer> producer = new KafkaProducer<>(properties);

        final Thread mainThread = Thread.currentThread();
        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected a shutdown, let's set running to false and interrupt thread...");
            running = false;
            mainThread.interrupt();
        }));

        try {
            while (running) {
                String firstName = RandomStringUtils.randomAlphanumeric(3).toUpperCase();
                String lastName = RandomStringUtils.randomAlphanumeric(5).toUpperCase();
                String fullName = firstName + " " + lastName;
                Customer customer = Customer.newBuilder()
                        .setCustomerTime(Instant.now().truncatedTo(ChronoUnit.MICROS))
                        .setFirstName(firstName)
                        .setLastName(lastName)
                        .build();

                //kafka producer - asynchronous writes
                final ProducerRecord<String, Customer> record = new ProducerRecord<>(TOPIC, fullName, customer);
                //send with callback
                producer.send(record, (metadata, e) -> {
                    if (e != null)
                        log.info("Send failed for record {}", record, e);
                    else
                        log.info("Sent name={}, time={} - Partition-{} - Offset {}", record.key(),
                                record.value().getCustomerTime(), metadata.partition(), metadata.offset());
                });
                //sleep
                Thread.sleep(SLEEP_TIME_MS);
            }
        } catch (InterruptedException e) {
            log.info("Thread interrupted.");
        } finally {
            log.info("Closing producer");
            producer.close();
        }

    }
}
