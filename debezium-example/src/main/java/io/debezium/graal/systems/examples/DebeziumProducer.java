package io.debezium.graal.systems.examples;


import io.debezium.embedded.EmbeddedEngine;
import io.debezium.config.Configuration;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

import java.text.MessageFormat;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class DebeziumProducer implements Runnable{
    private static final Properties props = new Properties();
    private PulsarClient client;
    private Logger logger = LoggerFactory.getLogger(DebeziumProducer.class);
    private DebeziumProducer() {
        String propFileName = "config.properties";
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
        try {
            props.load(inputStream);
        } catch (IOException e) {
            logger.error("Can't load config properties file");
        }
    }

    @Override
    public void run() {
        try {
            client = PulsarClient.builder()
                    .serviceUrl(props.getProperty("pulsar.broker.address"))
                    .build();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
        Configuration config = Configuration.from(props);

        EmbeddedEngine engine = EmbeddedEngine.create()
                .using(config)
                .notifying(this::sendRecord)
                .build();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(engine);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shut down");
            engine.stop();
        }));

        executor.shutdown();

        awaitTermination(executor);
    }

    private void sendRecord(SourceRecord record){
        logger.info("Send record on topic: " + record.topic());

        String topic = MessageFormat.format(props.getProperty("pulsar.topic"), record.topic());
        Producer<String> producer;
        try {
            producer = client.newProducer(Schema.STRING)
                    .topic(topic)
                    .create();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
        while(true)
        {
            try {
                producer.send(record.value().toString());
                logger.info("Send record content: " + record.value());
                break;
            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void awaitTermination(ExecutorService executor) {
        try {
            while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                logger.debug("Await");
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static void main(String[] args) {
        new DebeziumProducer().run();
    }
}
