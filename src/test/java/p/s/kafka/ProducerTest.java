package p.s.kafka;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

class ProducerTest {

    @Test
    void shouldSendMessageAfter10SecondsWhenLingerMsIsSetTo10Seconds() {
        Properties kafkaProps = createKafkaConnectionProperties();
        kafkaProps.put("linger.ms", "10000");
        kafkaProps.put("batch.size", "33554432");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaProps);

        sendMessage(producer, "ONE", "Poland");
    }

    @Test
    void shouldSendOneBatchMessageAfter10SecondsWhenLingerMsIsSetTo10Seconds() throws ExecutionException, InterruptedException {
        Properties kafkaProps = createKafkaConnectionProperties();
        kafkaProps.put("linger.ms", "10000");
        kafkaProps.put("batch.size", "33554432");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaProps);

        Future message1 = sendMessage(producer, "ONE", "France");
        Future message2 = sendMessage(producer, "ONE", "Poland");

        message1.get();
        message2.get();

    }

    private Future sendMessage(KafkaProducer<String, String> producer, String messageKey, String messageBody) {
        ProducerRecord<String, String> record =
                new ProducerRecord<>("CustomerCountry", messageKey,
                        messageBody);
        try {
            return producer.send(record);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private Properties createKafkaConnectionProperties() {
        Properties kafkaProps = new Properties();

        kafkaProps.put("bootstrap.servers", "localhost:29092,localhost:39092");
        kafkaProps.put("key.serializer",  "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        return kafkaProps;
    }
}
