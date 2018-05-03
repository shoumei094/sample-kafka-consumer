package lol.shomei;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class SampleKafkaConsumer {
    private static final AtomicBoolean closed = new AtomicBoolean(false);
    private static final String TOPIC = "topic1";
    private static final String BOOTSTRAP_SERVERS = "192.168.99.106:9092,192.168.99.107:9092";

    public static void main(String[] args) {
        runConsumer();
    }

    private static Consumer<Long, String> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "TestGroup");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(props);
    }

    private static void runConsumer() {
        final Consumer<Long, String> consumer = createConsumer();

        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try {
            consumer.subscribe(Collections.singletonList(TOPIC));
            while (!closed.get()) {
                ConsumerRecords<Long, String> records = consumer.poll(100);
                records.forEach(record ->
                    System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
                            record.key(), record.value(),
                            record.partition(), record.offset())
                );
                consumer.commitAsync();
            }
        } catch (WakeupException e) {
            // ignore since we're closing
        } catch (Exception e) {
            System.out.println("Unexpected error");
        } finally {
            try {
                consumer.commitSync();
            } finally {
                closed.set(true);
                consumer.close();
            }
        }
    }
}
