package demo.kafka;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;


public class ConsumerDemoWithShutdown {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());
    public static void main(String[] args) {
        log.info("I am a kafka consumer");
        String servers = "127.0.0.1:9092";
        String groupID = "application-consumer-4";
        String topic = "demo_java";
        //create config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run() {
                log.info("Detected a shutdown, lets shutdown by calling consumer wakeup");
                consumer.wakeup();
                try{
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
           // join the main thread to allow complete execution of main thread

        });

        try {
            //sub consumer to topic
            consumer.subscribe(Collections.singletonList(topic));

            // poll for new data
            while (true) {
           //     log.info("Polling");

                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    log.info("key : " + record.key() + ", Value : " + record.value());
                    log.info("Partition : " + record.partition() + ", offset : "  + record.offset());

                }
            }

        } catch (WakeupException e) {
            log.info("Wake up exception");
        } catch (Exception e) {
            log.error("Unexpected exception");
        } finally {
            consumer.close(); // will commit the offset
            log.info("Consumer is gracefully shut down");
        }


    }
}
