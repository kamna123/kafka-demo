package demo.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.datatransfer.StringSelection;
import java.util.Properties;
/*
 *  Sticky partition - default behaviour, where it will send the batch of msgs to same partition to optimise
 * the behaviour
 * */
public class ProducerDemoWithKey {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKey.class.getSimpleName());
    public static void main(String[] args) {
        log.info("I am a kafka producer");
        // create properties of producer
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // create a record tht you will push using producer

        // send the data - async operation
        for (int i = 0; i < 70; i++) {
            String topic = "demo_java";
            String key = "id_" + i;
            String value = "hello_world " + i;
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic,
                    key, value);


            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    // executes everytime record is successfully send or an exception is thrown
                    if (exception == null) {
                        log.info("Received new metadata/ \n" +
                                "Topic" + metadata.topic() + "\n" +
                                "key" + producerRecord.key() + "\n" +
                                "Partition " + metadata.partition() + "\n" +
                                "Offset " + metadata.offset() + "\n" +
                                "Timestamp " + metadata.timestamp());
                    } else {
                        log.error("Error while producing ", exception);
                    }
                }
            });


        }
        // flush -sync operation
        producer.flush();
        // flush and close producer
        producer.close();



    }
}
