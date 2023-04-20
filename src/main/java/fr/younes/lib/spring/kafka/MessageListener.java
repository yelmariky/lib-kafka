package fr.younes.lib.spring.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;


@ConditionalOnExpression(
        "${consumer.atLeastOnce.enabled:true} OR ${consumer.exactlyOne.enabled:true} OR ${consumer.mostOnce.enabled:true} "
)
public class MessageListener {
    Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private CountDownLatch latch = new CountDownLatch(3);

    private CountDownLatch partitionLatch = new CountDownLatch(2);

    private CountDownLatch filterLatch = new CountDownLatch(2);

    private CountDownLatch greetingLatch = new CountDownLatch(1);
    @Value("consumer.groupID")
    private String groupId;
    private ConsumerRecord<?, ?> record ;

    @KafkaListener(topics = "${consumer.topic.name}", groupId = "${consumer.groupID}", containerFactory = "fooKafkaListenerContainerFactory")
    public void readTopics(ConsumerRecord<?, ?> record) {
        logger.debug("Received Message in group: {} key: {}, partition: {}, offset: {}, value: {}", groupId, record.key(), record.partition(), record.offset(), record.value() );
        this.record = record;
        latch.countDown();
    }

    @KafkaListener(topics = "${consumer.topic.name}", groupId = "${consumer.groupID}", containerFactory = "fooKafkaListenerContainerFactory")
    public void readTopicsObject(String in) {
        logger.debug("Received Message in group: {} key: {}, partition: {}, offset: {}, value: {}", groupId, record.key(), record.partition(), record.offset(), record.value() );
       // this.record = record;

        logger.debug("Received Message {}",in);
        latch.countDown();
    }

    public Object readTopics() {
        //logger.debug("Received Message in group: {} key: {}, partition: {}, offset: {}, value: {}", groupId, record.key(), record.partition(), record.offset(), record.value() );
        logger.debug("Received {}",this.record.value());
        latch.countDown();
        return this.record.value();
    }


    @KafkaListener(topics = "${message.topic.name}", containerFactory = "headersKafkaListenerContainerFactory")
    public void readTopicsWithHeaders(@Payload String message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        System.out.println("Received Message: " + message + " from partition: " + partition);
        latch.countDown();
    }

    @KafkaListener(topicPartitions = @TopicPartition(topic = "${partitioned.topic.name}", partitions = { "0", "3" }), containerFactory = "partitionsKafkaListenerContainerFactory")
    public void readTopicsToPartition(@Payload String message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        System.out.println("Received Message: " + message + " from partition: " + partition);
        this.partitionLatch.countDown();
    }
/**
    @KafkaListener(topics = "${filtered.topic.name}", containerFactory = "filterKafkaListenerContainerFactory")
    public void listenWithFilter(String message) {
        System.out.println("Received Message in filtered listener: " + message);
        this.filterLatch.countDown();
    }

    @KafkaListener(topics = "${greeting.topic.name}", containerFactory = "greetingKafkaListenerContainerFactory")
    public void greetingListener(Greeting greeting) {
        System.out.println("Received greeting message: " + greeting);
        this.greetingLatch.countDown();
    }
**/
}