package net.project.kafka.orderconsumer.s9.consumergroup;

import net.project.kafka.orderconsumer.s9.consumergroup.model.Order;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.*;

public class OrderConsumer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, OrderDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "OrderGroup");
        // props.setProperty("auto.commit.interval.ms", "2000");
        props.setProperty("auto.commit.offset", "false");   // manually committing offsets, giving user control over when a message is considered processed

        /*
		props.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "102412323");
		props.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "200");
		props.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000");
		props.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "3000");
		//30 partions,5 consumers,6MB - 12MB
		props.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "1MB");
		props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "OrderConsumer");
		props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
		props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());
		*/

        // Keeps track of the latest processed offsets for each partition
        // Important for manual commits and safe rebalance handling
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props);

        // below methods are invoked suppose if re-balance happens. It must be written before subscribe method
        // Kafka can rebalance partitions when consumers join/leave the group or when topic partitions are added/removed
        class RebalanceHandler implements ConsumerRebalanceListener {

            // commit any offsets that are processed, but not yet committed before re-balance
            // Before partitions are taken away, commit the offsets of messages you’ve processed.
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                consumer.commitSync(currentOffsets); // commit last record that was processed before re-balance
            }

            // When partitions are assigned, seek to the last committed offset for each partition.
            // After partitions are assigned, seek to the last committed offsets to avoid processing duplicates.
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {

            }
        }

        consumer.subscribe(Collections.singletonList("OrderConsumerGroup"), new RebalanceHandler());

        try {
            while (true) {
                ConsumerRecords<String, Order> records = consumer.poll(Duration.ofSeconds(20));
                int count = 0;

                for (ConsumerRecord<String, Order> record: records) {
                    String customerName = record.key();
                    Order order = record.value();
                    System.out.println(String.format("CustomerName=%s, Product=%s, Quantity=%d, Partition=%d", customerName, order.getProduct(), order.getQuantity(), record.partition()));

                    // this map will have the latest offset that is processed. Useful when re-balance
                    // Updates currentOffsets map with the next offset (record.offset() + 1) for that partition
                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1));

                    // offset commit. To minimize risk when re-balance happens
                    // Commits offsets asynchronously every 10 records. Async commits are faster but can fail silently; handled by OffsetCommitCallback
                    // The count % 10 commit strategy balances performance and reliability
                    if (count % 10 == 0) {
                        consumer.commitAsync(currentOffsets, new OffsetCommitCallback() {
                            @Override
                            public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                                if(exception != null) {
                                    System.out.println("Commit failed for offset: " + offsets);
                                }
                            }
                        });
                    }
                    count++;
                }
                // commitSync, commitAsync, custom commit
                // consumer.commitSync();                       // commitSync(): Blocks until offsets are committed. Safer but slower.

                /*
                // Non-blocking, faster. Use callback to catch errors.
                consumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception e) {
                        if (e != null) {
                            System.out.println("Commit failed for offset: " + offsets);
                        }
                    }
                });
                */
            }
        } finally {
            consumer.close();
        }
    }
}