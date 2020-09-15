package huxihx.mtc;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class MultiThreadedConsumer {

    private final Map<TopicPartition, ConsumerWorker<String, String>> outstandingWorkers = new HashMap<>();
    private final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
    private long lastCommitTime = System.currentTimeMillis();
    private final Consumer<String, String> consumer;
    private final int DEFAULT_COMMIT_INTERVAL = 3000;
    private final Map<TopicPartition, Long> currentConsumedOffsets = new HashMap<>();
    private final long expectedCount;

    private final static Executor executor = Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors() * 10, r -> {
                Thread t = new Thread(r);
                t.setDaemon(true);
                return t;
            });

    public MultiThreadedConsumer(String brokerId, String topic, String groupID, long expectedCount) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerId);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic), new MultiThreadedRebalanceListener(consumer, outstandingWorkers, offsetsToCommit));
        this.expectedCount = expectedCount;
    }

    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                distributeRecords(records);
                checkOutstandingWorkers();
                commitOffsets();
                if (currentConsumedOffsets.values().stream().mapToLong(Long::longValue).sum() >= expectedCount) {
                    break;
                }
            }
        } finally {
            consumer.close();
        }
    }

    /**
     * 对已完成消息处理并提交位移的分区执行resume操作
     */
    private void checkOutstandingWorkers() {
        Set<TopicPartition> completedPartitions = new HashSet<>();
        outstandingWorkers.forEach((tp, worker) -> {
            if (worker.isFinished()) {
                completedPartitions.add(tp);
            }
            long offset = worker.getLatestProcessedOffset();
            currentConsumedOffsets.put(tp, offset);
            if (offset > 0L) {
                offsetsToCommit.put(tp, new OffsetAndMetadata(offset));
            }
        });
        completedPartitions.forEach(outstandingWorkers::remove);
        consumer.resume(completedPartitions);
    }

    /**
     * 提交位移
     */
    private void commitOffsets() {
        try {
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastCommitTime > DEFAULT_COMMIT_INTERVAL && !offsetsToCommit.isEmpty()) {
                consumer.commitSync(offsetsToCommit);
                offsetsToCommit.clear();
            }
            lastCommitTime = currentTime;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 将不同分区的消息交由不同的线程，同时暂停该分区消息消费
     * @param records
     */
    private void distributeRecords(ConsumerRecords<String, String> records) {
        if (records.isEmpty())
            return;
        Set<TopicPartition> pausedPartitions = new HashSet<>();
        records.partitions().forEach(tp -> {
            List<ConsumerRecord<String, String>> partitionedRecords = records.records(tp);
            pausedPartitions.add(tp);
            final ConsumerWorker<String, String> worker = new ConsumerWorker<>(partitionedRecords);
            CompletableFuture.supplyAsync(worker::run, executor);
            outstandingWorkers.put(tp, worker);
        });
        consumer.pause(pausedPartitions);
    }
}
