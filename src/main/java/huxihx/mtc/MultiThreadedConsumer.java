package huxihx.mtc;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
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
import java.util.concurrent.atomic.AtomicBoolean;

public class MultiThreadedConsumer implements Runnable {

    private final static Executor executor = Executors
            .newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 50, r -> {
                Thread t = new Thread(r);
                t.setDaemon(true);
                return t;
            });

    private final Map<TopicPartition, ConsumerWorker<String, String>> outstandingWorkers = new HashMap<>();
    private final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
    private long lastCommitTime = System.currentTimeMillis();
    private final Consumer<String, String> consumer;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final int DEFAULT_COMMIT_INTERVAL = 3000;

    public MultiThreadedConsumer(String groupID) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        consumer = new KafkaConsumer<>(props);
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(Collections.singletonList("test"),
                    new MultiThreadedRebalanceListener(consumer, outstandingWorkers, offsetsToCommit));
            while (!closed.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                distributeRecords(records);
                checkOutstandingWorkers();
                commitOffsets();
            }
        } catch (WakeupException e) {
            if (!closed.get())
                throw e;
        } finally {
            consumer.close();
        }
    }

    public void shudown() {
        closed.set(true);
        consumer.wakeup();
    }

    private void checkOutstandingWorkers() {
        Set<TopicPartition> completedPartitions = new HashSet<>();

        outstandingWorkers.forEach((tp, worker) -> {
            if (worker.isFinished()) {
                completedPartitions.add(tp);
            }
            long offset = worker.getLatestProcessedOffset();
            if (offset > 0L) {
                offsetsToCommit.put(tp, new OffsetAndMetadata(offset));
            }
        });
        completedPartitions.forEach(outstandingWorkers::remove);
        consumer.resume(completedPartitions);
    }

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
