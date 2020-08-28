package huxihx.mtc;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Map;

public class MultiThreadedRebalanceListener implements ConsumerRebalanceListener {

    private final Consumer<String, String> consumer;
    private final Map<TopicPartition, ConsumerWorker<String, String>> outstandingWorkers;

    public MultiThreadedRebalanceListener(Consumer<String, String> consumer,
                                          Map<TopicPartition, ConsumerWorker<String, String>> outstandingWorkers) {
        this.consumer = consumer;
        this.outstandingWorkers = outstandingWorkers;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        // todo
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        // todo
    }
}
