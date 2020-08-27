package huxihx.mtc;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class ConsumerWorker<K, V> implements Runnable {

    private final List<ConsumerRecord<K, V>> recordsOfSamePartition;
    private volatile boolean started = false;
    private volatile boolean stopped = false;
    private final ReentrantLock lock = new ReentrantLock();

    private final long INVALID_COMMITTED_OFFSET = -1L;
    private final AtomicLong latestProcessedOffset = new AtomicLong(INVALID_COMMITTED_OFFSET);
    private final CompletableFuture<Long> future = new CompletableFuture<>();

    public ConsumerWorker(List<ConsumerRecord<K, V>> recordsOfSamePartition) {
        this.recordsOfSamePartition = recordsOfSamePartition;
    }

    @Override
    public void run() {
        lock.lock();
        if (stopped)
            return;
        started = true;
        lock.unlock();
        for (ConsumerRecord<K, V> record : recordsOfSamePartition) {
            if (stopped)
                break;
            handleRecord(record);
            latestProcessedOffset.set(record.offset() + 1);
        }
        future.complete(latestProcessedOffset.get());
    }

    public long getLatestProcessedOffset() {
        return latestProcessedOffset.get();
    }

    private void handleRecord(ConsumerRecord<K, V> record) {
        try {
            Thread.sleep(ThreadLocalRandom.current().nextInt(1000));
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
        System.out.println("Finished message processed. Record offset = " + record.offset());
    }

    public void close() {
        lock.lock();
        this.stopped = true;
        if (!started) {
            future.complete(latestProcessedOffset.get());
        }
        lock.unlock();
    }

    public boolean isFinished() {
        return future.isDone();
    }

    public long waitForCompletion(long timeout, TimeUnit timeUnit) {
        try {
            return future.get(timeout, timeUnit);
        } catch (Exception e) {
            if (e instanceof InterruptedException)
                Thread.currentThread().interrupt();
            return INVALID_COMMITTED_OFFSET;
        }
    }
}
