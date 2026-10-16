package demo.platform.repair;

import demo.platform.common.PlatformProperties;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * In-process repair scheduler: one poller thread claims due work from the database every second and
 * hands each unit to a bounded worker pool. All coordination state is in the database, so any number
 * of replicas can run workers, and a worker can stop at any point without losing work.
 */
@Component
@Slf4j
public class RepairWorker {
    private final RepairExecutionService execution;
    private final ScheduledExecutorService poller;
    private final ExecutorService workers;
    private final AtomicInteger active = new AtomicInteger();
    private final int capacity;
    private volatile boolean closing;

    public RepairWorker(PlatformProperties properties, RepairExecutionService execution) {
        this.execution = execution;
        capacity = properties.getRepair().getWorkerThreads();
        if (capacity < 1 || capacity > 64) throw new IllegalArgumentException("Repair worker threads must be 1..64");
        boolean enabled = properties.getRepair().isWorkerEnabled();
        workers = enabled ? Executors.newFixedThreadPool(capacity, threads("repair-worker-")) : null;
        poller = enabled ? Executors.newSingleThreadScheduledExecutor(threads("repair-poller-")) : null;
    }

    private static ThreadFactory threads(String prefix) {
        AtomicInteger sequence = new AtomicInteger();
        return task -> {
            Thread thread = new Thread(task, prefix + sequence.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        if (poller != null) poller.scheduleWithFixedDelay(this::poll, 0, 1, TimeUnit.SECONDS);
    }

    /** Claims only while a worker slot is free, so claimed work never waits behind a full pool. */
    public void poll() {
        if (workers == null || closing) return;
        try {
            execution.reconcileExpired();
            while (!closing && active.get() < capacity) {
                Runnable action = execution.nextAction();
                if (action == null) return;
                active.incrementAndGet();
                try {
                    workers.submit(() -> {
                        try {
                            action.run();
                        } catch (RuntimeException e) {
                            log.warn("Repair action failed; its lease will expire and the work is revisited", e);
                        } finally {
                            active.decrementAndGet();
                        }
                    });
                } catch (RejectedExecutionException e) {
                    active.decrementAndGet();
                    return; // Shutting down: the unexecuted claim's short lease expires safely.
                }
            }
        } catch (Exception e) {
            log.warn("Repair poll failed; durable work will be revisited", e);
        }
    }

    @PreDestroy
    public void close() {
        closing = true;
        execution.close();
        if (poller != null) poller.shutdownNow();
        // Interrupting a local action does not cancel the remote Cassandra repair; it is observed again later.
        if (workers != null) workers.shutdownNow();
    }
}
