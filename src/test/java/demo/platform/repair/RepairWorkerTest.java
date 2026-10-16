package demo.platform.repair;

import demo.platform.common.PlatformProperties;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

class RepairWorkerTest {
    @Test
    void independentActionsRunConcurrentlyWithinWorkerCapacity() throws Exception {
        var service = mock(RepairExecutionService.class);
        var properties = new PlatformProperties();
        properties.getRepair().setWorkerThreads(2);
        var started = new CountDownLatch(2);
        var release = new CountDownLatch(1);
        var finished = new CountDownLatch(2);
        var running = new AtomicInteger();
        var maximum = new AtomicInteger();
        Runnable action = () -> {
            maximum.accumulateAndGet(running.incrementAndGet(), Math::max);
            started.countDown();
            try {
                assertTrue(release.await(10, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                running.decrementAndGet();
                finished.countDown();
            }
        };
        when(service.nextAction()).thenReturn(action, action, null);
        var worker = new RepairWorker(properties, service);
        try {
            worker.poll();
            assertTrue(started.await(10, TimeUnit.SECONDS));
            worker.poll();
            verify(service, times(2)).nextAction();
            assertEquals(2, maximum.get());
            release.countDown();
            assertTrue(finished.await(10, TimeUnit.SECONDS));
        } finally {
            release.countDown();
            worker.close();
        }
    }

    @Test
    void disabledWorkerDoesNotClaimOrExecuteAnything() {
        var service = mock(RepairExecutionService.class);
        var properties = new PlatformProperties();
        properties.getRepair().setWorkerEnabled(false);
        var worker = new RepairWorker(properties, service);
        worker.start();
        worker.poll();
        verifyNoInteractions(service);
        worker.close();
        verify(service).close();
    }

    @Test
    void closedWorkerDoesNotClaimFurtherWork() {
        var service = mock(RepairExecutionService.class);
        var worker = new RepairWorker(new PlatformProperties(), service);
        worker.close();
        worker.poll();
        verify(service).close();
        verifyNoMoreInteractions(service);
    }
}
