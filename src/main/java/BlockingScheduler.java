
import java.util.concurrent.*;

/**
 * Author: Amitabh Awasthi
 * Description:-
 * 1. Other methods from ScheduledThreadPoolExecutor to execute or submit a task
 * internally call #schedule which will still in turn invoke the methods overridden below
 */


public class BlockingScheduler extends ScheduledThreadPoolExecutor {
    private final Semaphore maxQueueSize;

    public BlockingScheduler(int corePoolSize,
                             ThreadFactory threadFactory,
                             int maxQueueSize) {
        super(corePoolSize, threadFactory, new AbortPolicy());
        this.maxQueueSize = new Semaphore(maxQueueSize);
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command,
                                       long delay,
                                       TimeUnit unit) {
        final long newDelayInMs = beforeSchedule(command, unit.toMillis(delay));
        return super.schedule(command, newDelayInMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable,
                                           long delay,
                                           TimeUnit unit) {
        final long newDelayInMs = beforeSchedule(callable, unit.toMillis(delay));
        return super.schedule(callable, newDelayInMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command,
                                                  long initialDelay,
                                                  long period,
                                                  TimeUnit unit) {
        final long newDelayInMs = beforeSchedule(command, unit.toMillis(initialDelay));
        return super.scheduleAtFixedRate(command, newDelayInMs, unit.toMillis(period), TimeUnit.MILLISECONDS);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command,
                                                     long initialDelay,
                                                     long period,
                                                     TimeUnit unit) {
        final long newDelayInMs = beforeSchedule(command, unit.toMillis(initialDelay));
        return super.scheduleWithFixedDelay(command, newDelayInMs, unit.toMillis(period), TimeUnit.MILLISECONDS);
    }

    @Override
    protected void afterExecute(Runnable runnable, Throwable t) {
        super.afterExecute(runnable, t);
        try {
            if (t == null && runnable instanceof Future<?>) {
                try {
                    ((Future<?>) runnable).get();
                } catch (CancellationException | ExecutionException e) {
                    t = e;
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt(); // ignore/reset
                }
            }
            if (t != null) {
                System.err.println(t);
            }
        } finally {
            releaseQueueUsage();
        }
    }

    private long beforeSchedule(Runnable runnable, long delay) {
        try {
            return getQueuePermitAndModifiedDelay(delay);
        } catch (InterruptedException e) {
            getRejectedExecutionHandler().rejectedExecution(runnable, this);
            return 0;
        }
    }

    private long beforeSchedule(Callable callable, long delay) {
        try {
            return getQueuePermitAndModifiedDelay(delay);
        } catch (InterruptedException e) {
            getRejectedExecutionHandler().rejectedExecution(new FutureTask(callable), this);
            return 0;
        }
    }

    private long getQueuePermitAndModifiedDelay(long delay) throws InterruptedException {
        final long beforeAcquireTimeStamp = System.currentTimeMillis();
        maxQueueSize.tryAcquire(delay, TimeUnit.MILLISECONDS);
        final long afterAcquireTimeStamp = System.currentTimeMillis();
        return afterAcquireTimeStamp - beforeAcquireTimeStamp;
    }

    private void releaseQueueUsage() {
        maxQueueSize.release();
    }
}
