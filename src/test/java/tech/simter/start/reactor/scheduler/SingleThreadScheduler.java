package tech.simter.start.reactor.scheduler;

import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

/**
 * Create Single-Thread Scheduler and get its thread id.
 *
 * @author RJ
 */
public class SingleThreadScheduler {
  public Scheduler scheduler;
  public Long threadId;

  private SingleThreadScheduler(Scheduler scheduler, Long threadId) {
    this.scheduler = scheduler;
    this.threadId = threadId;
  }

  public static SingleThreadScheduler[] createMany(int count) throws InterruptedException {
    CountDownLatch cdl = new CountDownLatch(count);
    final SingleThreadScheduler[] s = new SingleThreadScheduler[count];
    for (int i = 0; i < count; i++) {
      Scheduler scheduler = Schedulers.fromExecutor(Executors.newSingleThreadExecutor());
      s[i] = new SingleThreadScheduler(scheduler, 0L);
      final int j = i;
      scheduler.schedule(() -> {
        s[j].threadId = Thread.currentThread().getId();
        cdl.countDown();
      });
    }
    cdl.await();
    return s;
  }

  public static SingleThreadScheduler createOne() throws InterruptedException {
    CountDownLatch cdl = new CountDownLatch(1);
    Scheduler scheduler = Schedulers.fromExecutor(Executors.newSingleThreadExecutor());
    final Long[] subThreadId = new Long[1];
    scheduler.schedule(() -> {
      subThreadId[0] = Thread.currentThread().getId();
      cdl.countDown();
    });
    cdl.await();
    return new SingleThreadScheduler(scheduler, subThreadId[0]);
  }
}