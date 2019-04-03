package tech.simter.start.reactor.mono;

import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.function.Tuples;
import tech.simter.start.reactor.scheduler.SingleThreadScheduler;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * `subscribeOn` control which thread the previous/next operator running on until to the next occurrence of a `publishOn`.
 *
 * @author RJ
 */
@TestMethodOrder(OrderAnnotation.class)
class SubscribeOnTest {
  private Logger logger = LoggerFactory.getLogger(SubscribeOnTest.class);

  /**
   * influence the upstream
   */
  @Test
  @Order(1)
  void influenceUpStream() throws InterruptedException {
    Long mainThreadId = Thread.currentThread().getId();

    // create three different single-thread scheduler for test
    SingleThreadScheduler st = SingleThreadScheduler.createOne();
    assertNotEquals(mainThreadId, st.threadId);

    StepVerifier.create(
      // record all thread id that operator running on
      Mono.just(mainThreadId)
        .map(it -> Thread.currentThread().getId())                                                            // T1
        .subscribeOn(st.scheduler) // subscribe on another thread
        .map(it -> Tuples.of(it, Thread.currentThread().getId()))                                             // T2
    ).consumeNextWith(it -> {
      logger.debug("mainThreadId={}, subThreadId={}, operatorThreadIds={}", mainThreadId, st.threadId, it.toString());
      assertAll(
        () -> assertEquals(st.threadId, it.getT1(), "T1"),
        () -> assertEquals(st.threadId, it.getT2(), "T2")
      );
    }).verifyComplete();
  }

  /**
   * only the first subscribeOn take influence
   */
  @Test
  @Order(2)
  void onlyTheFirstSubscribeOnValid() throws InterruptedException {
    Long mainThreadId = Thread.currentThread().getId();

    // create three different single-thread scheduler for test
    SingleThreadScheduler[] st = SingleThreadScheduler.createMany(2);
    assertNotEquals(mainThreadId, st[0].threadId);
    assertNotEquals(mainThreadId, st[1].threadId);
    assertNotEquals(st[0].threadId, st[1].threadId);

    StepVerifier.create(
      // record all thread id that operator running on
      Mono.just(mainThreadId)
        .map(it -> Thread.currentThread().getId())                                                            // T1
        .subscribeOn(st[0].scheduler) // subscribe on another thread
        .map(it -> Tuples.of(it, Thread.currentThread().getId()))                                             // T2
        .subscribeOn(st[1].scheduler) // would be ignore
        .map(it -> Tuples.of(it.getT1(), it.getT2(), Thread.currentThread().getId()))                         // T3
    ).consumeNextWith(it -> {
      logger.debug("mainThreadId={}, subThreadIds={}, operatorThreadIds={}", mainThreadId,
        Arrays.stream(st).map(s -> s.threadId.toString()).collect(Collectors.joining(",")),
        it.toString());
      assertAll(
        () -> assertEquals(st[0].threadId, it.getT1(), "T1"), // by subscribeOn(st[0].scheduler)
        () -> assertEquals(st[0].threadId, it.getT2(), "T2"), // by subscribeOn(st[0].scheduler)
        () -> assertEquals(st[0].threadId, it.getT3(), "T3")  // by subscribeOn(st[0].scheduler)
      );
    }).verifyComplete();
  }

  /**
   * subscribeOn take influence until the next occurrence of publishOn
   */
  @Test
  @Order(3)
  void influenceUpStreamUntilNextPublishOn() throws InterruptedException {
    Long mainThreadId = Thread.currentThread().getId();

    // create three different single-thread scheduler for test
    SingleThreadScheduler[] st = SingleThreadScheduler.createMany(2);
    assertNotEquals(mainThreadId, st[0].threadId);
    assertNotEquals(mainThreadId, st[1].threadId);
    assertNotEquals(st[0].threadId, st[1].threadId);

    StepVerifier.create(
      // record all thread id that operator running on
      Mono.just(mainThreadId)
        .map(it -> Thread.currentThread().getId())                                                            // T1
        .subscribeOn(st[0].scheduler) // subscribe on another thread
        .map(it -> Tuples.of(it, Thread.currentThread().getId()))                                             // T2
        .publishOn(st[1].scheduler)   // publish on another thread
        .map(it -> Tuples.of(it.getT1(), it.getT2(), Thread.currentThread().getId()))                         // T3
    ).consumeNextWith(it -> {
      logger.debug("mainThreadId={}, subThreadIds={}, operatorThreadIds={}", mainThreadId,
        Arrays.stream(st).map(s -> s.threadId.toString()).collect(Collectors.joining(",")),
        it.toString());
      assertAll(
        () -> assertEquals(st[0].threadId, it.getT1(), "T1"), // by subscribeOn(st[0].scheduler)
        () -> assertEquals(st[0].threadId, it.getT2(), "T2"), // by subscribeOn(st[0].scheduler)
        () -> assertEquals(st[1].threadId, it.getT3(), "T3")  // by publishOn(st[1].scheduler)
      );
    }).verifyComplete();
  }

  /**
   * subscribeOn influence upstream before any publishOn.
   * <p>
   * Code is same with {@link PublishOnTest#downwardStrongerThanSubscribeOn()}.
   */
  @Test
  @Order(4)
  void influenceUpStreamBeforeAnyPublishOn() throws InterruptedException {
    Long mainThreadId = Thread.currentThread().getId();

    // create three different single-thread scheduler for test
    SingleThreadScheduler[] st = SingleThreadScheduler.createMany(3);
    assertAll(
      () -> assertNotEquals(st[0].scheduler, st[1].scheduler, "0≠1"),
      () -> assertNotEquals(st[0].scheduler, st[2].scheduler, "0≠2"),
      () -> assertNotEquals(st[1].scheduler, st[2].scheduler, "1≠2")
    );

    StepVerifier.create(
      // record all thread id that operator running on
      Mono.just(mainThreadId)
        .map(it -> Thread.currentThread().getId())                                                            // T1
        .publishOn(st[0].scheduler)
        .map(it -> Tuples.of(it, Thread.currentThread().getId()))                                             // T2
        .publishOn(st[1].scheduler)
        .map(it -> Tuples.of(it.getT1(), it.getT2(), Thread.currentThread().getId()))                         // T3
        .subscribeOn(st[2].scheduler)
        .map(it -> Tuples.of(it.getT1(), it.getT2(), it.getT3(), Thread.currentThread().getId()))             // T4
    ).consumeNextWith(it -> {
      logger.debug("mainThreadId={}, subThreadIds={}, operatorThreadIds={}", mainThreadId,
        Arrays.stream(st).map(s -> s.threadId.toString()).collect(Collectors.joining(",")),
        it.toString());
      assertAll(
        () -> assertEquals(st[2].threadId, it.getT1(), "T1"), // by subscribeOn(st[2].scheduler)
        () -> assertEquals(st[0].threadId, it.getT2(), "T2"), // by publishOn(st[0].scheduler)
        () -> assertEquals(st[1].threadId, it.getT3(), "T3"), // by publishOn(st[1].scheduler)
        () -> assertEquals(st[1].threadId, it.getT4(), "T4")  // by publishOn(st[1].scheduler)
      );
    }).verifyComplete();
  }

  /**
   * Logic is same with {@link SubscribeOnTest#influenceUpStreamBeforeAnyPublishOn()}.
   */
  @Test
  @Order(5)
  void doOnNextVersion() throws InterruptedException {
    Long mainThreadId = Thread.currentThread().getId();

    // create three different single-thread scheduler for test
    SingleThreadScheduler[] st = SingleThreadScheduler.createMany(3);
    assertAll(
      () -> assertNotEquals(st[0].scheduler, st[1].scheduler, "0≠1"),
      () -> assertNotEquals(st[0].scheduler, st[2].scheduler, "0≠2"),
      () -> assertNotEquals(st[1].scheduler, st[2].scheduler, "1≠2")
    );

    final Long[] operatorThreadIds = new Long[4];
    StepVerifier.create(
      // record all thread id that operator running on
      Mono.just(mainThreadId)
        .doOnNext(it -> operatorThreadIds[0] = Thread.currentThread().getId())             // 0
        .publishOn(st[0].scheduler)
        .doOnNext(it -> operatorThreadIds[1] = Thread.currentThread().getId())             // 1
        .publishOn(st[1].scheduler)
        .doOnNext(it -> operatorThreadIds[2] = Thread.currentThread().getId())             // 2
        .subscribeOn(st[2].scheduler)
        .doOnNext(it -> operatorThreadIds[3] = Thread.currentThread().getId())             // 3
    ).consumeNextWith(it -> {
      logger.debug("subThreadIds={}, operatorThreadIds={}",
        Arrays.stream(st).map(s -> s.threadId.toString()).collect(Collectors.joining(",")),
        Arrays.stream(operatorThreadIds).map(Object::toString).collect(Collectors.joining(",")));
      assertAll(
        () -> assertEquals(st[2].threadId, operatorThreadIds[0], "0"), // by subscribeOn(st[2].scheduler)
        () -> assertEquals(st[0].threadId, operatorThreadIds[1], "1"), // by publishOn(st[0].scheduler)
        () -> assertEquals(st[1].threadId, operatorThreadIds[2], "2"), // by publishOn(st[1].scheduler)
        () -> assertEquals(st[1].threadId, operatorThreadIds[3], "3")  // by publishOn(st[1].scheduler)
      );
    }).verifyComplete();
  }

  @Test
  @Order(6)
  void nestedMonoDefaultRunningOnParentScheduler() throws InterruptedException {
    SingleThreadScheduler st = SingleThreadScheduler.createOne();

    final Long[] operatorThreadIds = new Long[3];
    StepVerifier.create(
      Mono.just(0)
        .doOnNext(it -> operatorThreadIds[0] = Thread.currentThread().getId())
        .then(Mono.just(0).doOnNext(it -> operatorThreadIds[1] = Thread.currentThread().getId()))           // nested 1
        .flatMap(it -> Mono.just(0).doOnNext(it2 -> operatorThreadIds[2] = Thread.currentThread().getId())) // nested 2
        .subscribeOn(st.scheduler)
    ).consumeNextWith(it -> {
      logger.debug("subThreadId={}, operatorThreadIds={}", st.threadId,
        Arrays.stream(operatorThreadIds).map(Object::toString).collect(Collectors.joining(",")));
      assertAll(
        () -> assertEquals(st.threadId, operatorThreadIds[0], "0"), // by subscribeOn(st.scheduler)
        () -> assertEquals(st.threadId, operatorThreadIds[1], "1"), // by subscribeOn(st.scheduler)
        () -> assertEquals(st.threadId, operatorThreadIds[2], "2")  // by subscribeOn(st.scheduler)
      );
    }).verifyComplete();
  }

  @Test
  @Order(7)
  void parentSubscribeOnNotInfluenceNested() throws InterruptedException {
    SingleThreadScheduler[] st = SingleThreadScheduler.createMany(3);
    final Long[] operatorThreadIds = new Long[3];
    StepVerifier.create(
      Mono.just(0)
        .doOnNext(it -> operatorThreadIds[0] = Thread.currentThread().getId())
        .then(Mono.just(0)
          .doOnNext(it -> operatorThreadIds[1] = Thread.currentThread().getId())
          .subscribeOn(st[1].scheduler) // nested 1
        )
        .flatMap(it -> Mono.just(0)
          .doOnNext(it2 -> operatorThreadIds[2] = Thread.currentThread().getId())
          .subscribeOn(st[2].scheduler) // nested 2
        ).subscribeOn(st[0].scheduler)
    ).consumeNextWith(it -> {
      logger.debug("subThreadIds={}, operatorThreadIds={}",
        Arrays.stream(st).map(s -> s.threadId.toString()).collect(Collectors.joining(",")),
        Arrays.stream(operatorThreadIds).map(Object::toString).collect(Collectors.joining(",")));
      assertAll(
        () -> assertEquals(st[0].threadId, operatorThreadIds[0], "0"), // by subscribeOn(st[0].scheduler)
        () -> assertEquals(st[1].threadId, operatorThreadIds[1], "1"), // by subscribeOn(st[1].scheduler)
        () -> assertEquals(st[2].threadId, operatorThreadIds[2], "2")  // by subscribeOn(st[2].scheduler)
      );
    }).verifyComplete();
  }
}