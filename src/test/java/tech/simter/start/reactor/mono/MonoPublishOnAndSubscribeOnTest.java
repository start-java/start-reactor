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
 * @author RJ
 */
@TestMethodOrder(OrderAnnotation.class)
class MonoPublishOnAndSubscribeOnTest {
  private Logger logger = LoggerFactory.getLogger(MonoPublishOnAndSubscribeOnTest.class);

  // by default, all operator run on Main-Thread
  @Test
  @Order(10)
  void defaultBehavior() {
    long mainThreadId = Thread.currentThread().getId();
    StepVerifier.create(
      Mono.just(mainThreadId)
        .map(it -> Tuples.of(it, Thread.currentThread().getId()))
        .map(it -> Tuples.of(it.getT1(), it.getT2(), Thread.currentThread().getId()))
    ).expectNext(Tuples.of(mainThreadId, mainThreadId, mainThreadId))
      .verifyComplete();
  }

  // `subscribeOn` control which thread the previous/next operator running on until to the next occurrence of a `publishOn`.
  @Test
  @Order(20)
  void subscribeOn() throws InterruptedException {
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

  // only the first `subscribeOn` take influence
  @Test
  @Order(21)
  void subscribeOn_subscribeOn() throws InterruptedException {
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
      logger.debug("mainThreadId={}, subThreadId={}, operatorThreadIds={}", mainThreadId,
        Arrays.stream(st).map(s -> s.threadId.toString()).collect(Collectors.joining(",")),
        it.toString());
      assertAll(
        () -> assertEquals(st[0].threadId, it.getT1(), "T1"), // by subscribeOn(st[0].scheduler)
        () -> assertEquals(st[0].threadId, it.getT2(), "T2"), // by subscribeOn(st[0].scheduler)
        () -> assertEquals(st[0].threadId, it.getT3(), "T3")  // by subscribeOn(st[0].scheduler)
      );
    }).verifyComplete();
  }

  // `subscribeOn` take influence until to the next occurrence of a `publishOn`.
  @Test
  @Order(22)
  void subscribeOn_publishOn() throws InterruptedException {
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
      logger.debug("mainThreadId={}, subThreadId={}, operatorThreadIds={}", mainThreadId,
        Arrays.stream(st).map(s -> s.threadId.toString()).collect(Collectors.joining(",")),
        it.toString());
      assertAll(
        () -> assertEquals(st[0].threadId, it.getT1(), "T1"), // by subscribeOn(st[0].scheduler)
        () -> assertEquals(st[0].threadId, it.getT2(), "T2"), // by subscribeOn(st[0].scheduler)
        () -> assertEquals(st[1].threadId, it.getT3(), "T3")  // by publishOn(st[1].scheduler)
      );
    }).verifyComplete();
  }


  // `subscribeOn` control which thread the previous/next operator running on until to the next occurrence of a `publishOn`.
  @Test
  @Order(30)
  void publishOn() throws InterruptedException {
    Long mainThreadId = Thread.currentThread().getId();

    // create three different single-thread scheduler for test
    SingleThreadScheduler st = SingleThreadScheduler.createOne();
    assertNotEquals(mainThreadId, st.threadId);

    StepVerifier.create(
      // record all thread id that operator running on
      Mono.just(mainThreadId)
        .map(it -> Thread.currentThread().getId())                                                            // T1
        .publishOn(st.scheduler) // publish on another thread
        .map(it -> Tuples.of(it, Thread.currentThread().getId()))                                             // T2
    ).consumeNextWith(it -> {
      logger.debug("mainThreadId={}, subThreadId={}, operatorThreadIds={}", mainThreadId, st.threadId, it.toString());
      assertAll(
        () -> assertEquals(mainThreadId, it.getT1(), "T1"),
        () -> assertEquals(st.threadId, it.getT2(), "T2")
      );
    }).verifyComplete();
  }

  // only the first `subscribeOn` take influence
  @Test
  @Order(31)
  void publishOn_publishOn() throws InterruptedException {
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
        .publishOn(st[0].scheduler) // publish on another thread
        .map(it -> Tuples.of(it, Thread.currentThread().getId()))                                             // T2
        .publishOn(st[1].scheduler) // publish on another thread
        .map(it -> Tuples.of(it.getT1(), it.getT2(), Thread.currentThread().getId()))                         // T3
    ).consumeNextWith(it -> {
      logger.debug("mainThreadId={}, subThreadId={}, operatorThreadIds={}", mainThreadId,
        Arrays.stream(st).map(s -> s.threadId.toString()).collect(Collectors.joining(",")),
        it.toString());
      assertAll(
        () -> assertEquals(mainThreadId, it.getT1(), "T1"),
        () -> assertEquals(st[0].threadId, it.getT2(), "T2"), // by publishOn(st[0].scheduler)
        () -> assertEquals(st[1].threadId, it.getT3(), "T3")  // by publishOn(st[1].scheduler)
      );
    }).verifyComplete();
  }

  // publishOn downward prior to subscribeOn, until next to meet another publishOn
  @Test
  @Order(40)
  void publishOn_subscribeOn_publishOn() throws InterruptedException {
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
        .subscribeOn(st[1].scheduler)
        .map(it -> Tuples.of(it.getT1(), it.getT2(), Thread.currentThread().getId()))                         // T3
        .publishOn(st[2].scheduler)
        .map(it -> Tuples.of(it.getT1(), it.getT2(), it.getT3(), Thread.currentThread().getId()))             // T4
    ).consumeNextWith(it -> {
      logger.debug("mainThreadId={}, subThreadId={}, operatorThreadIds={}", mainThreadId,
        Arrays.stream(st).map(s -> s.threadId.toString()).collect(Collectors.joining(",")),
        it.toString());
      assertAll(
        () -> assertEquals(st[1].threadId, it.getT1(), "T1"), // by subscribeOn(st[1].scheduler)
        () -> assertEquals(st[0].threadId, it.getT2(), "T2"), // by publishOn(st[0].scheduler)
        () -> assertEquals(st[0].threadId, it.getT3(), "T3"), // by publishOn(st[0].scheduler)
        () -> assertEquals(st[2].threadId, it.getT4(), "T4")  // by publishOn(st[2].scheduler)
      );
    }).verifyComplete();
  }
}