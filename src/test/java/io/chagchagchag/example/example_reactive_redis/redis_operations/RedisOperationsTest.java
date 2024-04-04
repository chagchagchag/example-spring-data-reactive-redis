package io.chagchagchag.example.example_reactive_redis.redis_operations;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.connection.stream.StringRecord;
import org.springframework.data.redis.core.DefaultTypedTuple;
import org.springframework.data.redis.core.ReactiveHashOperations;
import org.springframework.data.redis.core.ReactiveHyperLogLogOperations;
import org.springframework.data.redis.core.ReactiveListOperations;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveStreamOperations;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.ReactiveValueOperations;
import org.springframework.data.redis.core.ReactiveZSetOperations;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;

@SpringBootTest
public class RedisOperationsTest {
  private static final Logger log = LoggerFactory.getLogger(RedisOperationsTest.class);

  @Autowired
  private ReactiveStringRedisTemplate reactiveStringRedisTemplate;

  @Autowired
  private ReactiveRedisTemplate<Object, Object> reactiveRedisTemplate;

  @Autowired
  private ReactiveRedisTemplate<String, String> bookHashTemplate;

  @Autowired
  private ReactiveRedisTemplate<String, String> bookSortedSetRedisTemplate;

  @DisplayName("간단한_ReactiveValueOperations_를_돌려봐요")
  @Test
  public void TEST_간단한_ReactiveValueOperations_를_돌려봐요(){
    // given

    // when

    // then
    ReactiveValueOperations<String, String> valueOperations = reactiveStringRedisTemplate.opsForValue();
    var bookNameWithIdKey = "book:1:name";
    var bookPriceWithIdKey = "book:1:price";

    Long result = valueOperations.set(bookNameWithIdKey, "바람과 함께 사라지다")
        .then(valueOperations.set(bookPriceWithIdKey, "13000"))
        .then(valueOperations.setIfAbsent(bookNameWithIdKey, "undefined"))
        .doOnNext(value -> log.info("onNext :: {}", value))
        .thenMany(valueOperations.multiGet(List.of(bookNameWithIdKey, bookPriceWithIdKey)))
        .doOnNext(value -> log.info("onNext :: {}", value))
        .then(valueOperations.increment(bookPriceWithIdKey, 1000))
        .doOnNext(value -> log.info("onNext :: {}", value))
        .block();

    log.info("result = {}", result);
  }

  @DisplayName("간단한_ReactiveListOperations_를_돌려봐요")
  @Test
  public void TEST_간단한_ReactiveListOperations_를_돌려봐요(){
    // given

    // when

    // then
    ReactiveListOperations<Object, Object> listOperations = reactiveRedisTemplate.opsForList();
    var queueWithId = "QUEUE###1";

    listOperations
        .leftPush(queueWithId, "33000")
        .then(listOperations.leftPush(queueWithId, "34000"))
        .then(listOperations.size(queueWithId))
        .doOnNext(value -> log.info("doOnNext = {}", value))
        .then(listOperations.rightPop(queueWithId))
        .doOnNext(value -> log.info("doOnNext = {}", value))
        .then(listOperations.rightPop(queueWithId))
        .doOnNext(value -> log.info("doOnNext = {}", value))
        .block();
  }

  @DisplayName("간단한_ReactiveHashOperations_를_돌려봐요")
  @Test
  public void TEST_간단한_ReactiveHashOperations_를_돌려봐요(){
    // given

    // when

    // then
    ReactiveHashOperations<String, Object, Object> hashOperations = bookHashTemplate.opsForHash();
    var hashWithId = "BOOK###1";
    var properties = Map.of("name", "바람과 함께 사라지다", "price", "2000");


    hashOperations
        .putAll(hashWithId, properties)
        .thenMany(hashOperations.values(hashWithId))
        .doOnNext(value -> log.info("doOnNext = {}", value))
        .then(hashOperations.size(hashWithId))
        .doOnNext(value -> log.info("doOnNext = {}", value))
        .then(hashOperations.increment(hashWithId, "price", 100.0))
        .thenMany(hashOperations.multiGet(hashWithId, List.of("name", "price")))
        .doOnNext(value -> log.info("doOnNext = {}", value))
        .then(hashOperations.remove(hashWithId, "price"))
        .block();
  }

  @DisplayName("간단한_ReactiveZSetOperations_를_돌려봐요")
  @Test
  public void TEST_간단한_ReactiveZSetOperations_를_돌려봐요(){
    // given

    // when

    // then
    var zSetWithId = "BOOK:1";
    ReactiveZSetOperations<String, String> zSetOperations = bookSortedSetRedisTemplate.opsForZSet();
    bookSortedSetRedisTemplate.delete(zSetWithId);

    List<TypedTuple<String>> tuples = new ArrayList<>();
    tuples.add(new DefaultTypedTuple<>("Paramount", 1.0));
    tuples.add(new DefaultTypedTuple<>("Apple", 1.1));
    tuples.add(new DefaultTypedTuple<>("Manning", 1.3));

    zSetOperations
        .addAll(zSetWithId, tuples)
        .doOnError(err -> log.info("err = {}", err.getMessage()))
        .then(zSetOperations.remove(zSetWithId, "Paramount"))
        .doOnNext(value -> log.info("doOnNext = {}", value))
        .then(zSetOperations.size(zSetWithId))
        .doOnNext(value -> log.info("doOnNext = {}", value))
        .thenMany(zSetOperations.rangeWithScores(zSetWithId, Range.closed(0L, -1L)))
        .doOnNext(value -> log.info("doOnNext = {}", value))
        .then(zSetOperations.rank(zSetWithId, "Manning"))
        .doOnNext(value -> log.info("doOnNext = {}", value))
        .block();
  }

  @DisplayName("간단한_ReactiveStreamOperations_를_돌려봐요")
  @Test
  public void TEST_간단한_ReactiveStreamOperations_를_돌려봐요(){
    // given

    // when

    // then
    var streamName = "stream:1";
    ReactiveStreamOperations<String, Object, Object> streamOperations = bookSortedSetRedisTemplate.opsForStream();

    streamOperations
        .read(StreamReadOptions.empty().block(Duration.ofSeconds(10)).count(2), StreamOffset.latest(streamName))
        .doOnSubscribe(subscription -> {
          log.info("subscribe");
        })
        .doOnNext(data -> {
          log.info("data = " + data);
        })
        .subscribe(message -> {
          log.info("message = {}", message);
        });

    try{
      Thread.sleep(1000);
      streamOperations
          .add(streamName, Map.of("1", "100", "2", "200"))
          .block();
    }
    catch (Exception e){
      e.printStackTrace();
    }
  }

  @DisplayName("간단한_ReactiveHyperLogLogOperations_를_돌려봐요")
  @Test
  public void TEST_간단한_ReactiveHyperLogLogOperations_를_돌려봐요(){
    // given

    // when

    // then
    ReactiveHyperLogLogOperations<String, String> hyperLogLogOperations = bookSortedSetRedisTemplate.opsForHyperLogLog();
    var key = "B:300";

    hyperLogLogOperations
        .add(key, "1","2","3","4","5")
        .then(hyperLogLogOperations.size(key))
        .doOnNext(value -> log.info("doOnNext = {}", value))
        .then(hyperLogLogOperations.add(key, "1","2","3","4","5","6","7","8","9","10"))
        .then(hyperLogLogOperations.size(key))
        .doOnNext(value -> log.info("doOnNext = {}", value))
        .block();
  }
}
