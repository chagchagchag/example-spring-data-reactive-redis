package io.chagchagchag.example.example_reactive_redis.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ResourceLoader;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@AutoConfiguration(after = RedisAutoConfiguration.class)
public class ReactiveRedisConfig {

  @Value("${spring.data.redis.host}")
  private String host;

  @Value("${spring.data.redis.port}")
  private int port;

  @Bean(name = "reactiveRedisConnectionFactory")
  public ReactiveRedisConnectionFactory reactiveRedisConnectionFactory(){
    return new LettuceConnectionFactory(host, port);
  }

  @Bean(name = "reactiveRedisTemplate")
  @ConditionalOnBean(ReactiveRedisConnectionFactory.class)
  public ReactiveRedisTemplate<Object, Object> reactiveRedisTemplate(
      ReactiveRedisConnectionFactory reactiveRedisConnectionFactory,
      ResourceLoader resourceLoader
  ){
    JdkSerializationRedisSerializer jdkSerializer = new JdkSerializationRedisSerializer(
        resourceLoader.getClassLoader()
    );

    RedisSerializationContext<Object, Object> serializationContext = RedisSerializationContext
        .newSerializationContext()
        .key(jdkSerializer).value(jdkSerializer)
        .hashKey(jdkSerializer).hashValue(jdkSerializer)
        .build();

    return new ReactiveRedisTemplate<>(reactiveRedisConnectionFactory, serializationContext);
  }

  @Bean(name = "reactiveStringRedisTemplate")
  @ConditionalOnBean(ReactiveRedisConnectionFactory.class)
  public ReactiveStringRedisTemplate reactiveStringRedisTemplate(
      ReactiveRedisConnectionFactory reactiveRedisConnectionFactory
  ){
    return new ReactiveStringRedisTemplate(reactiveRedisConnectionFactory);
  }

  @Bean(name = "bookHashTemplate")
  @ConditionalOnBean(ReactiveRedisConnectionFactory.class)
  public ReactiveRedisTemplate<String, String> reactiveRedisTemplate(
      ReactiveRedisConnectionFactory reactiveRedisConnectionFactory
  ){
    StringRedisSerializer stringSerializer = new StringRedisSerializer();

    RedisSerializationContext<String, String> serializationContext =
        RedisSerializationContext
            .<String, String>newSerializationContext()
            .key(stringSerializer).value(stringSerializer)
            .hashKey(stringSerializer).hashValue(stringSerializer)
            .build();

    return new ReactiveRedisTemplate<>(reactiveRedisConnectionFactory, serializationContext);
  }

  @Bean(name = "bookSortedSetRedisTemplate")
  @ConditionalOnBean(ReactiveRedisConnectionFactory.class)
  public ReactiveRedisTemplate<String, String> bookSortedSetRedisTemplate(
      ReactiveRedisConnectionFactory reactiveRedisConnectionFactory
  ){
    StringRedisSerializer stringSerializer = new StringRedisSerializer();
    Jackson2JsonRedisSerializer valueSerializer = new Jackson2JsonRedisSerializer<>(String.class);

    RedisSerializationContext<String, String> serializationContext =
        RedisSerializationContext
            .<String, String>newSerializationContext()
            .key(stringSerializer).value(valueSerializer)
            .hashKey(stringSerializer).hashValue(valueSerializer)
            .build();

    return new ReactiveRedisTemplate<>(reactiveRedisConnectionFactory, serializationContext);
  }
}
