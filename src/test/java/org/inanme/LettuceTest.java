package org.inanme;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisFuture;
import com.lambdaworks.redis.api.StatefulRedisConnection;
import com.lambdaworks.redis.api.async.RedisAsyncCommands;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * STRING   : SET, GET, INCR, DECR
 * LIST     : LPUSH, RPUSH, LPOP, RPOP, LRANGE, LINDEX
 * SET      : SADD, SREM, SMEMBERS, SISMEMBER, SINTER, SUNION, SDIFF, SCARD
 * HASH     : HSET, HGET, HGETALL
 * ZSET     : ZADD, ZREM, ZRANGEBYSCORE
 */
public class LettuceTest {

    @Test
    public void set_get() throws ExecutionException, InterruptedException {
        RedisClient client = RedisClient.create("redis://localhost");
        StatefulRedisConnection<String, String> connection = client.connect();
        RedisAsyncCommands<String, String> redis = connection.async();

        RedisFuture<String> set = redis.set("key", "value");
        RedisFuture<String> get = redis.get("key");

        set.thenCompose(__ -> get).thenAccept(System.out::println);

        CompletableFuture.allOf(set.toCompletableFuture(), get.toCompletableFuture())
                .thenRun(() -> {
                    try {
                        assertThat("OK", is(set.get()));
                        assertThat("value", is(get.get()));
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    }
                })
                .thenRun(connection::close)
                .exceptionally(th -> {
                    th.printStackTrace();
                    return null;
                })
                .get();
    }

    @Test
    public void lpush() throws InterruptedException {
        RedisClient client = RedisClient.create("redis://localhost");
        StatefulRedisConnection<String, String> connection = client.connect();
        RedisAsyncCommands<String, String> redis = connection.async();

        CompletionStage<Long> lpush1 = redis.lpush("rangekey", "val1");
        CompletionStage<Long> lpush2 = redis.lpush("rangekey", "val2");
        CompletionStage<Long> lpush3 = redis.lpush("rangekey", "val3");

        CompletableFuture.allOf(lpush1.toCompletableFuture(), lpush2.toCompletableFuture(), lpush3.toCompletableFuture())
                .thenCompose(__ -> redis.lrange(value -> System.out.println("Value: " + value), "rangekey", 0, -1))
                .exceptionally(th -> {
                    th.printStackTrace();
                    return null;
                });

        TimeUnit.SECONDS.sleep(2);
    }

}
