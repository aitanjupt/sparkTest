package com.angel.redis;

import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Created by dell on 2015/11/13.
 */
public class JedisTest {
    public static void main(String[] args) throws IOException, URISyntaxException {
        Jedis jedis = new Jedis("192.168.181.198", 6579);
        jedis.set("name", "xinxin");//向key-->name中放入了value-->xinxin
        System.out.println(jedis.get("name"));//执行结果：xinxin
    }
}
