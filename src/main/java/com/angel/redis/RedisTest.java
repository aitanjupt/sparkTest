package com.angel.redis;

import org.redisson.Config;
import org.redisson.Redisson;
import org.redisson.core.RList;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by dell on 2015/11/11.
 */
public class RedisTest {

    public static void main(String[] args) throws IOException, URISyntaxException {
        // 1.初始化
        Config config = new Config();
        config.useSingleServer().setAddress("192.168.181.198:6379");
        //.setConnectionPoolSize(5);
        Redisson redisson = Redisson.create(config);
        System.out.println("reids连接成功...");

        test1(redisson);

        RList<String> myList = redisson.getList("myList");
        if (myList != null) {
            myList.clear();
        }

        myList.add(new String("A"));
        myList.add(new String("B"));
        myList.add(new String("C"));

        RList<String> myListCache = redisson.getList("myList");

        for (String bean : myListCache) {
            System.out.println(bean);
        }

        // 关闭连接
        redisson.shutdown();
    }

    private static void test1(Redisson redisson) {
        // 2.测试concurrentMap,put方法的时候就会同步到redis中
        ConcurrentMap<String, Object> map = redisson.getMap("FirstMap");
        map.put("wuguowei", "男");
        map.put("zhangsan", "nan");
        map.put("lisi", "女");

        ConcurrentMap resultMap = redisson.getMap("FirstMap");
        System.out.println("resultMap==" + resultMap.keySet());

        // 2.测试Set集合
        Set mySet = redisson.getSet("MySet");
        mySet.add("wuguowei");
        mySet.add("lisi");

        Set resultSet = redisson.getSet("MySet");
        System.out.println("resultSet===" + resultSet.size());

        //3.测试Queue队列
        Queue myQueue = redisson.getQueue("FirstQueue");
        myQueue.add("wuguowei");
        myQueue.add("lili");
        myQueue.add("zhangsan");
        myQueue.peek();
        myQueue.poll();

        Queue resultQueue = redisson.getQueue("FirstQueue");
        System.out.println("resultQueue===" + resultQueue);
    }
}
