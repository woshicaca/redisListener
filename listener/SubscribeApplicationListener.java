package com.itwobyte.download.listener;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.itwobyte.common.utils.DateUtils;
import com.itwobyte.download.annotation.RedisListListener;
import com.itwobyte.download.annotation.RedisListenClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.redisson.Redisson;
import org.redisson.RedissonShutdownException;
import org.redisson.api.*;
import org.redisson.config.Config;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * redis订阅通知 通过redis的list实现
 */
@Slf4j
@Component
public class SubscribeApplicationListener implements ApplicationListener<ContextRefreshedEvent>, DisposableBean {

    private ExecutorService executorService;


    @Autowired
    private RedissonClient redissonClient;
    private static Map<String, List<ObjectAndMethod>> observers = new HashMap<>();

    public SubscribeApplicationListener() {
        executorService = Executors.newCachedThreadPool();
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        ApplicationContext applicationContext = event.getApplicationContext();
        String[] beanDefinitionNames = applicationContext.getBeanDefinitionNames();
        RKeys keys = redissonClient.getKeys();
        //查找容器中所有被注解的监听方法
        for (String beanDefinitionName : beanDefinitionNames) {
            Object bean = applicationContext.getBean(beanDefinitionName);
            Class<?> aClass = bean.getClass();
            RedisListenClass listenClass = aClass.getAnnotation(RedisListenClass.class);
            if (listenClass == null) {
                continue;
            }
            Method[] methods = aClass.getMethods();
            for (Method method : methods) {
                RedisListListener annotation = method.getAnnotation(RedisListListener.class);
                if (annotation != null) {
                    String channel = StringUtils.isBlank(annotation.channel()) ? "*" : annotation.channel();
                    Iterable<String> keysByPattern = keys.getKeysByPattern(channel+":*");
                    for (String s : keysByPattern) {
                        RBlockingQueue<Object> blockingQueue = redissonClient.getBlockingQueue(s);
                        while (!blockingQueue.isEmpty()){
                            Object o = blockingQueue.pollLastAndOfferFirstTo(channel);
                            System.out.println(o);
                        }
                    }
                    int errorTimes = annotation.errorTimes();
                    List<ObjectAndMethod> objects = observers.computeIfAbsent(channel, k -> new ArrayList<>());
                    Method errorHandler = null;
                    if (StringUtils.isNotEmpty(annotation.errorHandler())) {
                        try {
                            errorHandler = aClass.getMethod(annotation.errorHandler(),String.class,String.class,Integer.class,Object.class,Exception.class);
                            log.info("添加一个失败处理器" + bean.getClass().getSimpleName() + "#" + method.getName() + ",通道：" + channel+",处理器为-------》"+bean.getClass().getSimpleName() + "#"+annotation.errorHandler());
                        } catch (NoSuchMethodException e) {
                           log.warn("未找到方法:"+annotation.errorHandler());
                           e.printStackTrace();
                           throw  new RuntimeException(e.getMessage());
                        }
                    }
                    objects.add(new ObjectAndMethod(bean, method,errorTimes,errorHandler));
                    log.info("添加一个订阅" + bean.getClass().getSimpleName() + "#" + method.getName() + ",通道：" + channel);
                    if(errorHandler==null){
                        log.info("通道：" + channel+"没有添加失败处理器,将使用默认处理器");
                    }
                }
            }
        }
        //添加订阅
        for (Map.Entry<String, List<ObjectAndMethod>> stringListEntry : observers.entrySet()) {
            String channel = stringListEntry.getKey();
            List<ObjectAndMethod> value = stringListEntry.getValue();
            executorService.execute(() -> {
                while (true) {
                    Object blpop = null;
                    String channel_bak = channel+ ":"+DateUtils.getNowDate().getTime();
                    try {

                        // 取出来先备份，处理成功后在删除
                        blpop = redissonClient.getBlockingQueue(channel).takeLastAndOfferFirstTo(channel_bak);
                        System.out.println(blpop);
                    } catch (InterruptedException e) {
                        log.debug("队列数据获取出错，通道为：" + channel);
                        throw new RuntimeException(e);
                    } catch (RedissonShutdownException exception) {
                        log.debug("已经断开与redis的连接，通道为：" + channel);
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }

                    }

                    for (ObjectAndMethod objectAndMethod : value) {
                        Object target = objectAndMethod.target;
                        Method method = objectAndMethod.method;
                        Method errorHandler = objectAndMethod.errorMethod;
                        int errorTimes = objectAndMethod.errorTimes;
                        try {
                            method.invoke(target, blpop);
                        } catch (IllegalAccessException | InvocationTargetException e) {
                            // 失败后处理
                            if(errorHandler!=null){
                                try {
                                    errorHandler.invoke(target,channel,channel_bak,errorTimes,blpop,e);
                                } catch (IllegalAccessException ex) {
                                    throw new RuntimeException(ex);
                                } catch (InvocationTargetException ex) {
                                    throw new RuntimeException(ex);
                                }
                            }else {
                                this.errorHandler(channel,channel_bak,errorTimes,blpop,e);
                            }

                            continue;
                        }

                        if(blpop!=null){
                            // 成功后删除
                            JSONObject jsonObject = JSON.parseObject(blpop.toString());
                            // 失败插入队尾部
                            RMapCache<Object, Object> mapCache = redissonClient.getMapCache("message_error");
                            Object primary = jsonObject.get("messageId");
                            if(mapCache.containsKey(primary.toString())){
                                mapCache.remove(primary.toString());
                            }
                        }
                        redissonClient.getBlockingQueue(channel_bak).poll();
                    }
                }
            });
        }
    }

    private void errorHandler(String channel,String back,Integer errorTimes,Object object,Exception e){
        JSONObject jsonObject = JSON.parseObject(object.toString());
        // 失败插入队尾部
        RMapCache<Object, Object> mapCache = redissonClient.getMapCache("message_error");
        Object primary = mapCache.get(jsonObject.get("messageId"));
        if (primary==null) {
            JSONObject json = new JSONObject();
            json.put("errorTimes",1);
            json.put("data",object);
            json.put("channel",channel);
            json.put("messageId",jsonObject.get("messageId"));
            if(e instanceof InvocationTargetException){
                json.put("errorMsg",((InvocationTargetException)e).getTargetException().getMessage());
            }

            mapCache.put(jsonObject.get("messageId"),json.toJSONString());
            System.out.println(json);
            Object o = redissonClient.getBlockingQueue(back).pollLastAndOfferFirstTo(channel);
        }else {
            JSONObject json = JSON.parseObject(primary.toString());
            if(Integer.parseInt(json.getString("errorTimes"))<=errorTimes){
                json.put("errorTimes",json.getIntValue("errorTimes")+1);
                mapCache.put(jsonObject.get("messageId"),json.toJSONString());
                System.out.println(json);
                Object o = redissonClient.getBlockingQueue(back).pollLastAndOfferFirstTo(channel);
            }else {
                redissonClient.getBlockingQueue(back).pollLastAndOfferFirstTo(channel+"_back");
            }
        }
    }


    @Override
    public void destroy() throws Exception {
        executorService.shutdown();
    }

    private void reConnect() {
        Config config = new Config();
        config.useSingleServer()
            .setAddress("redis://192.168.31.6:6379")
            .setDatabase(10);
        //获取客户端
        RedissonClient redissonClient = Redisson.create(config);
        System.out.println(redissonClient.getConfig());
        this.redissonClient = redissonClient;
    }

    static class ObjectAndMethod {
        Object target;
        Method method;

        Integer errorTimes;

        Method errorMethod;

        ObjectAndMethod(Object target, Method method,Integer errorTimes, Method errorMethod) {
            this.target = target;
            this.method = method;
            this.errorTimes = errorTimes;
            this.errorMethod = errorMethod;
        }
    }
}
