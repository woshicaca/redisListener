package com.itwobyte.download.annotation;

import java.lang.annotation.*;
import java.lang.reflect.Method;

@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RedisListListener {

    /**
     * 监听的通道名称
     */
    String channel() default "*";


    /**
     * 消息处理失败次数，默认按照 messageId 记录次数
     */
    int errorTimes() default 10;



    /**
     * 或者在同一个类下面自定义一个失败处理，填写该值，使用后errorTimes无效
     */
    String errorHandler() default "";
}
