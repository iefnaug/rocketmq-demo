package com.gf.rocketmq.boot;

import com.alibaba.fastjson.JSONObject;
import com.gf.rocketmq.boot.entity.SimpleMsg;

import java.util.ArrayList;
import java.util.List;

/**
 * @author GF
 * @since 2023/4/13
 */
public class MyTest {


    public static void main(String[] args) {
        List<SimpleMsg> list = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            SimpleMsg msg = new SimpleMsg((long)i + 1, "hello" + i);
            list.add(msg);
        }
        String json = JSONObject.toJSONString(list, false);
        System.out.println(json);
    }

}
