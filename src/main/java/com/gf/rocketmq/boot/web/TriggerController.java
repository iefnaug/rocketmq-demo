package com.gf.rocketmq.boot.web;

import com.gf.rocketmq.boot.simple.SimpleProducer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * @author GF
 * @since 2023/4/12
 */
@RestController
@RequestMapping("/msg")
public class TriggerController {

    @Resource
    private SimpleProducer simpleProducer;

    @GetMapping("/simple")
    public void simple() {
        simpleProducer.sendMsg();
    }

}
