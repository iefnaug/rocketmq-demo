package com.gf.rocketmq.boot;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author GF
 * @since 2023/4/12
 */
@SpringBootApplication
public class RocketmqApplication {


    public static void main(String[] args) {
        SpringApplication.run(RocketmqApplication.class, args);
    }


//    @Bean
//    public RocketMQTemplate rocketMQTemplate() {
//        RocketMQTemplate rocketMQTemplate = new RocketMQTemplate();
//        return rocketMQTemplate;
//    }

}
