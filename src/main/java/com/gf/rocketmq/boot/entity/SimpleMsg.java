package com.gf.rocketmq.boot.entity;

import lombok.Data;

/**
 * @author GF
 * @since 2023/4/12
 */
@Data
public class SimpleMsg {

    private Long id;

    private String content;


    public SimpleMsg(Long id, String content) {
        this.id = id;
        this.content = content;
    }
}
