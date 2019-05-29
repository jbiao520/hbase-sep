package com.javaoom.loquat.entity;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class CdcDeltaMsg {
    private int type;
    private String rk;
    private String content0;
    private String content1;
    private Long ts;
}
