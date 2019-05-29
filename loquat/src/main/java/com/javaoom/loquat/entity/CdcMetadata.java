package com.javaoom.loquat.entity;

import lombok.Data;
import lombok.ToString;


@Data
@ToString

public class CdcMetadata {
    private String tableName;
    private String startRow;
    private long timeStamp;
    private String columns;
    private String cronExp;
    private int status;
}
