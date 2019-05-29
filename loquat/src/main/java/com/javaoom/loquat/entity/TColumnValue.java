package com.javaoom.loquat.entity;

import lombok.Data;

@Data
public class TColumnValue {
    private byte[] family;
    private byte[] qualifier;
    private long timestamp;
    private byte[] value;
    private String key;
    private byte[] rowKey;

    @Override
    public String toString() {
        return "TColumnValue{" +
                "family=" + bytesToStr(family) +
                ", qualifier=" + bytesToStr(qualifier) +
                ", timestamp=" + timestamp +
                ", value=" + bytesToStr(value) +
                ", rowKey=" + bytesToStr(rowKey) +
                ", key='" + key + '\'' +
                '}';
    }

    private  String  bytesToStr(byte[] bytes){
        return new String(bytes).trim();
    }
}
