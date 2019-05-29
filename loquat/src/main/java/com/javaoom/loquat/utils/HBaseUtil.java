package com.javaoom.loquat.utils;

import com.javaoom.loquat.entity.TColumnValue;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

public class HBaseUtil {

    public static List<Result> scanScanObject(String tableName, Scan scan) {
        Connection connection = HBaseConnectionPool.getConn();
        List<Result> rows = new ArrayList<>();
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            ResultScanner results = table.getScanner(scan);
            for (Result r : results) {
                rows.add(r);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return rows;
    }


    public static List<Result> scan(String tableName, long startTime, long endTime,
                                    Map<byte[], NavigableSet<byte[]>> familyMap) {
        Scan scan = null;
        try {
            scan = new Scan().setTimeRange(startTime, endTime);
            if (familyMap != null) {
                scan.setFamilyMap(familyMap);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        List<Result> rows = scanScanObject(tableName, scan);
        return rows;
    }

    public static List<TColumnValue> scanAndDecode(String tableName, long startTime, long endTime,
                                                   Map<byte[], NavigableSet<byte[]>> familyMap) {
        List<Result> rows = scan(tableName, startTime, endTime, familyMap);
        return decodeResultBatch(rows);
    }

    public static Result get(String tableName, byte[] rowkey, int readVersions,
                             Map<byte[], NavigableSet<byte[]>> familyMap) {
        Connection connection = HBaseConnectionPool.getConn();
        Result r = null;
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(rowkey);
//            get.readVersions(readVersions);
            get.setMaxVersions(10);
            if (familyMap != null) {
                familyMap.forEach((cf, qualifiers) -> {
                    qualifiers.forEach(qualifier -> {
                        get.addColumn(cf, qualifier);
                    });

                });
            }


            r = table.get(get);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return r;
    }

    private static List<TColumnValue> decodeResultBatch(List<Result> rows) {
        List<TColumnValue> columnValues = new ArrayList<TColumnValue>();
        for (Result r : rows) {
            columnValues.addAll(decodeResult(r));
        }
        return columnValues;
    }

    private static List<TColumnValue> decodeResult(Result r) {
        List<TColumnValue> columnValues = new ArrayList<TColumnValue>();
        for (Cell cell : r.rawCells()) {
            TColumnValue col = new TColumnValue();
            col.setFamily(CellUtil.cloneFamily(cell));
            col.setQualifier(CellUtil.cloneQualifier(cell));
            col.setTimestamp(cell.getTimestamp());
            col.setValue(CellUtil.cloneValue(cell));
            col.setKey(CellUtil.getCellKeyAsString(cell));
            col.setRowKey(CellUtil.cloneRow(cell));
            columnValues.add(col);
        }
        return columnValues;
    }

    public static String bytesToStr(byte[] bytes) {
        return new String(bytes).trim();
    }

    public static String getMapValue(Map<byte[], byte[]> map, String key) {

        byte[] valueBytes = map.get(Bytes.toBytes(key));
        if (valueBytes != null) {
            return bytesToStr(valueBytes);
        }
        return null;
    }

    /**
     * generate family map based on cfQualifierMap {Map<String cf, List<String> qualifiers> }
     *
     * @param cfQualifierMap
     * @param familyMap
     * @return
     */
    public static Map<byte[], NavigableSet<byte[]>> generateFamilyMapBatch(Map<String, List<String>> cfQualifierMap,
                                                                           Map<byte[], NavigableSet<byte[]>> familyMap) {
        if (familyMap == null) {
            familyMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
        }
        for (Map.Entry<String, List<String>> kv : cfQualifierMap.entrySet()) {
            familyMap = generateFamilyMap(kv.getKey(), familyMap, kv.getValue());
        }
        return familyMap;
    }


    public static Map<byte[], NavigableSet<byte[]>> generateFamilyMap(String colFamily,
                                                                      Map<byte[], NavigableSet<byte[]>> familyMap,
                                                                      List<String> qualifiers) {
        if (familyMap == null) {
            familyMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
        }
        NavigableSet<byte[]> colsSet = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        for (String qualifier : qualifiers) {
            colsSet.add(Bytes.toBytes(qualifier));
        }
        familyMap.put(Bytes.toBytes(colFamily), colsSet);
        return familyMap;
    }


}
