package com.javaoom.loquat.processors;

import com.javaoom.loquat.consumers.LoggingConsumer;
import com.ngdata.sep.EventListener;
import com.ngdata.sep.SepEvent;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class EventConsoleLogger implements EventListener {
    private static Logger logger = LoggerFactory.getLogger(LoggingConsumer.class);
    @Override
    public void processEvents(List<SepEvent> sepEvents) {
        for (SepEvent sepEvent : sepEvents) {
            logger.info("Received event:");
            logger.info("  table = " + Bytes.toString(sepEvent.getTable()));
            logger.info("  row = " + Bytes.toString(sepEvent.getRow()));
            logger.info("  payload = " + Bytes.toString(sepEvent.getPayload()));
            logger.info("  key values = ");
            for (Cell kv : sepEvent.getKeyValues()) {
                logger.info("    " + Bytes.toString(kv.getValueArray()));
            }
        }
    }
}
