/*
 * Copyright 2012 NGDATA nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.javaoom.loquat.consumers;

import com.google.common.base.Preconditions;
import com.javaoom.loquat.processors.EventConsoleLogger;
import com.javaoom.loquat.processors.EventKafkaSender;
import com.ngdata.sep.EventListener;
import com.ngdata.sep.PayloadExtractor;
import com.ngdata.sep.SepModel;
import com.ngdata.sep.impl.*;
import com.ngdata.sep.util.zookeeper.ZkUtil;
import com.ngdata.sep.util.zookeeper.ZooKeeperItf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

/**
 * A DeltaConsumer  that just distribute log event to kafka.
 */
public class DeltaConsumer {
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private String zkHost;
    private String tableName;
    private String cfName;
    private PayloadExtractor payloadExtractor;
    private SepConsumer sepConsumer;
    private EventListener eventListener;
    private String subscriptionName;

    public DeltaConsumer(String tableName, String cfName, String zkHost) {
        Preconditions.checkNotNull(tableName, "tableName cannot be null");
        this.tableName = tableName;
        this.cfName = cfName;
        this.zkHost = zkHost;

        if (cfName == null) {
            this.payloadExtractor = new TablePayloadExtractor(Bytes.toBytes(tableName));
            this.eventListener = new EventKafkaSender(tableName);
            this.subscriptionName = tableName;
        }
        else {
            this.payloadExtractor = new ColumnFamilyPayloadExtractor(Bytes.toBytes(tableName), Bytes.toBytes(cfName));
            this.eventListener = new EventKafkaSender(tableName + "-" + cfName);
            this.subscriptionName = tableName + "-" + cfName;
        }

    }

    public void start() throws Exception {
        logger.info("Start DeltaConsumer...");
        Configuration conf = HBaseConfiguration.create();
        conf.setBoolean("hbase.replication", true);
        ZooKeeperItf zk = ZkUtil.connect(zkHost, 20000);
        SepModel sepModel = new SepModelImpl(zk, conf);
        if (!sepModel.hasSubscription(subscriptionName)) {
            sepModel.addSubscriptionSilent(subscriptionName);
        }
        sepConsumer = new SepConsumer(subscriptionName, 0, eventListener, 1, zkHost, zk, conf,
                payloadExtractor);
        sepConsumer.start();
        logger.info("DeltaConsumer : {}-{} Started...", tableName, cfName);
        while (true) {
            Thread.sleep(Long.MAX_VALUE);
        }
    }

}
