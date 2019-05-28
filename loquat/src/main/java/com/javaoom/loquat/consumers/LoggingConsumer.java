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

import com.javaoom.loquat.processors.EventConsoleLogger;
import com.ngdata.sep.PayloadExtractor;
import com.ngdata.sep.SepModel;
import com.ngdata.sep.impl.BasePayloadExtractor;
import com.ngdata.sep.impl.SepConsumer;
import com.ngdata.sep.impl.SepModelImpl;
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
 * A simple consumer that just logs the events.
 */
@Service
public class LoggingConsumer {
    private static Logger logger = LoggerFactory.getLogger(LoggingConsumer.class);
    @Value("${zk.hostname}")
    private String zkHost;

    @PostConstruct
    public void init() throws Exception {
        logger.info("Start LoggingConsumer...");
        Configuration conf = HBaseConfiguration.create();
        conf.setBoolean("hbase.replication", true);
        ZooKeeperItf zk = ZkUtil.connect(zkHost, 20000);
        SepModel sepModel = new SepModelImpl(zk, conf);
        final String subscriptionName = "logger";
        if (!sepModel.hasSubscription(subscriptionName)) {
            sepModel.addSubscriptionSilent(subscriptionName);
        }
        PayloadExtractor payloadExtractor = new BasePayloadExtractor(Bytes.toBytes("crawler_post"), Bytes.toBytes("info"),
                Bytes.toBytes("content"));
        SepConsumer sepConsumer = new SepConsumer(subscriptionName, 0, new EventConsoleLogger(), 1, zkHost, zk, conf,
                payloadExtractor);
        sepConsumer.start();
        logger.info("Started...");
        while (true) {
            Thread.sleep(Long.MAX_VALUE);
        }
    }

}
