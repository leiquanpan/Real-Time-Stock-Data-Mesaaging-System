/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.nsq;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;

//nsq
import com.github.brainlag.nsq.NSQConfig;
import com.github.brainlag.nsq.NSQProducer;
import com.github.brainlag.nsq.lookup.DefaultNSQLookup;
import com.github.brainlag.nsq.NSQMessage;
import com.github.brainlag.nsq.callbacks.NSQMessageCallback;
import com.github.brainlag.nsq.callbacks.NSQErrorCallback;
import com.github.brainlag.nsq.exceptions.NSQException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class NSQSink<NSQMessage> extends RichSinkFunction<NSQMessage> {


    private NSQProducer producer;
    private final String host;
    private final int lookupdports;
    private String TopicName;
    protected SerializationSchema<NSQMessage> Schema;
    private NSQConfig nsqConfig;
    
    
	public NSQSink(NSQConfig nsqconfig, String topicName, String Host, String lookupdPorts, SerializationSchema<NSQMessage> schema) {
        this.nsqConfig = nsqconfig;
        this.TopicName = topicName;
        this.ChannelName = channelName;
        this.host = Host;
        this.lookupdports = lookupdPorts;
        
		this.Schema = schema;
	}

    
	@Override
	public void open(Configuration parameters) throws Exception {
		producer = new NSQProducer().addAddress(nsqConfig., lookupdports).start();
	}

    
	@Override
	public void invoke(NSQMessage value) throws Exception {
        try {
            byte[] msg = Schema.serialize(value);
            
            producer.produce(TopicName, msg);
        } catch (IOException e) {
            if (logFailuresOnly) {
                LOG.error("Cannot send NSQ message", e);
            } else {
                throw new RuntimeException("Cannot send NSQ message", e);
            }
        }
	}

	@Override
	public void close() throws Exception {
        try {
            if (producer != null) {
                producer.shutdown();
                producer = null;
            }
        } catch (IOException e) {
            LOG.error("Cannot close NSQ producer", e);
        }

	}

}
