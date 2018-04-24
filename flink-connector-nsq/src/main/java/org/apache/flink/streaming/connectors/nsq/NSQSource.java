/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.nsq;

import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

//nsq
import com.github.brainlag.nsq.NSQConfig;
import com.github.brainlag.nsq.NSQConsumer;
import com.github.brainlag.nsq.lookup.DefaultNSQLookup;
import com.github.brainlag.nsq.NSQMessage;
import com.github.brainlag.nsq.callbacks.NSQMessageCallback;
import com.github.brainlag.nsq.callbacks.NSQErrorCallback;
import com.github.brainlag.nsq.exceptions.NSQException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;

/**
 * A source that pulls data from Apache NiFi using the NiFi Site-to-Site client. This source
 * produces NiFiDataPackets which encapsulate the content and attributes of a NiFi FlowFile.
 */
public class NSQSource extends RichParallelSourceFunction<NSQMessage> implements StoppableFunction{

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(NSQSource.class);
    
    private NSQConsumer consumer;
    private final String host = "...";
    private final int lookupdports = 4150;
    private String TopicName;
    private String ChannelName;
    protected DeserializationSchema<NSQMessage> DeSchema;
    private NSQLookup lookup;
    private NSQConfig nsqConfig;
    private ArrayList<NSQMessage> nsqMessage_list = new ArrayList<NSQMessage>();
    private volatile int messageLoad = 0;
    private volatile boolean isRunning = true;

    

	/**
	 * Constructs a new NSQSource using the given client config and the default wait time of 1000 ms.
	 *
	 * @param clientConfig the configuration for building a NiFi SiteToSiteClient
	 */
    public NSQSink(NSQConfig nsqconfig, String topicName, String channelName, DeserializationSchema<NSQMessage> deschema) {
        this.nsqConfig = nsqconfig;
        this.TopicName = topicName;
        this.ChannelName = channelName;
        
        this.DeSchema = deschema;
    }
    


	/**
	 * Constructs a new NsqSource using the given client config and wait time.
	 *
	 * @param clientConfig the configuration for building a NiFi SiteToSiteClient
	 * @param waitTimeMs the amount of time to wait (in milliseconds) if no data is available to pull from NiFi
	 */
    
	@Override
	public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        lookup = new DefaultNSQLookup();
        lookup.addLookupAddress(host, lookupdports);
        consumer = new NSQConsumer(lookup, TopicName, channelName, (message) -> {
            
            NSQMessage nsqmessage;
            nsqmessage = DeSchema.deserialize(message);
            
            synchronized(messageLoad){
                nsqMessage_list.add(nsqmessage);
                messageLoad++;
            }
        });
        
	}

	@Override
	public void run(SourceContext<NSQMessage> ctx) throws Exception {
		while(isRunning)
        {
            //response when there is 5 messages
            if(messageLoad > 5){
                synchronized(messageLoad){
                    for(NSQMessage msg : nsqMessage_list){
                        ctx.collect(msg);
                    }
                    
                    messageLoad = 0;
                    nsqMessage_list.clear();
                }//lock
            }//if statement
        }//running?
	}

	@Override
	public void cancel() {
		isRunning = false;
	}

	@Override
	public void close() throws Exception {
		super.close();
		consumer.close();
	}

	@Override
	public void stop() {
		this.isRunning = false;
	}
}
