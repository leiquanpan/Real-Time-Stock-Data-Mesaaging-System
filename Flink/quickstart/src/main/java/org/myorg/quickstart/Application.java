package org.myorg.quickstart;

/**
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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import java.io.*;
import java.util.*;
/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram
 * over some sample data
 *
 * <p>
 * This example shows how to:
 * <ul>
 * <li>write a simple Flink program.
 * <li>use Tuple data types.
 * <li>write and use user-defined functions.
 * </ul>
 *
 */
public class Application {

	//
	//	Program
	//
	public static String modifiedtoString(Record e){

		String json;
                json = "{" + "\"ticker"+"\""+":"+"\""+e.ticker+"\""+"," + "\"date"+"\""+":"+"\""+e.date+"\""+"," + "\"open"+"\""+":"+"\""+e.open+"\""+"," + "\"high"+"\""+":"+"\""+e.high+"\""+"," + "\"low"+"\""+":"+"\""+e.low+"\""+"," + "\"close"+"\""+":"+"\""+e.close+"\""+"," + "\"volume"+"\""+":"+"\""+e.volume+"\""+"," + "\"ex_dividend"+"\""+":"+"\""+e.ex_dividend+"\""+"," + "\"split_ratio"+"\""+":"+"\""+e.split_ratio+"\""+","+ "\"adj_open"+"\""+":"+"\""+e.adj_open+"\""+","+ "\"adj_high"+"\""+":"+"\""+e.adj_high+"\""+","+ "\"adj_low"+"\""+":"+"\""+e.adj_low+"\""+","+ "\"adj_close"+"\""+":"+"\""+e.adj_close+"\""+","+ "\"adj_volume"+"\""+":"+"\""+e.adj_volume +"\""+ "}";
		return json;
	}

	public static void PublishString(String s,String topic) throws Exception {
		String[] command = {"go", "run" ,"../pubmess.go", topic , s };
                try{
                                Process p = Runtime.getRuntime().exec(command);
                                BufferedReader in = new BufferedReader(
                                                new InputStreamReader(p.getInputStream()));
                                String line = null;
                                while ((line = in.readLine()) != null) {
                                        System.out.println(line);
                                }
                        } catch (IOException e) {
                            e.printStackTrace();
                }
        }

	public static void main(String[] args) throws Exception {
		
		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// get input data
		DataSet<Record> financial_info = env.readCsvFile("hdfs:///data_flink/WIKI_PRICES.csv").pojoType(Record.class,"ticker","date","open","high","low","close","volume","ex_dividend","split_ratio","adj_open","adj_high","adj_low","adj_close","adj_volume");

		final long start_time = System.currentTimeMillis();
		System.out.println("The start time is " + start_time);
		DataSet<String> count_rise = financial_info
				.flatMap(new FlatMapFunction<Record, String>() {
					private static final long serialVersionUID = 1L;
				@Override
				public void flatMap(Record record, Collector<String> out) throws Exception {
					//out.collect(new Tuple2<String, Integer>(record.getticker(), 1));
					if(record.close-record.open > 0)
					{
						record.timestamp = System.currentTimeMillis() - start_time;
						out.collect(modifiedtoString(record));
					}
				}
				// group by the tuple field "0" and sum up tuple field "1"
				});
		
		DataSet<String> count_down = financial_info
                                .flatMap(new FlatMapFunction<Record, String>() {
                                        private static final long serialVersionUID = 1L;
                                @Override
                                public void flatMap(Record record, Collector<String> out) throws Exception {
                                        //out.collect(new Tuple2<String, Integer>(record.getticker(), 1));
                                        if(record.close-record.open < 0)
					{
                                                record.timestamp = System.currentTimeMillis() - start_time;
                                                out.collect(modifiedtoString(record));					
					}
                                }
                                // group by the tuple field "0" and sum up tuple field "1"
                                });
		// execute and print result
		List<String> count_rises = count_rise.collect();
		//System.out.println("The size is " + count_rises.size());
		for (String s : count_rises){
			PublishString(s,"rise_stock");
		}
		List<String> count_drop = count_down.collect();
		//System.out.println("The size is " + count_drop.size());
		for (String s : count_drop){
                        PublishString(s,"down_stock");
		}
	}
}
