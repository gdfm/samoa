package com.yahoo.labs.samoa.fpm.processors;

/*
 * #%L
 * SAMOA
 * %%
 * Copyright (C) 2013 Yahoo! Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.yahoo.labs.samoa.core.ContentEvent;
import com.yahoo.labs.samoa.core.Processor;
import com.yahoo.labs.samoa.fpm.datacontainers.AggregatorMap;
import com.yahoo.labs.samoa.fpm.messages.DoubleMessage;
import com.yahoo.labs.samoa.fpm.messages.Message;
import com.yahoo.labs.samoa.topology.Stream;

/**
 * @author Faisal Moeen
 *
 */
public class AggregatorProcessor implements Processor {

	
	/**
	 * The path of the output file to which the results of frequent itemset mining are to be written.
	 * Each batch of FIM will be written to a different file but with the same prefix as specified in this var.
	 */
	private Stream outputControlStream;
	private Stream outputDataStream;
	/**
	 * id of the task
	 */
	private int taskId;
	/**
	 * Number of samplers connected to this aggregator. 
	 * This value is provided by the user but verified by PARMA for validity. 
	 */
	private long samplerNum;
	/**
	 * The size of each sample to be maintained. 
	 * This is determined automatically by PARMA.
	 */
	private long sampleSize;
	/**
	 * The minimum number of samplers in which if an itemset is frequent, PARMA will consider it frequent.
	 * This is determined by PARMA itself.
	 * This is required to ensure the guarantees offered by PARMA 
	 */
	private long reqApproxNum;
	/**
	 * The epsilon approximation parameter in PARMA
	 */
	private float epsilon;
	/**
	 * Multiple batches could be in the process of aggregation. This is the list of AggregatorMap which is a datatype
	 * for storing the aggregation data for each batch. When the aggregation of frequent itemsets in a batch is complete,
	 * its AggregatorMap is removed from the list.
	 */
	private transient List<AggregatorMap> aggregatorsList;
	/**
	 * The single value message that is under process
	 */
	private Message message;
	/**
	 * The double value data message that is under process
	 */
	private DoubleMessage dataMessage;
	/**
	 * The double value control message that is under process
	 */
	private DoubleMessage controlMessage;
	
	/**
	 * default constructor
	 */
	public AggregatorProcessor(){}
	
	/**
	 * @param outputPath
	 * @param samplerNum
	 * @param sampleSize
	 * @param reqApproxNum
	 * @param epsilon
	 */
	public AggregatorProcessor(long samplerNum, long sampleSize, long reqApproxNum, float epsilon)
	{
		this.samplerNum = samplerNum;
		this.sampleSize=sampleSize;
		this.reqApproxNum = reqApproxNum;
		this.epsilon=epsilon;
		this.aggregatorsList = new ArrayList<AggregatorMap>();
		
	}
	/* (non-Javadoc)
	 * @see samoa.core.Processor#process(samoa.core.ContentEvent)
	 */
	@Override
	public boolean process(ContentEvent event) {
		System.out.println(((Message)event).getValue());
		message = (Message)event;
		if(message.getStreamId().compareTo("samplerDataStream")==0)
		{
			dataMessage = (DoubleMessage)message;
			String itemSet =  dataMessage.getKey();
			Double freq = Double.valueOf(dataMessage.getValue1());
			Long batchId = Long.valueOf(dataMessage.getValue2());
			AggregatorMap relevantAggregator = getAggregator(batchId);
			relevantAggregator.put(itemSet, freq);
//			_collector.emit(new Values(word, countList));
		}
		else if(message.getStreamId().compareTo("samplerControlStream")==0)
		{
			if(message.getKey().compareTo("batchEnd")==0 )
			{
				Long batchId = Long.valueOf(message.getValue());
				AggregatorMap relevantAggregator = getAggregator(batchId);
				boolean fimDone = relevantAggregator.endMessageReceived();
				if(fimDone)
				{
					aggregatorsList.remove(relevantAggregator);
				}
			}
			if(message.getKey().compareTo("sampleSize")==0 )
			{
				this.controlMessage = (DoubleMessage)message;
				Long batchId = Long.valueOf(controlMessage.getValue2());
				AggregatorMap relevantAggregator = getAggregator(batchId);
				relevantAggregator.setSampleSize(Long.valueOf(controlMessage.getValue1()));
				if(Long.valueOf(controlMessage.getValue1())>this.sampleSize)
				{
					this.sampleSize=Long.valueOf(controlMessage.getValue1());
				}
			}
		}
		return false;
	}

	/* (non-Javadoc)
	 * @see samoa.core.Processor#onCreate(int)
	 */
	@Override
	public void onCreate(int id) {
		this.taskId=id;
		aggregatorsList = new ArrayList<AggregatorMap>();
		AggregatorMap map = new AggregatorMap(0,samplerNum,taskId,outputDataStream,outputControlStream,reqApproxNum, sampleSize, epsilon);
		this.aggregatorsList.add(map);
	}

	/* (non-Javadoc)
	 * @see samoa.core.Processor#newProcessor(samoa.core.Processor)
	 */
	@Override
	public Processor newProcessor(Processor p) {
		return new AggregatorProcessor();
	}

	/**
	 * @param batchId
	 * @return
	 */
	public AggregatorMap getAggregator(Long batchId)
	{
		Iterator<AggregatorMap> i = aggregatorsList.iterator();
		AggregatorMap temp=null;
		while(i.hasNext())
		{
			temp = i.next();
			if(temp.getBatchId()==batchId)
				return temp;
		}
		AggregatorMap newAggregatorMap = new AggregatorMap(batchId,samplerNum,taskId, outputDataStream, outputControlStream, reqApproxNum, sampleSize, epsilon);
		aggregatorsList.add(newAggregatorMap);
		return newAggregatorMap;
	}

	
	public Stream getOutputControlStream() {
		return outputControlStream;
	}

	public void setOutputControlStream(Stream outputControlStream) {
		this.outputControlStream = outputControlStream;
	}

	public Stream getOutputDataStream() {
		return outputDataStream;
	}

	public void setOutputDataStream(Stream outputDataStream) {
		this.outputDataStream = outputDataStream;
	}
}
