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





import com.yahoo.labs.samoa.core.ContentEvent;
import com.yahoo.labs.samoa.core.Processor;
import com.yahoo.labs.samoa.topology.Stream;
import com.yahoo.labs.samoa.samplers.SamplerInterface;
import com.yahoo.labs.samoa.fpm.messages.DoubleMessage;
import com.yahoo.labs.samoa.fpm.messages.Message;
import com.yahoo.labs.samoa.fpm.spmf.fpgrowth.AlgoFPGrowthStorm;

public class SamplerProcessor implements Processor {

	
	private Stream samplerDataStream;
	private Stream samplerControlStream;
	
	private SamplerInterface sampler;
	private long sampleSize=0;
	private int minFreqPercent;
	private int taskId;
	private long fimBatchCount=0;
	private Message message;
	
	public SamplerProcessor(int minFreqPercent, long sampleSize, float epsilon, SamplerInterface sampler)
	{
		this.minFreqPercent = minFreqPercent;
		this.sampleSize=sampleSize;
		this.sampler = sampler;
		this.sampler = sampler;
		this.sampler.setReservoirSize(sampleSize);
	}
	public SamplerProcessor()
	{
		
	}
	
	//*******************************interface methods*****************************
	@Override
	public boolean process(ContentEvent event) {
		this.message = (Message)event;
		System.out.println(this.taskId);
		if(message.getStreamId().compareTo("sourceDataStream")==0)
		{
			sampler.put(message.getValue());
			writeTransaction(message.getValue());
		}
		else if(message.getStreamId().compareTo("sourceControlStream")==0)
		{
			if(message.getKey().compareTo("fpm")==0 && message.getValue().compareTo("yes")==0)
			{
//				FPgrowth.mineFrequentItemsets(reservoir.getTransactions().iterator(), reservoir.getCurrentSize(), minFreqPercent - (epsilon * 50) , _collector,"samplerDataStream",fimBatchCount);
				AlgoFPGrowthStorm algo = new AlgoFPGrowthStorm();
				algo.runAlgorithm(sampler.getTransactions(), sampler.getCurrentSize(), minFreqPercent, samplerDataStream, "samplerDataStream", fimBatchCount);
				samplerControlStream.put(new Message("batchEnd",fimBatchCount,false, "samplerControlStream"));
				fimBatchCount++;
				writeBatchIdHeader(fimBatchCount);
//				file = new File(outputPath+"_transactions"+"_"+fimBatchCount+"_"+taskId);
			}

			if(message.getKey().compareTo("sampleSize")==0 )
			{
				sampleSize=Long.valueOf(message.getValue());
				sampler.setReservoirSize(sampleSize);
				samplerControlStream.put(new DoubleMessage("sampleSize",String.valueOf(sampleSize),String.valueOf(fimBatchCount),"samplerControlStream"));
				return true;
			}
		}
		return false;
	}

	@Override
	public void onCreate(int id) {
		this.taskId = id;
	}

	@Override
	public Processor newProcessor(Processor p) {
		return new SamplerProcessor();
	}
	
	
	
	private void writeTransaction(String transaction)
	{
	}
	public Stream getSamplerDataStream() {
		return samplerDataStream;
	}
	public void setSamplerDataStream(Stream samplerDataStream) {
		this.samplerDataStream = samplerDataStream;
	}
	public Stream getSamplerControlStream() {
		return samplerControlStream;
	}
	public void setSamplerControlStream(Stream samplerControlStream) {
		this.samplerControlStream = samplerControlStream;
	}
	
	private void writeBatchIdHeader(long batchId) {
	}

}
