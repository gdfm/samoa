package com.yahoo.labs.samoa.fpm.datacontainers;

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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import com.yahoo.labs.samoa.fpm.messages.Message;
import com.yahoo.labs.samoa.topology.Stream;


public class AggregatorMap {
	private Stream outputControlStream;
	private Stream outputDataStream;
	private long batchId;
	private Map<String, List<Integer>> map;
	private long numSamples;
	private int numEndMessages=0;
	private int taskId;
	private long reqApproxNum;
	private long sampleSize;
	private float epsilon;

	public AggregatorMap(long batchId, long samplerNum, int taskId, Stream outputDataStream,Stream outputControlStream, long reqApproxNum, long sampleSize, float epsilon) {
		super();
		this.batchId = batchId;
		map = new HashMap<String, List<Integer>>();
		this.numSamples=samplerNum;
		this.taskId = taskId;
		this.outputDataStream = outputDataStream;
		this.outputControlStream = outputControlStream;
		this.reqApproxNum = reqApproxNum;
		this.sampleSize = sampleSize;
		this.epsilon = epsilon;
	}
	
	public void put(String itemSet, Double freq)
	{
		List<Integer> countList = map.get(itemSet);
		if(countList==null)
			countList = new ArrayList<Integer>();
		countList.add(freq.intValue());
		map.put(itemSet, countList);
	}

	public long getBatchId() {
		return batchId;
	}

	public void setBatchId(long batchId) {
		this.batchId = batchId;
	}
	
	public boolean endMessageReceived()
	{
		numEndMessages++;
		if(numEndMessages==numSamples)
		{
			writeResult();
			return true;
		}
		else return false;
	}

	private boolean writeResult()
	{
		outputControlStream.put(new Message("batchId",String.valueOf(this.batchId), false,"outputControlStream"));
		Set<Entry<String, List<Integer>>> set = map.entrySet();
		Iterator<Entry<String,List<Integer>>> iter = set.iterator();
		String key;
		List<Integer> freqList;
		Entry e;
		//			out.println("Item" + " ****** "+"Est Freq"+"Est Freq Lower Bound"+"Est Freq Upper Bound");
		while(iter.hasNext())
		{
			e = iter.next();
			key=(String)e.getKey();
			freqList=(List<Integer>)e.getValue();
			if(freqList.size()>=reqApproxNum)
			{
				Integer[] valuesArr = new Integer[freqList.size()];
				valuesArr = freqList.toArray(valuesArr);
				Arrays.sort(valuesArr);

				/**
				 * Compute the smallest frequency interval containing
				 * reducersNum-requiredApproxNum+1 estimates of the
				 * frequency of the itemset. Use the center of this
				 * interval as global estimate for the frequency. The
				 * confidence interval is obtained by enlarging the
				 * above interval by epsilon/2 on both sides.
				 */
				double minIntervalLength = valuesArr[(int) (numSamples-reqApproxNum)] - valuesArr[0];
				int startIndex = 0;
				for (int i = 1; i < valuesArr.length - numSamples + reqApproxNum; i++)
				{
					double intervalLength = valuesArr[(int) (numSamples-reqApproxNum + i)] - valuesArr[i];
					if (intervalLength < minIntervalLength) 
					{
						minIntervalLength = intervalLength;
						startIndex = i;
					}
				}

				double estimatedFreq = (valuesArr[startIndex] + ( ((double)valuesArr[(int) (startIndex + numSamples - reqApproxNum)] - (double)valuesArr[startIndex])/2)) / (double)sampleSize;
				double confIntervalLowBound = Math.max(0, (double)valuesArr[startIndex] / (double)this.sampleSize - (epsilon / 2));
				double confIntervalUppBound = Math.min(1, (double)(valuesArr[(int) (startIndex + numSamples - reqApproxNum)] / (double)sampleSize) + (epsilon / 2));

				String estFreqAndBoundsStr = "(" + estimatedFreq + " " + confIntervalLowBound + " " + confIntervalUppBound + ")"; 
				Object output;
				//					output.collect(key, estFreqAndBoundsStr);

				outputDataStream.put(new Message(key, estimatedFreq, false, "outputDataStream"));
				System.out.println(key + " ****** "+estFreqAndBoundsStr);
			}
		}
		return true;
	}

	public long getSampleSize() {
		return sampleSize;
	}

	public void setSampleSize(long sampleSize) {
		this.sampleSize = sampleSize;
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
