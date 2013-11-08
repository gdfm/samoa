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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import com.yahoo.labs.samoa.core.ContentEvent;
import com.yahoo.labs.samoa.core.Processor;
import com.yahoo.labs.samoa.fpm.messages.Message;
import com.yahoo.labs.samoa.topology.Stream;

public class StreamSourceProcessor implements Processor {

	private Stream dataStream;
	private Stream controlStream;
	private transient BufferedReader br=null;
	private long tupleCounter=0;
	private long globalTupleCounter=0;
	private long sampleSize=0;
	private long fpmGap=0;
	private double epsilon = 0;
	private double phi = 0;
	private boolean firstBatch=true;
	private long firstGap=0;
	private long numSamples;
	private boolean firstTuple=true;
	private long startTime=0;
	private String inputPath;
	private int d;
	private int taskId;
	
	public void setDataStream(Stream stream)
	{
		this.dataStream = stream;
	}
	public void setControlStream(Stream stream)
	{
		this.controlStream = stream;
	}
	
	
	public void nextTuple() {
		if(firstTuple)
		{
			startTime=System.currentTimeMillis();
			firstTuple=false;
		}
		String line;
		try {
			if ((line = br.readLine()) != null) {
				if(line.split("\\s+").length>d)
				{
					sampleSize = (int) Math.ceil((2 / Math.pow(epsilon, 2))*(d + Math.log(1/ phi)));
//					_collector.emit("spoutControlStream", new Values("sampleSize",sampleSize));
					controlStream.put(new Message("sampleSize",sampleSize, false,"sourceControlStream"));
					d=line.split("\\s+").length;
					firstGap = numSamples*sampleSize;
				}
//				_collector.emit("spoutDataStream", new Values(line));
				dataStream.put(new Message(String.valueOf(globalTupleCounter), line, false, "sourceDataStream"));
				tupleCounter++;
				globalTupleCounter++;
//				System.out.println(++tupleCounter);
//				System.out.println(++globalTupleCounter);;
				if(firstBatch)
				{
					if(tupleCounter>=firstGap)
					{
//						_collector.emit("spoutControlStream", new Values("fpm","yes"));
						controlStream.put(new Message("fpm","yes", false, "sourceControlStream"));
						tupleCounter=0;
						firstBatch=false;
					}	
				}
				else if(tupleCounter>=fpmGap)
				{
//					_collector.emit("spoutControlStream", new Values("fpm","yes"));
					controlStream.put(new Message("fpm","yes", false,"sourceControlStream"));
					tupleCounter=0;
				}
			}
			else
			{
				System.out.println("Start of Stream = "+startTime+" ; End of Stream = "+ System.currentTimeMillis());
				
				Thread.sleep(100000);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	
	//********************************Processor interface methods***********************************
	@Override
	public boolean process(ContentEvent event) {
		if(firstTuple)
		{
			startTime=System.currentTimeMillis();
			firstTuple=false;
		}
		String line=((Message)event).getValue();
			if(line.split("\\s+").length>d)
			{
				sampleSize = (int) Math.ceil((2 / Math.pow(epsilon, 2))*(d + Math.log(1/ phi)));
				//					_collector.emit("spoutControlStream", new Values("sampleSize",sampleSize));
				controlStream.put(new Message("sampleSize",sampleSize, false,"sourceControlStream"));
				d=line.split("\\s+").length;
				firstGap = numSamples*sampleSize;
			}
			//				_collector.emit("spoutDataStream", new Values(line));
			dataStream.put(new Message(String.valueOf(globalTupleCounter), line, false, "sourceDataStream"));
			tupleCounter++;
			globalTupleCounter++;
			//				System.out.println(++tupleCounter);
			//				System.out.println(++globalTupleCounter);;
			if(firstBatch)
			{
				if(tupleCounter>=firstGap)
				{
					//						_collector.emit("spoutControlStream", new Values("fpm","yes"));
					controlStream.put(new Message("fpm","yes", false, "sourceControlStream"));
					tupleCounter=0;
					firstBatch=false;
				}	
			}
			else if(tupleCounter>=fpmGap)
			{
				//					_collector.emit("spoutControlStream", new Values("fpm","yes"));
				controlStream.put(new Message("fpm","yes", false,"sourceControlStream"));
				tupleCounter=0;
			}
		return false;
	}
	
	public StreamSourceProcessor(int d, long sampleSize, long fpmGap, double epsilon, double phi, long numSamples)
	{
		this.d = d;
		this.sampleSize = sampleSize;
		this.fpmGap = fpmGap;
		this.epsilon = epsilon;
		this.phi = phi;
		this.numSamples = numSamples;
		firstGap = 2*numSamples*sampleSize;
	}
	public StreamSourceProcessor()
	{
		System.out.println("Why meeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");
	}
	@Override
	public void onCreate(int id) {
		this.taskId = id;
	}
	@Override
	public Processor newProcessor(Processor p) {
		return new StreamSourceProcessor();
	}

}
