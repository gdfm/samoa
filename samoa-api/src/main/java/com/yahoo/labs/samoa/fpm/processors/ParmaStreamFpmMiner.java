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

import com.github.javacliparser.ClassOption;
import com.github.javacliparser.FloatOption;
import com.github.javacliparser.IntOption;
import com.yahoo.labs.samoa.fpm.FpmMinerInterface;
import com.yahoo.labs.samoa.samplers.SamplerInterface;
import com.yahoo.labs.samoa.topology.ProcessingItem;
import com.yahoo.labs.samoa.topology.Stream;
import com.yahoo.labs.samoa.topology.TopologyBuilder;

public class ParmaStreamFpmMiner implements FpmMinerInterface {

	public FloatOption epsilonOption = new FloatOption("epsilon", 'e',
			"Epsilon parameter for PARMA",
			0, 0, 1);
	public FloatOption deltaOption = new FloatOption("delta", 'd',
			"Delta parameter for PARMA",
			0, 0, 1);
	public IntOption minFreqPercentOption = new IntOption("minFreqPercentOption", 'f',
			"Minimum frequency percent for frequent items",
			1, 0, 100);
	public IntOption dOption = new IntOption("maxTransSize", 't',
			"Maximum length of transaction",
			10, 1, 10000);
	public IntOption numSamplesOption = new IntOption("numSamples", 'n',
			"Number of samples to maintain",
			1, 1, 10000);
	
	public FloatOption phiOption = new FloatOption("phi", 'p',
			"phi parameter for PARMA",
			0, 0, 1);
	public IntOption fpmGapOption = new IntOption("batchSize", 'b',
			"The no. of transactions after which fpm should be done",
			1, 1, Integer.MAX_VALUE);
	public ClassOption samplerClassOption = new ClassOption("samplerClass", 's',
			"Sampler Class for maintaining sample at each node", SamplerInterface.class, "com.yahoo.labs.samoa.samplers.reservoir.SpaceRestrictedTimeBiasedReservoirSampler");
	TopologyBuilder builder;
	Stream outputDataStream;
	Stream outputControlStream;
	ProcessingItem inputProcessingItem;
	@Override
	public void init(TopologyBuilder topologyBuilder, Stream inputStream) {
		builder = topologyBuilder;
		
		double epsilon = this.epsilonOption.getValue();
		double delta = this.deltaOption.getValue();
		int minFreqPercent = minFreqPercentOption.getValue();
		int d = dOption.getValue();
		int numSamples = this.numSamplesOption.getValue();
		double phi = this.phiOption.getValue();
		int fpmGap = this.fpmGapOption.getValue();
		
		
		
		if(minFreqPercent-(epsilon*50) <= 0){
			System.out.println("minFreqPercent is less than or equal to zero: Please revise your input values");
			return;
		}
		
		//*********************Parameter Calculation*******************//
		/*
		 * Compute the number of required "votes" for an itemsets to betransaction
		 * declared frequent 	
		 */
		// The +1 at the end is needed to ensure reqApproxNum > numsamples / 2.
		int reqApproxNum=0;
		//for(numSamples=2;numSamples<=50;numSamples++)
		//{
			reqApproxNum = (int) Math.floor((numSamples*(1-phi))-Math.sqrt(numSamples*(1-phi)*2*Math.log(1/delta))) + 1;
			//System.out.println(numSamples + " : " + reqApproxNum);
		//}
		if(reqApproxNum<=0 || reqApproxNum<=numSamples/2)
		{
			System.out.println("reqApproxNum="+reqApproxNum+" value invalid, it should be >=1 and >="+numSamples/2);
			return;
		}
		SamplerInterface sampler = (SamplerInterface)this.samplerClassOption.getValue();
		int sampleSize = (int) Math.ceil((2 / Math.pow(epsilon, 2))*(d + Math.log(1/ phi)));//w
		sampler.init(sampleSize);
		//Just for the tests. later it can be changed
//		if(sampleSize*numSamples > fpmGap)
//		{
//			System.out.println("Sample size requirement can not be fulfilled for the current configuration");
//			System.out.println("Required fpmGap = " + sampleSize*numSamples);
//			System.out.println("Existing fpmGap = "+fpmGap);
//			System.exit(0);
//		}
		
		
		
		
		//builder.initTopology("Parma Topology");
//		System.out.println(this.sampleSizeOption.getValue());
		
		//********************************Topology building***********************************
		StreamSourceProcessor ss = new StreamSourceProcessor(d,sampleSize,fpmGap,epsilon,phi,numSamples);
		
		ProcessingItem sourcePi = builder.createPi(ss);
		sourcePi.connectInputShuffleStream(inputStream);
		Stream sourceDataStream = builder.createStream(sourcePi);
		ss.setDataStream(sourceDataStream);
		Stream sourceControlStream = builder.createStream(sourcePi);
		ss.setControlStream(sourceControlStream);
		this.inputProcessingItem=sourcePi;
		
		SamplerProcessor samplerP = new SamplerProcessor(minFreqPercent,sampleSize,(float)epsilon,sampler);
		ProcessingItem samplerPi = builder.createPi(samplerP, numSamples);
		samplerPi.connectInputAllStream(sourceControlStream);
		samplerPi.connectInputShuffleStream(sourceDataStream);
		Stream samplerDataStream = builder.createStream(samplerPi);
		samplerP.setSamplerDataStream(samplerDataStream);
		Stream samplerControlStream = builder.createStream(samplerPi);
		samplerP.setSamplerControlStream(samplerControlStream);
		
		AggregatorProcessor a = new AggregatorProcessor((long)numSamples,(long)sampleSize,(long)reqApproxNum,(float)epsilon);
		ProcessingItem aggregatorPi = builder.createPi(a);
		aggregatorPi.connectInputKeyStream(samplerDataStream);
		aggregatorPi.connectInputAllStream(samplerControlStream);
		this.outputDataStream = builder.createStream(aggregatorPi);
		this.outputControlStream = builder.createStream(aggregatorPi);
		a.setOutputControlStream(outputControlStream);
		a.setOutputDataStream(outputDataStream);
	}
	@Override
	public ProcessingItem getInputProcessingItem() {
		return this.inputProcessingItem;
	}
	@Override
	public Stream getOutputControlStream() {
		return this.outputControlStream;
	}
	@Override
	public Stream getOutputDataStream() {
		return this.outputDataStream;
	}
	

}
