package com.yahoo.labs.samoa.tasks;

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



import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.javacliparser.ClassOption;
import com.github.javacliparser.Configurable;
import com.github.javacliparser.StringOption;
import com.yahoo.labs.samoa.core.Processor;
import com.yahoo.labs.samoa.core.TopologyStarter;
import com.yahoo.labs.samoa.fpm.FpmMinerInterface;
import com.yahoo.labs.samoa.fpm.FpmTopologyStarter;
import com.yahoo.labs.samoa.fpm.ReaderInterface;
import com.yahoo.labs.samoa.fpm.processors.FileReaderProcessor;
import com.yahoo.labs.samoa.fpm.processors.FileWriterProcessor;
import com.yahoo.labs.samoa.topology.ComponentFactory;
import com.yahoo.labs.samoa.topology.EntranceProcessingItem;
import com.yahoo.labs.samoa.topology.ProcessingItem;
import com.yahoo.labs.samoa.topology.Stream;
import com.yahoo.labs.samoa.topology.Topology;
import com.yahoo.labs.samoa.topology.TopologyBuilder;

public class FpmTask implements Task, Configurable {

	
	
	public StringOption topologyNameOption = new StringOption("topologyName", 't',
			"Topology Name",
			"Default Topology");
	public ClassOption FpmMinerClassOption = new ClassOption("fpmMinerClass", 'm',
			"FPM Miner Class", FpmMinerInterface.class, "com.yahoo.labs.samoa.fpm.processors.ParmaStreamFpmMiner");
	public ClassOption readerClassOption = new ClassOption("readerClass", 'r',
			"Reader Class", ReaderInterface.class, "com.yahoo.labs.samoa.fpm.processors.FileReaderProcessor");
	public ClassOption writerClassOption = new ClassOption("writerClass", 'w',
			"Writer Class", Processor.class, "com.yahoo.labs.samoa.fpm.processors.FileWriterProcessor");
	
	
	TopologyBuilder builder;
	TopologyStarter starter;
	private static Logger logger = LoggerFactory.getLogger(FpmTask.class);
	
	@Override
	public void init() {
		
		
		//*********************Parameter Calculation*******************//
		/*
		 * Compute the number of required "votes" for an itemsets to betransaction
		 * declared frequent 	
		 */
		// The +1 at the end is needed to ensure reqApproxNum > numsamples / 2.
		
		
		builder.initTopology(this.topologyNameOption.getValue());
		
		//********************************Topology building***********************************
		Processor sourceProcessor = this.readerClassOption.getValue();
		FpmMinerInterface fpmMiner = this.FpmMinerClassOption.getValue();
		FileReaderProcessor fileReaderProcessor = this.readerClassOption.getValue();
		FileWriterProcessor fileWriterProcessor =  this.writerClassOption.getValue();
		
		starter = new FpmTopologyStarter(fileReaderProcessor);
		EntranceProcessingItem sourcePi = builder.createEntrancePi(sourceProcessor, starter);
		Stream sourceDataStream = builder.createStream(sourcePi);
		((ReaderInterface)sourceProcessor).setDataStream(sourceDataStream);
				
		fpmMiner.init(builder, sourceDataStream);
		fpmMiner.getInputProcessingItem().connectInputShuffleStream(sourceDataStream);
		ProcessingItem writerProcessingItem = builder.createPi(fileWriterProcessor);
		writerProcessingItem.connectInputShuffleStream(fpmMiner.getOutputDataStream());
		writerProcessingItem.connectInputShuffleStream(fpmMiner.getOutputControlStream());
		
		
//		Sampler sampler = new Sampler();
//		starter =  new FpmTopologyStarter(sampler);
//		EntranceProcessingItem sourcePi = builder.createEntrancePi(sampler, starter);
//		Stream s = builder.createStream(sourcePi);
//		sampler.setStream(s);
//		Aggregator a = new Aggregator();
//		ProcessingItem pi = builder.createPi(a, 2);
//		pi.connectInputShuffleStream(s);
	}

	@Override
	public Topology getTopology() {
		return builder.build();
	}

	@Override
	public TopologyStarter getTopologyStarter() {
		return starter;
	}

	@Override
	public void setFactory(ComponentFactory factory) {
		// for now use this code first, will be removed after dynamic binding is done
		//called by Storm
		builder = new TopologyBuilder(factory);
		logger.debug("Sucessfully instantiating TopologyBuilder");
	}

}
