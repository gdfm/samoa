package com.yahoo.labs.samoa.learners.classifiers.trees;

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

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

import com.google.common.collect.EvictingQueue;
import com.yahoo.labs.samoa.moa.classifiers.core.AttributeSplitSuggestion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.labs.samoa.instances.Instance;

final class ActiveLearningNode extends LearningNode {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2892102872646338908L;
	private static final Logger logger = LoggerFactory.getLogger(ActiveLearningNode.class);
	
	private double weightSeenAtLastSplitEvaluation;
	
	private final Map<Integer, String> attributeContentEventKeys;
	
	private AttributeSplitSuggestion bestSuggestion;
	private AttributeSplitSuggestion secondBestSuggestion;
	
	private final long id;
	private final int parallelismHint;
	private final SplittingOption splittingOption;
	private final int maxBufferSize;
	private final EvictingQueue<Instance> buffer;
	private int suggestionCtr;
	private int thrownAwayInstance;
	
	private boolean isSplitting;

    protected AttributeBatchContentEvent[] attributeBatchContentEvent;
	
	ActiveLearningNode(double[] classObservation, int parallelismHint, SplittingOption splitOption, int maxBufferSize) {
		super(classObservation);
		this.weightSeenAtLastSplitEvaluation = this.getWeightSeen();
		this.id = VerticalHoeffdingTree.LearningNodeIdGenerator.generate();
		this.attributeContentEventKeys = new HashMap<>();
		this.isSplitting = false;
		this.parallelismHint = parallelismHint;
		this.splittingOption = splitOption;
		this.maxBufferSize = maxBufferSize;
		this.buffer = EvictingQueue.create(this.maxBufferSize);
	}
	
	long getId(){
		return id;
	}

    public AttributeBatchContentEvent[] getAttributeBatchContentEvent() {
        return this.attributeBatchContentEvent;
    }

    public void setAttributeBatchContentEvent(AttributeBatchContentEvent[] attributeBatchContentEvent) {
        this.attributeBatchContentEvent = attributeBatchContentEvent;
    }
    
    public Queue<Instance> getBuffer() {
        return this.buffer;
    }
         
	@Override
    void learnFromInstance(Instance inst, ModelAggregatorProcessor proc) {
	    //log statement are commented since this method is potentially 
	    //executed million times
        if (isSplitting) {
            switch (this.splittingOption) {
            case THROW_AWAY:
                //logger.trace("node {}: splitting is happening, throw away the instance", this.id); // throw all instance will splitting
                this.thrownAwayInstance++;
                return;
            case KEEP:
                //logger.trace("node {}: keep instance with max buffer size: {}, continue sending to local stats", this.id, this.maxBufferSize);
                if(this.maxBufferSize > 0) {
                    //logger.trace("node {}: add to buffer", this.id);
                    buffer.add(inst);
                }
                break;
            default:
                logger.error("node {}: invalid splittingOption option: {}", this.id, this.splittingOption);
                break;
            }
        }

        this.observedClassDistribution.addToValue((int) inst.classValue(),
                inst.weight());

        // done: parallelize by sending attributes one by one
        // TODO: meanwhile, we can try to use the ThreadPool to execute it
        // separately
        // DONE: parallelize by sending in batch, i.e. split the attributes into
        // chunk instead of send the attribute one by one
        for (int i = 0; i < inst.numAttributes() - 1; i++) {
            int instAttIndex = modelAttIndexToInstanceAttIndex(i, inst);
            Integer obsIndex = Integer.valueOf(i);
            String key = attributeContentEventKeys.get(obsIndex);

            if (key == null) {
                key = this.generateKey(i);
                attributeContentEventKeys.put(obsIndex, key);
            }
            AttributeContentEvent ace = new AttributeContentEvent.Builder(
                    this.id, i, key).attrValue(inst.value(instAttIndex))
                    .classValue((int) inst.classValue()).weight(inst.weight())
                    .isNominal(inst.attribute(instAttIndex).isNominal())
                    .build();
            if (this.attributeBatchContentEvent == null) {
                this.attributeBatchContentEvent = new AttributeBatchContentEvent[inst
                        .numAttributes() - 1];
            }
            if (this.attributeBatchContentEvent[i] == null) {
                this.attributeBatchContentEvent[i] = new AttributeBatchContentEvent.Builder(
                        this.id, i, key)
                        // .attrValue(inst.value(instAttIndex))
                        // .classValue((int) inst.classValue())
                        // .weight(inst.weight()]
                        .isNominal(inst.attribute(instAttIndex).isNominal())
                        .build();
            }
            this.attributeBatchContentEvent[i].add(ace);
            // proc.sendToAttributeStream(ace);
        }
    }
	
	@Override
	double[] getClassVotes(Instance inst, ModelAggregatorProcessor map) {
		return this.observedClassDistribution.getArrayCopy();
	}
	
	double getWeightSeen(){
		return this.observedClassDistribution.sumOfValues();
	}
	
	void setWeightSeenAtLastSplitEvaluation(double weight){
		this.weightSeenAtLastSplitEvaluation = weight;
	}
	
	double getWeightSeenAtLastSplitEvaluation(){
		return this.weightSeenAtLastSplitEvaluation;
	}
	
	void requestDistributedSuggestions(long splitId, ModelAggregatorProcessor modelAggrProc) {
	    logger.debug("node: {}. start splitting", this.id);
		this.isSplitting = true; 
		this.suggestionCtr = 0;
		this.thrownAwayInstance = 0;
		
		ComputeContentEvent cce = new ComputeContentEvent(splitId, this.id,
				this.getObservedClassDistribution());
		modelAggrProc.sendToControlStream(cce);
	}
	
	void addDistributedSuggestions(AttributeSplitSuggestion bestSuggestion, AttributeSplitSuggestion secondBestSuggestion){		
		//starts comparing from the best suggestion
		if(bestSuggestion != null){
			if((this.bestSuggestion == null) || (bestSuggestion.compareTo(this.bestSuggestion) > 0)){
				this.secondBestSuggestion = this.bestSuggestion;
				this.bestSuggestion = bestSuggestion;
				
				if(secondBestSuggestion != null){
					
					if((this.secondBestSuggestion == null) || (secondBestSuggestion.compareTo(this.secondBestSuggestion) > 0)){
						this.secondBestSuggestion = secondBestSuggestion;
					}
				}
			}else{
				if((this.secondBestSuggestion == null) || (bestSuggestion.compareTo(this.secondBestSuggestion) > 0)){
					this.secondBestSuggestion = bestSuggestion;
				}
			}
		}
		
		//TODO: optimize the code to use less memory
		this.suggestionCtr++;
	}

	boolean isSplitting(){
		return this.isSplitting;
	}
	
	void endSplitting(){
		this.isSplitting = false;
		logger.debug("node: {}. end splitting, thrown away instance: {}, buffer size: {}", this.id, this.thrownAwayInstance, this.buffer.size());
		this.thrownAwayInstance = 0;
		this.buffer.clear();
	}
	
	AttributeSplitSuggestion getDistributedBestSuggestion(){
		return this.bestSuggestion;
	}
	
	AttributeSplitSuggestion getDistributedSecondBestSuggestion(){
		return this.secondBestSuggestion;
	}
	
	boolean isAllSuggestionsCollected(){
		return (this.suggestionCtr == this.parallelismHint);
	}
	
	private static int modelAttIndexToInstanceAttIndex(int index, Instance inst){
		return inst.classIndex() > index ? index : index + 1;
	}
	
	private String generateKey(int obsIndex){
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (this.id ^ (this.id >>> 32));
		result = prime * result + obsIndex;
		return Integer.toString(result);
	}
	
	static enum SplittingOption {THROW_AWAY, KEEP};
}
