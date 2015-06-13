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

import com.yahoo.labs.samoa.moa.classifiers.core.AttributeSplitSuggestion;
import com.yahoo.labs.samoa.core.ContentEvent;

/**
 * Local Result Content Event is the content event that represents local 
 * calculation of statistic in Local Statistic Processor.
 * 
 * @author Arinto Murdopo
 *
 */
final class LocalResultContentEvent implements ContentEvent{
    
	private static final long serialVersionUID = -4206620993777418571L;
	
	private final AttributeSplitSuggestion bestSuggestion;
	private final AttributeSplitSuggestion secondBestSuggestion;
	private final long splitId;
	private final double maxWeightSeen;
	
	public LocalResultContentEvent(){
		bestSuggestion = null;
		secondBestSuggestion = null;
		splitId = -1;
		maxWeightSeen = 0;
	}
	
	LocalResultContentEvent(long splitId, AttributeSplitSuggestion best, AttributeSplitSuggestion secondBest, double maxWeightSeen){
		this.splitId = splitId;
		this.bestSuggestion = best;
		this.secondBestSuggestion = secondBest;
		this.maxWeightSeen = maxWeightSeen;
	}
	
	@Override
	public String getKey() {
		return null;
	}
	
	/**
	 * Method to return the best attribute split suggestion from this local statistic calculation.
	 * @return The best attribute split suggestion.
	 */
	AttributeSplitSuggestion getBestSuggestion(){
		return this.bestSuggestion;
	}
	
	/**
	 * Method to return the second best attribute split suggestion from this local statistic calculation.
	 * @return The second best attribute split suggestion.
	 */
	AttributeSplitSuggestion getSecondBestSuggestion(){
		return this.secondBestSuggestion;
	}
	
	/**
	 * Method to get the split ID of this local statistic calculation result
	 * @return The split id of this local calculation result
	 */
	long getSplitId(){
		return this.splitId;
	}
	
	
	/**
	 * Method to get the maximum weight seen when VHT executes with model replication
	 * @return
	 */
	double getMaxWeightSeen() {
	    return this.maxWeightSeen;
	}

	@Override
	public void setKey(String str) {
		//do nothing
		
	}

	@Override
	public boolean isLastEvent() {
		return false;
	}
}
