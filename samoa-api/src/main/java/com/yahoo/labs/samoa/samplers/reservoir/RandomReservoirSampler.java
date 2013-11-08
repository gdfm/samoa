package com.yahoo.labs.samoa.samplers.reservoir;

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
import java.util.List;
import java.util.Random;

import com.yahoo.labs.samoa.samplers.SamplerInterface;
import com.yahoo.labs.samoa.fpm.exceptions.OutOfRangeException;

public class RandomReservoirSampler implements SamplerInterface{

	private List<String> transactions = null;

	private long reservoirSize=0;
	private Random additionRandom, ejectionRandom;
	private int randomInt;
	
	public static void main(String[] args) {
		
	}
	
	public RandomReservoirSampler()
	{
		this.additionRandom = new Random();
		this.ejectionRandom = new Random();
		transactions = new ArrayList<String>((int)reservoirSize);
	}
	
	public void put(String transaction)
	{
		if(transactions.size()<reservoirSize)
		{
			transactions.add(transaction);
			return;
		}
		if(this.additionRandom.nextDouble()<=0.5)//the transaction should be inserted in the reservoir
		{
			randomInt = ejectionRandom.nextInt((int)reservoirSize);
			transactions.remove(randomInt);
			transactions.add(randomInt,transaction);
		}
	}
	
	public List<String> getTransactions() {
		return transactions;
	}
	
	public int getCurrentSize()
	{
		return transactions.size();
	}
	
	public long getReservoirSize() {
		return reservoirSize;
	}

	public void setReservoirSize(long reservoirSize) {
		this.reservoirSize = reservoirSize;
	}

	public boolean isFull()
	{
		return reservoirSize <= transactions.size();
	}

	@Override
	public void init(long reservoirSize) {
		this.reservoirSize=reservoirSize;
	}

}
