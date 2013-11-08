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

import com.github.javacliparser.Configurable;
import com.github.javacliparser.FloatOption;
import com.yahoo.labs.samoa.samplers.SamplerInterface;
import com.yahoo.labs.samoa.fpm.exceptions.OutOfRangeException;

public class SpaceRestrictedTimeBiasedReservoirSampler implements SamplerInterface, Configurable{

	private List<String> transactions = null;

	private long reservoirSize=0;
	private double lambda=0;
	private double pIn = 1;
	private Random additionRandom, ejectionRandom;
	private double q;
	private int randomInt;
	
	public FloatOption lambdaOption = new FloatOption("lambda", 'l',
			"lambda parameter for time-biased sampling",
			0, 0, 1);
	
	public void init(long reservoirSize)
	{
		this.reservoirSize=reservoirSize;
		this.lambda=this.lambdaOption.getValue();
		if(lambda>=0 && lambda<=1)
		{
			this.lambda = lambda;
		}
		else
		{
			throw new OutOfRangeException(lambda, 0, 1);
		}
		this.additionRandom = new Random();
		this.ejectionRandom = new Random();
		transactions = new ArrayList<String>();
	}
	public SpaceRestrictedTimeBiasedReservoirSampler(){
		
		
	}
	public SpaceRestrictedTimeBiasedReservoirSampler(long reservoirSize, double lambda)
	{
		System.out.println("Reservoir sampler multi constructor called");
		this.reservoirSize=reservoirSize;
		if(lambda>=0 && lambda<=1)
		{
			this.lambda = lambda;
		}
		else
		{
			throw new OutOfRangeException(lambda, 0, 1);
		}
		this.pIn = this.lambda * this.reservoirSize;
		this.additionRandom = new Random();
		this.ejectionRandom = new Random();
		this.q = (double)(1/(double)this.reservoirSize);
		transactions = new ArrayList<String>((int)reservoirSize);
	}
	
	public void put(String transaction)
	{
		if(transactions.size()<reservoirSize)
		{
			transactions.add(transaction);
			return;
		}
		pIn=pIn*(1-q);
		if(this.additionRandom.nextDouble()<=this.pIn)//the transaction should be inserted in the reservoir
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
		this.q = (double)(1/(double)this.reservoirSize);
		this.pIn = this.lambda * this.reservoirSize;
	}

	public boolean isFull()
	{
		return reservoirSize <= transactions.size();
	}

}
