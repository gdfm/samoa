 package com.yahoo.labs.samoa.fpm.spmf.fpgrowth;

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
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.yahoo.labs.samoa.fpm.messages.DoubleMessage;
import com.yahoo.labs.samoa.fpm.messages.Message;
import com.yahoo.labs.samoa.topology.Stream;

import ca.pfv.spmf.patterns.itemset_array_integers_with_count.Itemset;
import ca.pfv.spmf.patterns.itemset_array_integers_with_count.Itemsets;
import ca.pfv.spmf.tools.MemoryLogger;

/** 
 * This is an implementation of the FPGROWTH algorithm (Han et al., 2004) 
 * based on the description in the book of Han & Kamber.
 * 
 * This is an optimized version that saves the result to a file
 * or keep it into memory if no output path is provided
 * by the user.
 *
 * Copyright (c) 2008-2012 Philippe Fournier-Viger
 * 
 * This file is part of the SPMF DATA MINING SOFTWARE
 * (http://www.philippe-fournier-viger.com/spmf).
 *
 * SPMF is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * SPMF is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with SPMF.  If not, see <http://www.gnu.org/licenses/>.
 */
public class AlgoFPGrowthStorm {

	// for statistics
	private long startTimestamp; // start time of the latest execution
	private long endTime; // end time of the latest execution
	private int transactionCount = 0; // transaction count in the database
	private int itemsetCount; // number of freq. itemsets found
	
	// parameter
	public int relativeMinsupp;// the relative minimum support
	
	Stream stream = null; // object to write the output file
	String outputStreamId="";
	List<String> transactions=null;
	long fimBatchCount=0;
	
	// The  patterns that are found 
	// (if the user want to keep them into memory)
	protected Itemsets patterns = null;
	
	// the number of transactions in the last database read
	private int databaseSize;
	
	private MemoryLogger memoryLogger = null;


	/**
	 * Constructor
	 */
	public AlgoFPGrowthStorm() {
		
	}

	/**
	 * Method to run the FPGRowth algorithm.
	 * @param input the path to an input file containing a transaction database.
	 * @param output the output file path for saving the result (if null, the result 
	 *        will be returned by the method instead of being saved).
	 * @param minsupp the minimum support threshold.
	 * @return the result if no output file path is provided.
	 * @throws IOException exception if error reading or writing files
	 */
	
	public Itemsets runAlgorithm(List<String> transactions, long reservoirSize, float minFreqPercent, Stream stream, String outputStreamId, long fimBatchCount){
		// record start time
		startTimestamp = System.currentTimeMillis();
		// number of itemsets found
		itemsetCount =0;
		float minsupp = minFreqPercent/100;
		// reset the transaction count
		databaseSize =0;
		
		//initialize tool to record memory usage
		memoryLogger = new MemoryLogger();
		memoryLogger.checkMemory();
		this.stream=stream;
		this.outputStreamId = outputStreamId;
		this.transactions = transactions;
		this.fimBatchCount = fimBatchCount;
				
		// (1) PREPROCESSING: Initial database scan to determine the frequency of each item
		// The frequency is stored in a map:
		//    key: item   value: support
		final Map<Integer, Integer> mapSupport = new HashMap<Integer, Integer>();
		
		scanDatabaseToDetermineFrequencyOfSingleItems(transactions, mapSupport);
		
		// convert the minimum support as percentage to a
		// relative minimum support
		this.relativeMinsupp = (int) Math.ceil(minsupp * transactionCount);
		
		// (2) Scan the database again to build the initial FP-Tree
		// Before inserting a transaction in the FPTree, we sort the items
		// by descending order of support.  We ignore items that
		// do not have the minimum support.
		FPTree tree = new FPTree();
		
		// read the file
		
		Iterator<String> reader = transactions.iterator();
		String line;
		// for each line (transaction) until the end of file
		while(reader.hasNext()){ 
			// if the line is  a comment, is  empty or is a
			// kind of metadata
			line=reader.next();
			if (line.isEmpty() == true ||
					line.charAt(0) == '#' || line.charAt(0) == '%'
					|| line.charAt(0) == '@') {
				continue;
			}	

			String[] lineSplited = line.split(" ");
			//			Set<Integer> alreadySeen = new HashSet<Integer>();
			List<Integer> transaction = new ArrayList<Integer>();
			// for each item in the transaction
			for(String itemString : lineSplited){  
				Integer item = Integer.parseInt(itemString);
				// only add items that have the minimum support
				if( //alreadySeen.contains(item)  == false  &&
						mapSupport.get(item) >= relativeMinsupp){
					transaction.add(item);	
					//alreadySeen.add(item);
				}
			}
			// sort item in the transaction by descending order of support
			Collections.sort(transaction, new Comparator<Integer>(){
				public int compare(Integer item1, Integer item2){
					// compare the frequency
					int compare = mapSupport.get(item2) - mapSupport.get(item1);
					// if the same frequency, we check the lexical ordering!
					if(compare == 0){ 
						return (item1 - item2);
					}
					// otherwise, just use the frequency
					return compare;
				}
			});
			// add the sorted transaction to the fptree.
			tree.addTransaction(transaction);
			// increase the transaction count
			databaseSize++;
		}
		// close the input file
		
		// We create the header table for the tree
		tree.createHeaderList(mapSupport);
		
		// (5) We start to mine the FP-Tree by calling the recursive method.
		// Initially, the prefix alpha is empty.
		int[] prefixAlpha = new int[0];
		fpgrowth(tree, prefixAlpha, transactionCount, mapSupport);
		
		// record the execution end time
		endTime= System.currentTimeMillis();
		
		// check the memory usage
		memoryLogger.checkMemory();
		
		// return the result (if saved to memory)
		return patterns;
	}
	
	
//	public Itemsets runAlgorithm(String input, String output, double minsupp) throws FileNotFoundException, IOException {
//		// record start time
//		startTimestamp = System.currentTimeMillis();
//		// number of itemsets found
//		itemsetCount =0;
//		
//		// reset the transaction count
//		databaseSize =0;
//		
//		//initialize tool to record memory usage
//		memoryLogger = new MemoryLogger();
//		memoryLogger.checkMemory();
//		
//		// if the user want to keep the result into memory
//		if(output == null){
//			writer = null;
//			patterns =  new Itemsets("FREQUENT ITEMSETS");
//	    }else{ // if the user want to save the result to a file
//			patterns = null;
//			writer = new BufferedWriter(new FileWriter(output)); 
//		}
//		
//		// (1) PREPROCESSING: Initial database scan to determine the frequency of each item
//		// The frequency is stored in a map:
//		//    key: item   value: support
//		final Map<Integer, Integer> mapSupport = new HashMap<Integer, Integer>();
//		
//		scanDatabaseToDetermineFrequencyOfSingleItems(input, mapSupport);
//		
//		// convert the minimum support as percentage to a
//		// relative minimum support
//		this.relativeMinsupp = (int) Math.ceil(minsupp * transactionCount);
//		
//		// (2) Scan the database again to build the initial FP-Tree
//		// Before inserting a transaction in the FPTree, we sort the items
//		// by descending order of support.  We ignore items that
//		// do not have the minimum support.
//		FPTree tree = new FPTree();
//		
//		// read the file
//		BufferedReader reader = new BufferedReader(new FileReader(input));
//		String line;
//		// for each line (transaction) until the end of the file
//		while( ((line = reader.readLine())!= null)){ 
//			// if the line is  a comment, is  empty or is a
//			// kind of metadata
//			if (line.isEmpty() == true ||
//					line.charAt(0) == '#' || line.charAt(0) == '%'
//							|| line.charAt(0) == '@') {
//				continue;
//			}
//			
//			String[] lineSplited = line.split(" ");
////			Set<Integer> alreadySeen = new HashSet<Integer>();
//			List<Integer> transaction = new ArrayList<Integer>();
//			// for each item in the transaction
//			for(String itemString : lineSplited){  
//				Integer item = Integer.parseInt(itemString);
//				// only add items that have the minimum support
//				if( //alreadySeen.contains(item)  == false  &&
//						mapSupport.get(item) >= relativeMinsupp){
//					transaction.add(item);	
//					//alreadySeen.add(item);
//				}
//			}
//			// sort item in the transaction by descending order of support
//			Collections.sort(transaction, new Comparator<Integer>(){
//				public int compare(Integer item1, Integer item2){
//					// compare the frequency
//					int compare = mapSupport.get(item2) - mapSupport.get(item1);
//					// if the same frequency, we check the lexical ordering!
//					if(compare == 0){ 
//						return (item1 - item2);
//					}
//					// otherwise, just use the frequency
//					return compare;
//				}
//			});
//			// add the sorted transaction to the fptree.
//			tree.addTransaction(transaction);
//			// increase the transaction count
//			databaseSize++;
//		}
//		// close the input file
//		reader.close();
//		
//		// We create the header table for the tree
//		tree.createHeaderList(mapSupport);
//		
//		// (5) We start to mine the FP-Tree by calling the recursive method.
//		// Initially, the prefix alpha is empty.
//		int[] prefixAlpha = new int[0];
//		fpgrowth(tree, prefixAlpha, transactionCount, mapSupport);
//		
//		// close the output file if the result was saved to a file
//		if(writer != null){
//			writer.close();
//		}
//		// record the execution end time
//		endTime= System.currentTimeMillis();
//		
//		// check the memory usage
//		memoryLogger.checkMemory();
//		
//		// return the result (if saved to memory)
//		return patterns;
//	}

	/**
	 * This method scans the input database to calculate the support of single items
	 * @param input the path of the input file
	 * @param mapSupport a map for storing the support of each item (key: item, value: support)
	 * @throws IOException  exception if error while writing the file
	 */
	private void scanDatabaseToDetermineFrequencyOfSingleItems(List<String> input,
			final Map<Integer, Integer> mapSupport){
		//Create object for reading the input file
//		BufferedReader reader = new BufferedReader(new FileReader(input));
		Iterator<String> reader = input.iterator();
		String line;
		// for each line (transaction) until the end of file
		while(reader.hasNext()){ 
			// if the line is  a comment, is  empty or is a
			// kind of metadata
			line=reader.next();
			if (line.isEmpty() == true ||
					line.charAt(0) == '#' || line.charAt(0) == '%'
							|| line.charAt(0) == '@') {
				continue;
			}
			
			// split the line into items
			String[] lineSplited = line.split(" ");
			// for each item
			for(String itemString : lineSplited){  
				// increase the support count of the item
				Integer item = Integer.parseInt(itemString);
				// increase the support count of the item
				Integer count = mapSupport.get(item);
				if(count == null){
					mapSupport.put(item, 1);
				}else{
					mapSupport.put(item, ++count);
				}
			}
			// increase the transaction count
			transactionCount++;
		}
	}


	/**
	 * This method mines pattern from a Prefix-Tree recursively
	 * @param tree  The Prefix Tree
	 * @param prefix  The current prefix "alpha"
	 * @param mapSupport The frequency of each item in the prefix tree.
	 * @throws IOException  exception if error writing the output file
	 */
	private void fpgrowth(FPTree tree, int[] prefixAlpha, int prefixSupport, Map<Integer, Integer> mapSupport){
		// We need to check if there is a single path in the prefix tree or not.
		// So first we check if there is only one item in the header table
		if(tree.headerList.size() == 1){
			FPNode node = tree.mapItemNodes.get(tree.headerList.get(0));
			// We need to check if this item has some node links.
			if(node.nodeLink == null){ 
				// That means that there is a single path, so we 
				// add all combinations of this path, concatenated with the prefix "alpha", to the set of patterns found.
				addAllCombinationsForPathAndPrefix(node, prefixAlpha); // CORRECT?
			}else{
				// There is more than one path
				fpgrowthMoreThanOnePath(tree, prefixAlpha, prefixSupport, mapSupport);
			}
		}else{ // There is more than one path
			fpgrowthMoreThanOnePath(tree, prefixAlpha, prefixSupport, mapSupport);
		}
	}
	
	/**
	 * Mine an FP-Tree having more than one path.
	 * @param tree  the FP-tree
	 * @param prefix  the current prefix, named "alpha"
	 * @param mapSupport the frequency of items in the FP-Tree
	 * @throws IOException  exception if error writing the output file
	 */
	private void fpgrowthMoreThanOnePath(FPTree tree, int [] prefixAlpha, int prefixSupport, Map<Integer, Integer> mapSupport){
		// For each frequent item in the header table list of the tree in reverse order.
		for(int i= tree.headerList.size()-1; i>=0; i--){
			// get the item
			Integer item = tree.headerList.get(i);
			
			// get the support of the item
			int support = mapSupport.get(item);
			// if the item is not frequent, we skip it
			if(support <  relativeMinsupp){
				continue;
			}
			// Create Beta by concatening Alpha with the current item
			// and add it to the list of frequent patterns
			int [] beta = new int[prefixAlpha.length+1];
			System.arraycopy(prefixAlpha, 0, beta, 0, prefixAlpha.length);
			beta[prefixAlpha.length] = item;
			
			// calculate the support of beta
			int betaSupport = (prefixSupport < support) ? prefixSupport: support;
			// save beta to the output file
			saveItemset(beta, betaSupport);
			
			// === Construct beta's conditional pattern base ===
			// It is a subdatabase which consists of the set of prefix paths
			// in the FP-tree co-occuring with the suffix pattern.
			List<List<FPNode>> prefixPaths = new ArrayList<List<FPNode>>();
			FPNode path = tree.mapItemNodes.get(item);
			while(path != null){
				// if the path is not just the root node
				if(path.parent.itemID != -1){
					// create the prefixpath
					List<FPNode> prefixPath = new ArrayList<FPNode>();
					// add this node.
					prefixPath.add(path);   // NOTE: we add it just to keep its support,
					// actually it should not be part of the prefixPath
					
					//Recursively add all the parents of this node.
					FPNode parent = path.parent;
					while(parent.itemID != -1){
						prefixPath.add(parent);
						parent = parent.parent;
					}
					// add the path to the list of prefixpaths
					prefixPaths.add(prefixPath);
				}
				// We will look for the next prefixpath
				path = path.nodeLink;
			}
			
			// (A) Calculate the frequency of each item in the prefixpath
			// The frequency is stored in a map such that:
			// key:  item   value: support
			Map<Integer, Integer> mapSupportBeta = new HashMap<Integer, Integer>();
			// for each prefixpath
			for(List<FPNode> prefixPath : prefixPaths){
				// the support of the prefixpath is the support of its first node.
				int pathCount = prefixPath.get(0).counter;  
				 // for each node in the prefixpath,
				// except the first one, we count the frequency
				for(int j=1; j<prefixPath.size(); j++){ 
					FPNode node = prefixPath.get(j);
					// if the first time we see that node id
					if(mapSupportBeta.get(node.itemID) == null){
						// just add the path count
						mapSupportBeta.put(node.itemID, pathCount);
					}else{
						// otherwise, make the sum with the value already stored
						mapSupportBeta.put(node.itemID, mapSupportBeta.get(node.itemID) + pathCount);
					}
				}
			}
			
			// (B) Construct beta's conditional FP-Tree
			// Create the tree.
			FPTree treeBeta = new FPTree();
			// Add each prefixpath in the FP-tree.
			for(List<FPNode> prefixPath : prefixPaths){
				treeBeta.addPrefixPath(prefixPath, mapSupportBeta, relativeMinsupp); 
			}  
			// Create the header list.
			treeBeta.createHeaderList(mapSupportBeta); 
			
			// Mine recursively the Beta tree if the root as child(s)
			if(treeBeta.root.childs.size() > 0){
				// recursive call
				fpgrowth(treeBeta, beta, betaSupport, mapSupportBeta);
			}
		}
		
	}

	/**
	 * This method is for adding recursively all combinations of nodes in a path, concatenated with a given prefix,
	 * to the set of patterns found.
	 * @param nodeLink the first node of the path
	 * @param prefix  the prefix
	 * @param minsupportForNode the support of this path.
	 * @throws IOException exception if error while writing the output file
	 */
	private void addAllCombinationsForPathAndPrefix(FPNode node, int[] prefix){
		// Concatenate the node item to the current prefix
		int [] itemset = new int[prefix.length+1];
		System.arraycopy(prefix, 0, itemset, 0, prefix.length);
		itemset[prefix.length] = node.itemID;

		// save the resulting itemset to the file with its support
		saveItemset(itemset, node.counter);
		
		// recursive call if there is a node link
		if(node.nodeLink != null){
			addAllCombinationsForPathAndPrefix(node.nodeLink, prefix);
			addAllCombinationsForPathAndPrefix(node.nodeLink, itemset);
		}
	}

	/**
	 * Write a frequent itemset that is found to the output file or
	 * keep into memory if the user prefer that the result be saved into memory.
	 */
	private void saveItemset(int [] itemset, int support){
		// increase the number of itemsets found for statistics purpose
		itemsetCount++;
		
		// We sort the itemset before showing it to the user so that it is
		// in lexical order.
		Arrays.sort(itemset);
		
		// if the result should be saved to a file
		if(stream != null){
			// Create a string buffer
			StringBuffer buffer = new StringBuffer();
			// write the items of the itemset
			for(int i=0; i< itemset.length; i++){
				buffer.append(itemset[i]);
				if(i != itemset.length-1){
					buffer.append(' ');
				}
			}
			// Then, write the support
//			buffer.append(" #SUP: ");
//			buffer.append(support);
			// write to file and create a new line
//			writer.write(buffer.toString());
//			writer.newLine();
			stream.put(new DoubleMessage(buffer.toString(), String.valueOf(support),String.valueOf(fimBatchCount),outputStreamId)); 
		}// otherwise the result is kept into memory
		else{
			// create an object Itemset and add it to the set of patterns 
			// found.
			Itemset itemsetObj = new Itemset(itemset);
			itemsetObj.setAbsoluteSupport(support);
			patterns.addItemset(itemsetObj, itemsetObj.size());
		}
	}

	/**
	 * Print statistics about the algorithm execution to System.out.
	 */
	public void printStats() {
		System.out
				.println("=============  FP-GROWTH - STATS =============");
		long temps = endTime - startTimestamp;
		System.out.println(" Transactions count from database : " + transactionCount);
		System.out.print(" Max memory usage: " + memoryLogger.getMaxMemory() + " mb \n");
		System.out.println(" Frequent itemsets count : " + itemsetCount); 
		System.out.println(" Total time ~ " + temps + " ms");
		System.out
				.println("===================================================");
	}

	/**
	 * Get the number of transactions in the last transaction database read.
	 * @return the number of transactions.
	 */
	public int getDatabaseSize() {
		return databaseSize;
	}
}
