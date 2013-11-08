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

import com.github.javacliparser.Configurable;
import com.github.javacliparser.StringOption;
import com.yahoo.labs.samoa.core.ContentEvent;
import com.yahoo.labs.samoa.core.Processor;
import com.yahoo.labs.samoa.fpm.ReaderInterface;
import com.yahoo.labs.samoa.fpm.messages.Message;
import com.yahoo.labs.samoa.topology.Stream;

public class FileReaderProcessor implements ReaderInterface{

	public StringOption inputFilePathOption = new StringOption("inputFilePath", 'i',
			"Input transactions file path",
			"");
	private int taskId;
	private BufferedReader br;
	private String line;
	private Stream dataStream;
	@Override
	public boolean process(ContentEvent event) {
		return false;
	}

	@Override
	public void onCreate(int id) {
		this.taskId = id;
		try {
			this.br = new BufferedReader(new FileReader(new File(this.inputFilePathOption.getValue())));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	@Override
	public Processor newProcessor(Processor p) {
		return null;
	}

	@Override
	public String nextTuple() {
		try {
			if((line=br.readLine())!=null)
				this.dataStream.put(new Message(null,line , false, ""));
			else
				try {
					System.out.println("Sleeping for 5s");
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public Stream getDataStream() {
		return dataStream;
	}

	public void setDataStream(Stream dataStream) {
		this.dataStream = dataStream;
	}

	
}
