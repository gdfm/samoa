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
import java.io.PrintWriter;

import com.github.javacliparser.Configurable;
import com.github.javacliparser.StringOption;
import com.yahoo.labs.samoa.core.ContentEvent;
import com.yahoo.labs.samoa.core.Processor;
import com.yahoo.labs.samoa.fpm.messages.Message;

public class FileWriterProcessor implements Processor{

	private int taskId;
	private PrintWriter p;
	public StringOption outputFilePathOption = new StringOption("outputFilePath", 'o',
			"Output transactions file",
			"./output");
	@Override
	public boolean process(ContentEvent event) {
		Message message = (Message)event;
		if(message.getStreamId().compareTo("outputControlStream")==0)
		{
			if(message.getKey().compareTo("batchId")==0)
			{
				try {
					p.flush();
					p.close();
					this.p = new PrintWriter(new File(this.outputFilePathOption.getValue()+"-"+message.getValue()));
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		else
		{
			p.println(message.getKey()+"--------"+message.getValue());
		}
		return false;
	}

	@Override
	public void onCreate(int id) {
		this.taskId=id;
		try {
			this.p = new PrintWriter(new File(this.outputFilePathOption.getValue()));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public Processor newProcessor(Processor p) {
		return new FileWriterProcessor();
	}

}
