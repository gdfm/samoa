package com.yahoo.labs.samoa.fpm.messages;

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

import com.yahoo.labs.samoa.core.ContentEvent;


/**
 * @author Faisal Moeen
 * A generic message class but contains two values.
 */
public class DoubleMessage extends Message {

	/**
	 *  Value 1
	 */
	private String value1;
	/**
	 *  Value 2
	 */
	private String value2;
	
	
	/**
	 * @param key
	 * @param value1
	 * @param value2
	 * @param The if of the stream to which the message will be sent
	 */
	public DoubleMessage(String key, String value1, String value2, String streamId)
	{
		super(key,value1,false,streamId);
		this.value1 = value1;
		this.value2 = value2;
	}
	/**
	 * @return value1
	 */
	public String getValue1() {
		return value1;
	}
	/**
	 * @param value1
	 */
	public void setValue1(String value1) {
		this.value1 = value1;
	}
	/**
	 * @return value2
	 */
	public String getValue2() {
		return value2;
	}
	/**
	 * @param value2
	 */
	public void setValue2(String value2) {
		this.value2 = value2;
	}

}
