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
 * * @author Faisal Moeen
 * A general key-value message class which adds a stream id in the class variables
 * Stream id information helps in determining to which stream does the message belongs to.
 */
public class Message implements ContentEvent {

	/**
	 * To tell if the message is the last message of the stream. This may be required in some applications where
	 * a stream can cease to exist
	 */
	private boolean last=false;
	/**
	 * Id of the stream to which the messge belongs
	 */
	private String streamId;
	/**
	 * The key of the message. Can be any sting value. Duplicates are allowed.
	 */
	private String key;
	/**
	 * The value of the message. Can be any object. Casting may be necessary to the desired type.
	 */
	private Object value;
	/**
	 * 
	 */
	public Message()
	{}
	/**
	 * @param key
	 * @param value
	 * @param isLastEvent
	 * @param streamId
	 */
	public Message(String key, Object value, boolean isLastEvent, String streamId)
	{
		this.key=key;
		this.value = value;
		this.last = isLastEvent;
		this.streamId=streamId;
	}
	/* (non-Javadoc)
	 * @see samoa.core.ContentEvent#getKey()
	 */
	@Override
	public String getKey() {
		// TODO Auto-generated method stub
		return key;
	}

	/* (non-Javadoc)
	 * @see samoa.core.ContentEvent#setKey(java.lang.String)
	 */
	@Override
	public void setKey(String str) {
		// TODO Auto-generated method stub
		this.key = str;
	}

	/* (non-Javadoc)
	 * @see samoa.core.ContentEvent#isLastEvent()
	 */
	@Override
	public boolean isLastEvent() {
		// TODO Auto-generated method stub
		return last;
	}
	
	/**
	 * @return value of the message
	 */
	public String getValue()
	{
		return value.toString();
	}
	
	/**
	 * @return id of the stream to which the message belongs
	 */
	public String getStreamId() {
		return streamId;
	}
	/**
	 * @param streamId
	 */
	public void setStreamId(String streamId) {
		this.streamId = streamId;
	}

}
