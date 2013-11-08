package com.yahoo.labs.samoa.fpm.exceptions;

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

public class OutOfRangeException extends IllegalArgumentException {

	private final double value, min, max;

	public OutOfRangeException(double value, double min, double max) {
		super("Value " + value + " out of range " +
				"[" + min + ".." + max + "]");
		this.value = value;
		this.min = min;
		this.max = max;
	}

	public double getValue() {
		return value;
	}

	public double getMin() {
		return min;
	}

	public double getMax() {
		return max;
	}

}
