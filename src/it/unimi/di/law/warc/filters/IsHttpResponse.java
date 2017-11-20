package it.unimi.di.law.warc.filters;

/*
 * Copyright (C) 2004-2017 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import it.unimi.di.law.warc.records.HttpResponseWarcRecord;
import it.unimi.di.law.warc.records.WarcHeader;
import it.unimi.di.law.warc.records.WarcRecord;

import org.apache.http.Header;

// RELEASE-STATUS: DIST

/** A filter accepting only records that are http/https responses. */
public class IsHttpResponse extends AbstractFilter<WarcRecord> {

	public final static IsHttpResponse INSTANCE = new IsHttpResponse();

	private IsHttpResponse() {}

	/**
	 * Apply the filter to a WarcRecord
	 *
	 * @param x the <code>WarcRecord</code> to be filtered
	 * @return <code>true</code> if <code>x</code> is an http/https response.
	 */
	@Override
	public boolean apply(final WarcRecord x) {
		Header messageType = x.getWarcHeader(WarcHeader.Name.CONTENT_TYPE);
		return (x.getWarcType() == WarcRecord.Type.RESPONSE) &&
				(messageType != null && messageType.getValue().equals(HttpResponseWarcRecord.HTTP_RESPONSE_MSGTYPE));
	}

	/**
	 * Get a new IsHttpResponse that will accept only WarcRecords that are http/https responses.
	 *
	 * @param emptySpec an empty String.
	 * @return a new <code>IsHttpResponse</code> that will accept only <code>WarcRecord</code>s that are http/https responses.
	 */
	public static IsHttpResponse valueOf(final String emptySpec) {
		if (emptySpec.length() > 0) throw new IllegalArgumentException();
		return INSTANCE;
	}

	/**
	 * A string representation of the state of this object.
	 *
	 * @return the class name of this + "()"
	 */
	@Override
	public String toString() {
		return getClass().getSimpleName() + "()";
	}

	@Override
	public Filter<WarcRecord> copy() {
		return this;
	}
}
