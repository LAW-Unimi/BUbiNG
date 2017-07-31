package it.unimi.di.law.warc.filters;

/*
 * Copyright (C) 2004-2013 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
 *
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 3 of the License, or (at your option)
 *  any later version.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
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
