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

import it.unimi.di.law.warc.records.WarcHeader;
import it.unimi.di.law.warc.records.WarcRecord;
import it.unimi.di.law.warc.util.Util;

import java.util.Arrays;

import org.apache.http.Header;

// RELEASE-STATUS: DIST

/** A filter accepting only records of given digest, specified as a hexadecimal string. */
public class DigestEquals extends AbstractFilter<WarcRecord> {

	/** The digest of accepted records. */
	private final byte[] digest;

	// TODO: PORTING: how we treat different digesting algorithms

	/**
	 * Create a filter that accepts only records of given digest, specified as a hexadecimal string
	 *
	 * @param digest the digest of accepted records
	 */
	private DigestEquals(final byte[] digest) {
		this.digest = digest;
	}

	/**
	 * Apply the filter to a given WarcRecord
	 *
	 * @param x the WarcRecord to be filtered
	 * @return <code>true</code> if the <code>WarcHeader.Name.WARC_PAYLOAD_DIGEST</code> of the <code>WarcHeader</code> of <code>x</code> is not null and equal to the inner digest (hexadecimal comparison)
	 */
	@Override
	public boolean apply(final WarcRecord x) {
		Header s = x.getWarcHeader(WarcHeader.Name.WARC_PAYLOAD_DIGEST);
		return s != null && Arrays.equals(digest, Util.fromHexString(s.getValue()));
	}

	/**
	 * Get a new <code>DigestEquals</code> that will accept only <code>WarcRecord</code> whose digest is a given string
	 *
	 * @param spec a string, that will be used to compare digests.
	 * @return a new <code>DigestEquals</code> that will accept only <code>WarcRecord</code> whose digest is <code>spec</code>
	 */
	public static DigestEquals valueOf(final String spec) {
		return new DigestEquals(Util.fromHexString(spec));
	}

	/**
	 * A string representation of the state of this object, that is just the digest allowed.
	 *
	 * @return the string representation of the digest used by this object to filter records
	 */
	@Override
	public String toString() {
		return toString(Util.toHexString(digest));
	}

	@Override
	public Filter<WarcRecord> copy() {
		return this;
	}

}
