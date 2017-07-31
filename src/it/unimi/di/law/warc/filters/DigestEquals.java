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
