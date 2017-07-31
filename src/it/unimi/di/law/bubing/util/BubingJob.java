package it.unimi.di.law.bubing.util;

/*
 * Copyright (C) 2012-2013 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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

import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.jai4j.Job;

import java.io.Serializable;

import com.google.common.base.Charsets;

//RELEASE-STATUS: DIST

/** The JAI4J {@link it.unimi.dsi.jai4j.Job} used by BUbiNG. */

public class BubingJob implements Job, Serializable {
	private static final long serialVersionUID = 1L;
	/** The {@linkplain BURL BUbiNG URL} that should be visited. */
	public final ByteArrayList url;

	/** Creates a new BUbiNG job corresponding to a given {@linkplain BURL BUbiNG URL}.
	 *
	 * @param url the {@linkplain BURL BUbiNG URL} for this job.
	 */
	public BubingJob(final ByteArrayList url) {
		this.url = url;
	}

	/** A hash based on the host of {@link #url}.
	 *
	 * return a hash based on the host of {@link #url}.
	 */
	@Override
	public long hash64() {
		final byte[] urlBuffer = url.elements();
		final int startOfHost = BURL.startOfHost(urlBuffer);
		return MurmurHash3.hash(urlBuffer, startOfHost, BURL.lengthOfHost(urlBuffer, startOfHost));
	 }

	 /**
	  * A string representation of this job
	  *
	  * @return the URI of this job in string format
	  */
	 @Override
	public String toString() {
		 return "[" + new String(url.elements(), 0, url.size(), Charsets.ISO_8859_1) + "]";
	 }
}
