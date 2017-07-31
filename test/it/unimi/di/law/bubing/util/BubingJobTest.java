package it.unimi.di.law.bubing.util;

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

//RELEASE-STATUS: DIST

import static org.junit.Assert.assertEquals;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;

import org.junit.Test;

/** A class to test {@link BURL}. */

public class BubingJobTest {

	private long string2Hash64(String url) {
		return new BubingJob(Util.toByteArrayList(BURL.parse(url).toASCIIString(), new ByteArrayList())).hash64();
	}

	@Test
	public void testHash() {
		assertEquals(string2Hash64("http://example.com/foo.html"), string2Hash64("http://example.com/foo.html:"));
	}

}
