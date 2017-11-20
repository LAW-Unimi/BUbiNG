package it.unimi.di.law.bubing.util;

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
