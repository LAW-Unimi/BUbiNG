package it.unimi.di.law.warc.util;

/*
 * Copyright (C) 2013-2017 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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
import it.unimi.di.law.warc.records.RandomTestMocks;
import it.unimi.dsi.fastutil.io.InspectableFileCachedInputStream;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

public class InspectableCachedHttpEntityTest {

	final static int RND_RECORDS = 100;
	final static int RECORDS = 10;
	final static int MAX_NUMBER_OF_HEADERS = 50;
	final static int MAX_LENGTH_OF_HEADER = 20;
	final static int MAX_LENGTH_OF_BODY = 10 * 1024;

	@Test
	public void testCopyContent() throws IOException {
		final RandomTestMocks.HttpResponse mockResponse = new RandomTestMocks.HttpResponse(MAX_NUMBER_OF_HEADERS, MAX_LENGTH_OF_HEADER, MAX_LENGTH_OF_BODY, 0);
		final InspectableFileCachedInputStream inputStream = new InspectableFileCachedInputStream();
		final InspectableCachedHttpEntity wrappedEntity = new InspectableCachedHttpEntity(inputStream);
		wrappedEntity.setEntity(mockResponse.getEntity());
		wrappedEntity.copyContent(Long.MAX_VALUE, System.currentTimeMillis(), Long.MAX_VALUE, 0);
		assertEquals(mockResponse.getMockContent(), IOUtils.toString(wrappedEntity.getContent(), StandardCharsets.ISO_8859_1));
		inputStream.close();
	}

}
