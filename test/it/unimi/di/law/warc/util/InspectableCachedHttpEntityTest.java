package it.unimi.di.law.warc.util;

/*
 * Copyright (C) 2013 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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
