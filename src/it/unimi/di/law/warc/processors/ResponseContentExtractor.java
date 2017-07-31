package it.unimi.di.law.warc.processors;

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

// RELEASE-STATUS: DIST

import it.unimi.di.law.warc.processors.ParallelFilteredProcessorRunner.Processor;
import it.unimi.di.law.warc.records.HttpResponseWarcRecord;
import it.unimi.di.law.warc.records.WarcRecord;

import java.io.IOException;

import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResponseContentExtractor implements Processor<String> {
	private static final Logger LOGGER = LoggerFactory.getLogger(ResponseContentExtractor.class);

	private static final ResponseContentExtractor INSTANCE = new ResponseContentExtractor();
	private ResponseContentExtractor() {}

	public static ResponseContentExtractor getInstance() {
		return INSTANCE;
	}

	@Override
	public String process(final WarcRecord r, final long storePosition) {
		if (r.getWarcType() != WarcRecord.Type.RESPONSE) return null;
		HttpResponseWarcRecord resp = (HttpResponseWarcRecord) r;
		try {
			return EntityUtils.toString(resp.getEntity());
		}
		catch (Exception e) {
			// We must ALWAYS return a result (or the reordering queue will deadlock).
			LOGGER.error("Unexpected exception during entity convertion to string", e);
			return null;
		}
	}
	@Override
	public void close() throws IOException {}

	@Override
	public ResponseContentExtractor copy() {
		return INSTANCE;
	}
}
