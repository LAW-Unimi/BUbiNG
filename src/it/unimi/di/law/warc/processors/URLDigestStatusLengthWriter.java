package it.unimi.di.law.warc.processors;

import java.io.IOException;
import java.io.PrintStream;

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

// RELEASE-STATUS: DIST

import it.unimi.di.law.warc.processors.ParallelFilteredProcessorRunner.Writer;
import it.unimi.di.law.warc.records.HttpResponseWarcRecord;
import it.unimi.di.law.warc.records.WarcHeader;
import it.unimi.di.law.warc.records.WarcHeader.Name;
import it.unimi.di.law.warc.records.WarcRecord;

public class URLDigestStatusLengthWriter implements Writer<WarcRecord> {

	@Override
	public void write(final WarcRecord warcRecord, final long storePosition, final PrintStream out) throws IOException {
		out.print(warcRecord.getWarcTargetURI());
		out.print('\t');
		out.print(warcRecord.getWarcHeader(Name.WARC_PAYLOAD_DIGEST).getValue());
		out.print('\t');
		out.print(((HttpResponseWarcRecord)warcRecord).getStatusLine().getStatusCode());
		out.print('\t');
		out.print(warcRecord.getWarcHeader(WarcHeader.Name.BUBING_IS_DUPLICATE));
		out.print('\t');
		out.print(((HttpResponseWarcRecord)warcRecord).getEntity().getContentLength());
		out.write('\n');
	}

	@Override
	public void close() throws IOException {}
}
