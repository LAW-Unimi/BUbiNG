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
import it.unimi.di.law.warc.records.WarcRecord;

import java.io.IOException;
import java.net.URI;

public class WarcTargetUriExtractor implements Processor<URI> {

	private static final WarcTargetUriExtractor INSTANCE = new WarcTargetUriExtractor();
	private WarcTargetUriExtractor() {}

	public static WarcTargetUriExtractor getInstance() {
		return INSTANCE;
	}

	@Override
	public URI process(final WarcRecord r, final long storePosition) {
		return r.getWarcTargetURI();
	}

	@Override
	public void close() throws IOException {}

	@Override
	public WarcTargetUriExtractor copy() {
		return INSTANCE;
	}
}
