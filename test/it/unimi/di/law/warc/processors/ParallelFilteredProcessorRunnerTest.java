package it.unimi.di.law.warc.processors;

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

import it.unimi.di.law.warc.io.RandomReadWritesTest;
import it.unimi.di.law.warc.records.WarcRecord;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.junit.BeforeClass;
import org.junit.Test;

public class ParallelFilteredProcessorRunnerTest {

	final static int TEST_RECORDS = 200;
	final static String PATH = "/tmp/warc.gz";

	static int[] position;

	@BeforeClass
	public static void init() throws IOException, InterruptedException {
		final WarcRecord[] randomRecords = RandomReadWritesTest.prepareRndRecords();
		position = RandomReadWritesTest.writeRecords(PATH, TEST_RECORDS, randomRecords, 1); // 1 stands for compressed!
	}

	@Test
	public void sequentialReads() throws Exception {
		final InputStream in = new FileInputStream(PATH);
		new ParallelFilteredProcessorRunner(in).add(ResponseContentExtractor.getInstance(), ToStringWriter.getInstance(), System.out).runSequentially();
	}

	@Test
	public void parallelReads() throws Exception {
		final InputStream in = new FileInputStream(PATH);
		new ParallelFilteredProcessorRunner(in).add(ResponseContentExtractor.getInstance(), ToStringWriter.getInstance(), System.out).run();
	}
}
