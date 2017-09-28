package it.unimi.di.law.warc.processors;

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
