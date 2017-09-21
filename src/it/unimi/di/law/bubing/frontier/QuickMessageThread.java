package it.unimi.di.law.bubing.frontier;

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

import it.unimi.di.law.bubing.util.ByteArrayDiskQueue;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//RELEASE-STATUS: DIST

/** A thread that takes care of pouring the content of {@link Frontier#quickReceivedURLs} into {@link Frontier#receivedURLs}.
 * The {@link #run()} method waits on the {@link Frontier#quickReceivedURLs} queue, checking that {@link #stop} becomes true every second. */

public final class QuickMessageThread extends Thread {
	private static final Logger LOGGER = LoggerFactory.getLogger(QuickMessageThread.class);
	/** A reference to the frontier. */
	private final Frontier frontier;

	/** Creates the thread.
	 *
	 * @param frontier the frontier instantiating the thread.
	 */
	public QuickMessageThread(final Frontier frontier) {
		setName(this.getClass().getSimpleName());
		setPriority(Thread.MAX_PRIORITY); // This must be done quickly
		this.frontier = frontier;
	}

	/** When set to true, this thread will complete its execution. */
	public volatile boolean stop;

	@Override
	public void run() {
		try {
			final ByteArrayDiskQueue receivedURLs = frontier.receivedURLs;
			final ArrayBlockingQueue<ByteArrayList> quickReceivedURLs = frontier.quickReceivedURLs;
			while(! stop) {
				final ByteArrayList list = quickReceivedURLs.poll(1, TimeUnit.SECONDS);
				if (list != null) receivedURLs.enqueue(list.elements(), 0, list.size());
			}
		}
		catch (Throwable t) {
			LOGGER.error("Unexpected exception ", t);
		}

		LOGGER.info("Completed");
	}
}
