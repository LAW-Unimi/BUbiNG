package it.unimi.di.law.bubing.frontier;

/*
 * Copyright (C) 2012-2017 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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
import it.unimi.di.law.bubing.util.Util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//RELEASE-STATUS: DIST

/** A thread that takes care of pouring the content of {@link Frontier#receivedURLs} into the {@link Frontier} itself (via the
 *  {@link Frontier#enqueue(it.unimi.dsi.fastutil.bytes.ByteArrayList)} method). The {@link #run()} method performs a busy polling on the {@link Frontier#receivedURLs}
 *  queue, at exponentially spaced time intervals (but anyway not less infrequently than 1s).
 */
public final class MessageThread extends Thread {
	private static final Logger LOGGER = LoggerFactory.getLogger(MessageThread.class);

	/** A reference to the frontier. */
	private final Frontier frontier;

	/** Creates the thread.
	 *
	 * @param frontier the frontier instantiating this thread.
	 */
	public MessageThread(final Frontier frontier) {
		setName(this.getClass().getSimpleName());
		this.frontier = frontier;
	}

	/** When set to true, this thread will complete its execution. */
	public volatile boolean stop;

	@Override
	public void run() {
		try {
			final ByteArrayDiskQueue receivedURLs = frontier.receivedURLs;
			for(;;) {
				for(int round = 0; frontier.rc.paused || receivedURLs.isEmpty() || ! frontier.agent.isConnected() || frontier.agent.getAliveCount() == 0; round++) {
					if (stop) return;
					Thread.sleep(1 << Math.min(10, round));
				}
				receivedURLs.dequeue();
				if (LOGGER.isTraceEnabled()) LOGGER.trace("Dequeued URL {} from the message queue", Util.toString(receivedURLs.buffer()));
				frontier.numberOfReceivedURLs.incrementAndGet();
				frontier.enqueueLocal(receivedURLs.buffer());
			}
		}
		catch (Throwable t) {
			LOGGER.error("Unexpected exception ", t);
		}

		LOGGER.info("Completed");
	}
}
