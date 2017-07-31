package it.unimi.di.law.bubing.frontier;

/*
 * Copyright (C) 2012-2013 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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
