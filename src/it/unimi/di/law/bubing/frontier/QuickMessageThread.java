package it.unimi.di.law.bubing.frontier;

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
