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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//RELEASE-STATUS: DIST

/** A thread that continuously dequeues a {@link VisitState} from the {@link Frontier#done} queue and
 *  {@link Workbench#release(VisitState) releases} it to the {@link Workbench}.
 *
 * @see TodoThread
 */
public final class DoneThread extends Thread {
	private static final Logger LOGGER = LoggerFactory.getLogger(DoneThread.class);

	/** A reference to the frontier. */
	private final Frontier frontier;
	/** When set to true, this thread will complete its execution. */
	public volatile boolean stop;

	/** A {@link DoneThread} for the given {@link Frontier}.
	 *
	 * @param frontier the frontier.
	 */
	public DoneThread(final Frontier frontier) {
		this.frontier = frontier;
		setName(this.getClass().getSimpleName());
		setPriority(Thread.MAX_PRIORITY);
	}

	@Override
	public void run() {
		try {
			main: while(! stop) {
				VisitState visitState;
				for (int i = 0; (visitState = frontier.done.poll()) == null; i++) {
					if (stop) break main;
					Thread.sleep(1 << Math.min(i, 10));
				}

				do {
					// We do not schedule for refill purged visit states
					if (visitState.nextFetch != Long.MAX_VALUE && frontier.virtualizer.count(visitState) > 0 && visitState.isEmpty()) frontier.refill.add(visitState);
					frontier.workbench.release(visitState);
				} while((visitState = frontier.done.poll()) != null);
			}
		}
		catch (Throwable t) {
			LOGGER.error("Unexpected exception", t);
		}
		LOGGER.info("Completed");
	}
}
