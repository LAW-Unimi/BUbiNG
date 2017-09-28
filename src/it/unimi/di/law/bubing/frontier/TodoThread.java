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

import java.util.concurrent.DelayQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//RELEASE-STATUS: DIST

/** A thread that continuously {@linkplain Workbench#acquire() acquires} a {@link VisitState} from the {@link Workbench} and adds it
 *  to the {@link Frontier#todo} queue. Note that we use the {@linkplain DelayQueue#take() blocking dequeue method}, which will take
 *  care of delays due to politeness, so this thread does not need to do any busy polling.
 *  {@link FetchingThread} instances will later remove the {@link VisitState} from the {@link Frontier#todo} queue.
 *
 *  <p>This design has been chosen so to minimize contention on the {@link Workbench}, as its
 *  state changes require logarithmic time. Only this thread, the {@link DoneThread}, the {@link Distributor} and {@link DNSThread}/{@link ParsingThread} instances
 *  access directly the {@link Workbench}&mdash;access by {@link FetchingThread} and {@link ParsingThread} instances is mediated
 *  by the {@link Frontier#todo} wait-free queue. As a side-effect, if a {@link VisitState} with a very low {@link VisitState#nextFetch}
 *  value comes back to life, it will be placed in the correct position on the  {@link Workbench},
 *  but not in the {@link Frontier#todo} queue.
 */
public final class TodoThread extends Thread {
	private static final Logger LOGGER = LoggerFactory.getLogger(TodoThread.class);

	/** A reference to the frontier. */
	private final Frontier frontier;

	/** Instantiates the thread.
	 *
	 * @param frontier the frontier instantiating the thread.
	 */
	public TodoThread(final Frontier frontier) {
		this.frontier = frontier;
		setName(this.getClass().getSimpleName());
		setPriority(Thread.MAX_PRIORITY);
	}

	@Override
	public void run() {
		try {
			while(! Thread.currentThread().isInterrupted()) {
				VisitState visitState = frontier.workbench.acquire();
				assert frontier.schemeAuthority2Count.get(visitState.schemeAuthority) <= frontier.rc.maxUrlsPerSchemeAuthority : frontier.schemeAuthority2Count.get(visitState.schemeAuthority) + " > " + frontier.rc.maxUrlsPerSchemeAuthority;
				frontier.todo.add(visitState);
			}
		}
		catch (InterruptedException e) {
			LOGGER.info("Interrupted");
		}
		catch (Throwable t) {
			LOGGER.error("Unexpected exception", t);
		}

		LOGGER.info("Completed");
	}
}
