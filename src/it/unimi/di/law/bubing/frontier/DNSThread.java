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

import it.unimi.di.law.bubing.RuntimeConfiguration;
import it.unimi.di.law.bubing.util.BURL;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Ints;

//RELEASE-STATUS: DIST

/** A thread that continuously dequeues a {@link VisitState} from the {@linkplain Frontier#newVisitStates queue of new visit states} (those that still need a DNS resolution),
 *  resolves its host and puts it on the {@link Workbench}. The number of instances of this thread
 *  is {@linkplain RuntimeConfiguration#dnsThreads configurable}.
 *
 *  <p>Note that <em>only instances of this class manipulate {@linkplain Workbench#address2WorkbenchEntry the map
 *  from IP address hashes to workbench entries}</em>. The map needs to be updated with a get-and-set atomic
 *  operation, so synchronization is explicit.
 */

public final class DNSThread extends Thread {
	private static final Logger LOGGER = LoggerFactory.getLogger(DNSThread.class);
	/** Whether we should stop (used also to reduce the number of threads). */
	public volatile boolean stop;

	/** A reference to the frontier. */
	private final Frontier frontier;

	/** A DNS thread for the given {@link Frontier}, with an index used to set the thread's name.
	 *
	 * @param frontier the frontier.
	 * @param index the index of this thread
	 */
	public DNSThread(final Frontier frontier, final int index) {
		setName(this.getClass().getSimpleName() + '-' + index);
		this.frontier = frontier;
	}

	@Override
	public void run() {
		while(! stop) {
			try {
				frontier.rc.ensureNotPaused();

				// Try to get an available unknown host.
				VisitState visitState = frontier.unknownHosts.poll();
				// If none, try to get a new visit state.
				if (visitState == null) visitState = frontier.newVisitStates.poll(1, TimeUnit.SECONDS);
				// If none after one second, try again.
				if (visitState == null) continue;

				final String host = BURL.hostFromSchemeAndAuthority(visitState.schemeAuthority);

				try {
					// This is the first point in which DNS resolution happens for new hosts.
					if (LOGGER.isDebugEnabled()) LOGGER.debug("Resolving host {} with DNS because of URL {}", host, BURL.fromNormalizedSchemeAuthorityAndPathQuery(visitState.schemeAuthority, visitState.firstPath()));
					final byte[] address = frontier.rc.dnsResolver.resolve(host)[0].getAddress();

					if (address.length == 4) {
						Lock lock = frontier.rc.blackListedIPv4Lock.readLock();
						lock.lock();
						try {
							if (frontier.rc.blackListedIPv4Addresses.contains(Ints.fromByteArray(address))) {
								LOGGER.warn("Visit state for host {} was not created and rather scheduled for purge because its IP {} was blacklisted", host, Arrays.toString(address));
								visitState.schedulePurge();
								continue;
							}
						} finally {
							lock.unlock();
						}
					}

					visitState.lastExceptionClass = null; // In case we had previously set UnknownHostException.class
					// Fetch or create atomically a new workbench entry.
					visitState.setWorkbenchEntry(frontier.workbench.getWorkbenchEntry(address));
				}
				catch(UnknownHostException e) {
					LOGGER.warn("Unknown host " + host + " for visit state " + visitState);

					if (visitState.lastExceptionClass != UnknownHostException.class) visitState.retries = 0;
					else visitState.retries++;

					visitState.lastExceptionClass = UnknownHostException.class;

					if (visitState.retries < ParsingThread.EXCEPTION_TO_MAX_RETRIES.getInt(UnknownHostException.class)) {
						final long delay = ParsingThread.EXCEPTION_TO_WAIT_TIME.getLong(UnknownHostException.class) << visitState.retries;
						// Exponentially growing delay
						visitState.nextFetch = System.currentTimeMillis() + delay;
						LOGGER.info("Will retry DNS resolution of state " + visitState + " with delay " + delay);
						frontier.unknownHosts.add(visitState);
					}
					else {
						visitState.schedulePurge();
						LOGGER.warn("Visit state " + visitState + " killed by " + UnknownHostException.class.getSimpleName());
					}
				}
			}
			catch(Throwable t) {
				LOGGER.error("Unexpected exception", t);
			}
		}

		if (LOGGER.isDebugEnabled()) LOGGER.debug("Completed");
	}


}
