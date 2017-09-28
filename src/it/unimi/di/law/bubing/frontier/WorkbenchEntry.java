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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.PriorityQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.http.conn.DnsResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//RELEASE-STATUS: DIST

/** An element of the {@link Workbench}.
 *
 * <p>A workbench entry is associated with a single IP address (actually, to a 64-bit <em>{@linkplain #ipAddress hash}</em> of an
 * IP address, to accommodate for IPv6), and contains a queue of all
 * {@linkplain VisitState visit states} associated with that address, prioritized by {@link VisitState#nextFetch}. The queue can
 * be modified using the synchronized methods {@link #add(VisitState)}, {@link #remove()}, {@link #size()} and {@link #isEmpty()}.
 *
 * <p>A <em>{@linkplain #isEmpty() nonempty}</em> workbench entry is a {@link Delayed} object whose priority is
 * given by the maximum between its {@link #nextFetch} and the {@link VisitState#nextFetch} of the top
 * of the queue. The {@linkplain #getDelay(TimeUnit) delay}, if any, is given by the priority minus
 * {@link System#currentTimeMillis()}. This setup makes it possible to just {@linkplain DelayQueue#take() take}
 * a {@link WorkbenchEntry} from the {@link Workbench} and remove its top {@link VisitState}, with the guarantee that
 * it is ready to be visited. Moreover, if there at least one {@link VisitState} that is ready to be visited,
 * the method will not block.
 *
 * <p>A workbench entry is either {@link #acquired} or not. If it not acquired, it is on the
 * workbench if and only if it is nonempty.
 */
public final class WorkbenchEntry implements Delayed {
	private static final Logger LOGGER = LoggerFactory.getLogger(WorkbenchEntry.class);
	private static final boolean ASSERTS = false;

	/** The queue of visit states with the IP address associated with this entry. Access to this
	 * member <strong>must</strong> happen through the synchronized methods
	 * {@link #add(VisitState)}, {@link #remove()}, {@link #size()} and {@link #isEmpty()}. */
	private final PriorityQueue<VisitState> visitStates;
	/** The IP address of this workbench entry, computed by {@link DnsResolver#resolve(String)}. */
	public final byte[] ipAddress;
	/** A cached reference to {@link Workbench#broken}. */
	private final AtomicLong workbenchBroken;
	/** The number of broken (i.e., {@link VisitState#lastExceptionClass} &ne; {@code null}) visit states in {@link #visitStates}.*/
	private int brokenVisitStates;
	/** Whether this entry has been {@linkplain Workbench#acquire() acquired}. */
	protected boolean acquired;
	/** The minimum time at which {@linkplain VisitState visit states} in this entry can be accessed because of IP-based politeness. */
	protected volatile long nextFetch;

	/** Creates a workbench entry for a given IP address.
	 *
	 * @param ipAddress the IP address.
	 * @param brokenVisitStates a reference to {@link Frontier#brokenVisitStates}.
	 */
	public WorkbenchEntry(final byte[] ipAddress, final AtomicLong brokenVisitStates) {
		this.ipAddress = ipAddress;
		this.workbenchBroken = brokenVisitStates;
		this.visitStates = new PriorityQueue<>();
	}

	/** Returns true if this entry is nonempty and all its visit states are broken (i.e., {@link VisitState#lastExceptionClass} &ne; {@code null})
	 *
	 * @return true if this entry is nonempty and all its visit states are broken.
	 */
	public synchronized boolean isEntirelyBroken() {
		return brokenVisitStates != 0 && brokenVisitStates == visitStates.size();
	}

	/** Adds the given visit state to the visit-state queue.
	 *
	 * <p>This method is used for deserialization only.
	 *
	 * @param visitState the visit state that must be added to the visit-state queue.
	 * @see PriorityQueue#add(Object)
	 */
	public synchronized void add(VisitState visitState) {
		final boolean wasEntirelyBroken = isEntirelyBroken();
		if (visitState.lastExceptionClass != null) brokenVisitStates++;
		visitStates.add(visitState);
		assert brokenVisitStates <= visitStates.size();
		if (wasEntirelyBroken && ! isEntirelyBroken()) workbenchBroken.decrementAndGet();
		if (! wasEntirelyBroken && isEntirelyBroken()) workbenchBroken.incrementAndGet();
	}

	/** Removes the top element from the visit-state queue.
	 *
	 * @return the top element from the visit-state queue.
	 * @see PriorityQueue#remove()
	 */
	public synchronized VisitState remove() {
		final boolean wasEntirelyBroken = isEntirelyBroken();
		final VisitState visitState = visitStates.remove();
		if (visitState.lastExceptionClass != null) brokenVisitStates--;
		assert brokenVisitStates >= 0;
		assert brokenVisitStates <= visitStates.size();
		if (wasEntirelyBroken && ! isEntirelyBroken()) workbenchBroken.decrementAndGet();
		if (! wasEntirelyBroken && isEntirelyBroken()) workbenchBroken.incrementAndGet();
		return visitState;
	}

	/** Puts this entry on the workbench, if not {@linkplain #isEmpty() empty}.
	 *
	 * <ul>
	 * <li>Preconditions: {@link #acquired}.
	 * <li>Postconditions: not {@link #acquired}.
	 * </ul>
	 *
	 * @param workbench the workbench.
	 */
	public synchronized void putOnWorkbenchIfNotEmpty(final Workbench workbench) {
		assert acquired : this;
		acquired = false;
		if (! isEmpty()) workbench.add(this);
	}

	/** Adds the given visit state to the visit-state queue, and adds this entry to the workbench if it was empty
	 * and not {@linkplain #acquired}.
	 *
	 * @param visitState the nonempty visit state that must be added to the visit-state queue.
	 * @param workbench the workbench.
	 * @see PriorityQueue#add(Object)
	 */
	public synchronized void add(VisitState visitState, Workbench workbench) {
		assert ! visitState.isEmpty() : visitState;
		if (ASSERTS) if (visitStates.contains(visitState)) LOGGER.error("Visit state " + visitState + " already in this workbench entry (" + Arrays.asList(Thread.currentThread().getStackTrace()));
		final boolean wasEmpty = isEmpty();
		add(visitState);
		if (wasEmpty && ! acquired) workbench.add(this);
	}

	/** Returns the number of visit states currently in the visit-state queue.
	 *
	 * @return the number of visit states currently in the visit-state queue.
	 */
	public synchronized int size() {
		return visitStates.size();
	}

	/** Returns the visit states currently in the queue.
	 *
	 * <p>This method is mainly useful to compute statistics
	 * The returned array is created with the current queue content, and it will not
	 * reflect changes of the queue. In particular, some visit states might have
	 * an empty URL queue because of concurrent modifications.
	 *
	 * @return the visit states currently in the queue.
	 * @see PriorityQueue#toArray()
	 */
	public synchronized VisitState[] visitStates() {
		return visitStates.toArray(new VisitState[visitStates.size()]);
	}

	/** Returns true if the visit-state queue is not empty.
	 *
	 * @return true if the visit-state queue is not empty.
	 */
	public synchronized boolean isEmpty() {
		return visitStates.isEmpty();
	}

	/** Returns the minimum time at which some URL in some {@link VisitState} of the visit-state queue
	 * of this workbench entry can be accessed.
	 * It is computed by maximizing {@link #nextFetch} with {@link VisitState#nextFetch} of the
	 * top element of the visit-state queue.
	 *
	 * <p>Note that this method will access blindly the top element, which might cause a {@link NullPointerException}
	 * if the visit-state queue is empty. Thus, great care must be exercised in placing on the workbench
	 * entries with a nonempty visit-state queue only.
	 *
	 * @return the minimum time at which some URL in some {@link VisitState} of this entry can be accessed.
	 */
	public synchronized long nextFetch() {
		assert visitStates.peek() != null;
		return Math.max(nextFetch, visitStates.peek().nextFetch);
	}

	@Override
	public long getDelay(final TimeUnit unit) {
		return unit.convert(Math.max(0, nextFetch() - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
	}

	@Override
	public int compareTo(final Delayed o) {
		return Long.signum(nextFetch() - ((WorkbenchEntry)o).nextFetch());
	}

	@Override
	public synchronized String toString() {
		try {
			return "[" + InetAddress.getByAddress(ipAddress) + " (" + visitStates.size() + ")]";
		}
		catch (UnknownHostException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}
}
