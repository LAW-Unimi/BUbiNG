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


import java.util.Iterator;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.Iterators;

//RELEASE-STATUS: DIST

/** <p>The workbench is a {@link DelayQueue} queue of {@link WorkbenchEntry} instances, each associated with an IP address.
 *
 * <p>Each {@link WorkbenchEntry} contains a priority queue of {@link VisitState} instances ordered
 * by {@link VisitState#nextFetch}&mdash;the closest moment in
 * {@linkplain System#currentTimeMillis() time} at which the scheme+authority stored in the
 * {@link VisitState} is fetchable (by politeness policies). Each workbench entry has also a
 * {@link WorkbenchEntry#nextFetch} field with a similar meaning, but associated with the IP address
 * instead that with the scheme+authority, the idea being that the politeness policy for an IP can
 * be more tolerant (i.e., shorter delay) than that for an authority.
 *
 * <p>Observe that there exist, in general, {@linkplain WorkbenchEntry workbench entries} that are not
 * on the workbench, typically because all visit states the are associated with are empty.
 *
 * <p>The workbench is sorted by the <em>maximum between {@link WorkbenchEntry#nextFetch} and the
 * {@link VisitState#nextFetch} of the top of the queue of visit states</em>. This guarantees that
 * the top of the visit-state queue of the workbench entry at the top of the workbench contains a
 * URL that can be fetched by (both authority and IP) politeness if an only if a fetchable URL exists. By setting the value
 * returned by {@link Delayed#getDelay(TimeUnit)} to the truncated difference between the maximum
 * above and {@link System#currentTimeMillis()} we can just wait on a {@link DelayQueue#take()} until
 * the next {@link WorkbenchEntry} is ready (this is what {@link #acquire()} does).
 *
 * <p>A basic invariant is that <em>all workbench entries on the workbench contains nonempty queues
 * of visit states, and every visit state in a workbench entry has nonempty a URL queue</em>. Workbench
 * entries can be out of the workbench either because their top visit state has been
 * acquired by a thread, or because their visit state queue is empty. Visit states can be out
 * of the workbench also if they caused connection problems.
 *
 * <h2>Using a visit state</h2>
 *
 * <p>A {@link VisitState} must be
 * {@linkplain #acquire() acquired} and then {@linkplain #release(VisitState) released}.
 * When a {@link VisitState} is acquired, its {@link WorkbenchEntry} is removed from the workbench,
 * so it is not possible to acquire any other {@link VisitState} with the same IP address. This
 * mechanism guarantees that we never download data using two different {@link FetchingThread} instances
 * from the same IP (and, <i>a fortiori</i>, from the same host or scheme+authority).
 *
 * <p>Once a visit state has been acquired, a visit can be performed by calling
 * {@link VisitState#dequeue()} to obtain (ready) URLs and {@link Frontier#enqueue(it.unimi.dsi.fastutil.bytes.ByteArrayList)} to filter
 * newly discovered URLs. More information can be found in the documentation of {@link VisitState}.
 */

public class Workbench implements Iterable<WorkbenchEntry> {
	/** The set of {@linkplain WorkbenchEntry workbench entries}. */
	protected final WorkbenchEntrySet address2WorkbenchEntry;
	/** The workbench. */
	private final DelayQueue<WorkbenchEntry> entries;
	/** The approximate size of {@link #entries}. */
	private final AtomicLong approximatedSize;
	/** The number of entirely broken entries (i.e., entries containing only broken visit states). */
	public final AtomicLong broken;

	/** Creates the workbench. */
	public Workbench() {
		entries = new DelayQueue<>();
		address2WorkbenchEntry = new WorkbenchEntrySet();
		approximatedSize = new AtomicLong();
		broken = new AtomicLong();
	}

	/** Returns an (unmodifiable) iterator over the entries currently on the workbench.
	 *
	 * @return an (unmodifiable) iterator over the entries currently on the workbench.
	 */
	@Override
	public Iterator<WorkbenchEntry> iterator() {
		return Iterators.unmodifiableIterator(entries.iterator());
	}

	/** Returns a vector containing the known workbench entries and {@code null}s.
	 *
	 * <p>During the crawl, the vector might change concurrently.
	 *
	 * @return a vector containing the known workbench entries and {@code null}s.
	 */
	public WorkbenchEntry[] workbenchEntries() {
		synchronized (address2WorkbenchEntry) {
			return address2WorkbenchEntry.workbenchEntries();
		}
	}

	/** Returns a workbench entry for the given address, possibly creating one. The entry may or may not be on the {@link Workbench}
	 *  currently.
	 *
	 * @param address an IP address in byte-array form.
	 * @return a workbench entry for {@code address} (possibly a new one).
	 */

	public WorkbenchEntry getWorkbenchEntry(final byte[] address) {
		WorkbenchEntry workbenchEntry;
		synchronized (address2WorkbenchEntry) {
			workbenchEntry = address2WorkbenchEntry.get(address);
			if (workbenchEntry == null) address2WorkbenchEntry.add(workbenchEntry = new WorkbenchEntry(address, broken));
		}
		return workbenchEntry;
	}

	/** Returns the number of existing workbench entries (in and out of the workbench).
	 *
	 * @return the number of existing workbench entries.
	 */
	public int numberOfWorkbenchEntries() {
		synchronized (address2WorkbenchEntry) {
			return address2WorkbenchEntry.size();
		}
	}

	/** Adds a nonempty, not acquired workbench entry to the workbench.
	 *
	 * @param entry a nonempty, not acquired workbench entry.
	 */
	public void add(WorkbenchEntry entry) {
		assert ! entry.isEmpty() : entry;
		assert ! entry.acquired : entry;
		entries.add(entry);
		approximatedSize.incrementAndGet();
	}

	/** Acquires a visit state for a scheme+authority accessible by politeness.
	 * Note that this is a blocking method that will wait until such a state is available.
	 *
	 * <p>You <strong>must</strong> call {@link #release(VisitState)} when you have finished.
	 *
	 * @return a visit state with nonempty URL queue
	 * for a scheme+authority accessible by politeness.
	 */
	public VisitState acquire() throws InterruptedException {
		final WorkbenchEntry entry = entries.take();
		assert ! entry.isEmpty();
		// No race condition is possible here, because entry is nonempty.
		assert ! entry.acquired;
		entry.acquired = true;

		final VisitState visitState = entry.remove();
		assert visitState.workbenchEntry == entry;
		assert ! visitState.isEmpty();
		// No race condition is possible here, because visitState is nonempty.
		assert ! visitState.acquired;
		visitState.acquired = true;

		approximatedSize.decrementAndGet();
		return visitState;
	}

	/** Releases a previously {@linkplain #acquire() acquired} visit state.
	 *
	 * @param visitState a previously {@linkplain #acquire() acquired} visit state.
	 */
	public void release(final VisitState visitState) {
		assert visitState.acquired: visitState;
		final WorkbenchEntry workbenchEntry = visitState.workbenchEntry;
		assert workbenchEntry != null;
		assert workbenchEntry.acquired: workbenchEntry;

		visitState.putInEntryIfNotEmpty();
		workbenchEntry.putOnWorkbenchIfNotEmpty(this);
	}

	/** Returns an approximation of the workbench size (in number of entries present on the workbench).
	 *  The value is approximated (with guarantees of being not drifted away from the real value), to allow
	 *  for a less stringent synchronization needs.
	 *
	 * @return an approximation of the workbench size.
	 */
	public long approximatedSize() {
		return approximatedSize.get();
	}
}
