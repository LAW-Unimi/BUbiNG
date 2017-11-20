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

import it.unimi.dsi.Util;
import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.stat.SummaryStats;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongArray;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//RELEASE-STATUS: DIST

/** A class isolating a number of {@link ProgressLogger} instances keeping track of a number of
 *  quantities of interest related to the {@link Distributor}, e.g.,
 * {@linkplain #requestLogger requests}, {@linkplain #transferredBytesLogger transferred byets}, etc. */
public final class StatsThread implements Runnable {
	private static final Logger LOGGER = LoggerFactory.getLogger(StatsThread.class);
	/** A reference to the frontier. */
	private final Frontier frontier;
	/** A reference to the distributor. */
	private final Distributor distributor;
	/** A global progress logger, measuring the number of completed requests. */
	public final ProgressLogger requestLogger;
	/** A global progress logger, measuring the number of non-duplicate resources actually stored. */
	public final ProgressLogger resourceLogger;
	/** A global progress logger, measuring the number of transferred bytes. */
	public final ProgressLogger transferredBytesLogger;
	/** A global progress logger, counting the URLs received from other agents. */
	public final ProgressLogger receivedURLsLogger;

	/** A variable used for exponentially-binned distribution of visit state sizes. */
	public volatile int dist[] = new int[0];
	/** The number of path+queries living in an unresolved visit state. */
	public volatile long unresolved = 0;
	/** The number of path+queries living in a broken visit state. */
	public volatile long brokenPathQueryCount = 0;
	/** A variable accumulating statistics about the size (in visit states) of {@linkplain WorkbenchEntry workbench entries}. */
	public volatile SummaryStats entrySummaryStats;
	/** The number of resolved visit states. */
	public volatile long resolvedVisitStates;
	/** The number of broken visit states on the workbench. */
	public volatile long brokenVisitStatesOnWorkbench;

	/** Creates the thread.
	 *
	 * @param frontier the frontier instantiating the thread.
	 * @param distributor the distributor used.
	 */
	public StatsThread(final Frontier frontier, final Distributor distributor) {
		this.frontier = frontier;
		this.distributor = distributor;

		requestLogger = new ProgressLogger(LOGGER, Long.MAX_VALUE, TimeUnit.MILLISECONDS, "requests");
		requestLogger.displayFreeMemory = requestLogger.displayLocalSpeed = true;
		requestLogger.speedTimeUnit = TimeUnit.SECONDS;
		requestLogger.itemTimeUnit = TimeUnit.MILLISECONDS;

		resourceLogger = new ProgressLogger(LOGGER, Long.MAX_VALUE, TimeUnit.MILLISECONDS, "resources");
		resourceLogger.displayLocalSpeed = true;
		resourceLogger.speedTimeUnit = TimeUnit.SECONDS;
		resourceLogger.itemTimeUnit = TimeUnit.MILLISECONDS;

		transferredBytesLogger = new ProgressLogger(LOGGER, Long.MAX_VALUE, TimeUnit.MILLISECONDS, "bytes");
		transferredBytesLogger.displayLocalSpeed = true;
		transferredBytesLogger.speedTimeUnit = TimeUnit.SECONDS;
		transferredBytesLogger.itemTimeUnit = TimeUnit.NANOSECONDS;

		receivedURLsLogger = new ProgressLogger(LOGGER, Long.MAX_VALUE, TimeUnit.MILLISECONDS, "receivedURLs");
		receivedURLsLogger.displayLocalSpeed = true;
		receivedURLsLogger.speedTimeUnit = TimeUnit.SECONDS;
	}

	/** Starst all progress loggers.
	 *
	 * @param previousCrawlDuration the duration of the previous crawl, or zero for a new crawl.
	 */
	public void start(long previousCrawlDuration) {
		requestLogger.start(previousCrawlDuration);
		resourceLogger.start(previousCrawlDuration);
		transferredBytesLogger.start(previousCrawlDuration);
		receivedURLsLogger.start(previousCrawlDuration);
	}

	/** Returns an integer array as a string, but does not print trailing zeroes.
	 *
	 * @param a an array.
	 * @return {@link Arrays#toString()} of {@code a}, but without trailing zeroes.
	 */
	public static String toString(final int[] a) {
		int i;
		for(i = a.length; i-- != 0;) if (a[i] != 0) break;
		return Arrays.toString(Arrays.copyOfRange(a, 0, i + 1));
	}

	/** Returns an {@link AtomicLongArray} array as a string, but does not print trailing zeroes.
	 *
	 * @param a an atomic array.
	 * @return {@link Arrays#toString(long[])} of {@code a}, but without trailing zeroes.
	 */
	public static String toString(final AtomicLongArray a) {
		int i;
		for(i = a.length(); i-- != 0;) if (a.get(i) != 0) break;
		final long[] b = new long[i + 1];
		for(++i; i-- != 0;) b[i] = a.get(i);
		return Arrays.toString(b);
	}

	/** Emits the statistics. */
	public void emit() {
		requestLogger.setAndDisplay(frontier.fetchedResources.get() + frontier.fetchedRobots.get());
		final long duplicates = frontier.duplicates.get();
		final long archetypes = frontier.archetypes();
		resourceLogger.setAndDisplay(archetypes + duplicates);
		transferredBytesLogger.setAndDisplay(frontier.transferredBytes.get());
		receivedURLsLogger.setAndDisplay(frontier.numberOfReceivedURLs.get());

		LOGGER.info("Duplicates: " + Util.format(duplicates) + " (" + Util.format(100.0 * duplicates / (duplicates + archetypes)) + "%)");
		LOGGER.info("Archetypes 1XX/2XX/3XX/4XX/5XX/Other: "
				+ Util.format(frontier.archetypesStatus[1].get()) + "/"
				+ Util.format(frontier.archetypesStatus[2].get()) + "/"
				+ Util.format(frontier.archetypesStatus[3].get()) + "/"
				+ Util.format(frontier.archetypesStatus[4].get()) + "/"
				+ Util.format(frontier.archetypesStatus[5].get()) + "/"
				+ Util.format(frontier.archetypesStatus[0].get()));

		LOGGER.info("Outdegree stats: " + frontier.outdegree.toString());
		LOGGER.info("External outdegree stats: " + frontier.externalOutdegree.toString());
		LOGGER.info("Archetype content-length stats: " + frontier.contentLength.toString());
		LOGGER.info("Archetypes text/image/application/other: "
				+ Util.format(frontier.contentTypeText.get()) + "/"
				+ Util.format(frontier.contentTypeImage.get()) + "/"
				+ Util.format(frontier.contentTypeApplication.get()) + "/"
				+ Util.format(frontier.contentTypeOthers.get()));

		LOGGER.info("Ready URLs: " + Util.format(frontier.readyURLs.size64()));

		LOGGER.info("FetchingThread waits: " + frontier.fetchingThreadWaits.get() + "; total wait time: " + frontier.fetchingThreadWaitingTimeSum.get());
		frontier.resetFetchingThreadsWaitingStats();
	}

	private boolean checkState() {
		for(VisitState visitState: distributor.schemeAuthority2VisitState.visitStates())
			if (visitState != null)
				synchronized (visitState) {
					if (visitState.workbenchEntry == null && visitState.acquired) LOGGER.error("Acquired visit state with empty workbench entry: " + visitState);
					if (visitState.workbenchEntry == null && visitState.nextFetch != Long.MAX_VALUE && visitState.isEmpty()) LOGGER.error("Empty visit state with empty workbench entry: " + visitState);
					//if (! visitState.acquired && frontier.virtualizer.count(visitState) > 0 && visitState.isEmpty() && visitState.nextFetch != Long.MAX_VALUE && ! frontier.refill.contains(visitState)) LOGGER.error("Empty visit state with URLs on disk not scheduled for refill (not a problem if it doesn't appear again): " + visitState);
			}

		long c = 0;
		for(WorkbenchEntry workbenchEntry: frontier.workbench.workbenchEntries()) {
			if (workbenchEntry != null) {
				synchronized (workbenchEntry) {
					if (workbenchEntry.isEntirelyBroken()) c++;
				}
			}
		}
		final long broken = frontier.workbench.broken.get();
		if (Math.abs(c - broken) > Math.max(4 , .1 * (c + 1))) LOGGER.error("Broken count (counter): " + broken  + " Broken count (counted): " + c + " (not a problem if it doesn't appear often)");
		return true;
	}

	@Override
	public void run() {
		frontier.workbenchSizeInPathQueries = frontier.rc.workbenchMaxByteSize / Math.max(1, frontier.weightOfpathQueriesInQueues.get() / (1 + frontier.pathQueriesInQueues.get()));

		LOGGER.info("There are now " + frontier.pathQueriesInQueues.get() + " URLs in queues (" + Util.formatSize(frontier.weightOfpathQueriesInQueues.get()) + "B, " + Util.format(100.0 * frontier.weightOfpathQueriesInQueues.get() / frontier.rc.workbenchMaxByteSize) + "%)");

		double totalSpeed = 0;
		long nonEmptyResolvedVisitStates = 0;
		final int dist[] = new int[32];
		final int distUnresolved[] = new int[32];
		final int distBroken[] = new int[32];

		long resolvedVisitStates = 0, brokenVisitStatesOnWorkbench = 0, unresolved = 0, brokenPathQueryCount = 0;

		for(VisitState visitState: distributor.schemeAuthority2VisitState.visitStates()) { // We own the map.
			if (visitState == null) continue;
			final int size = visitState.size();
			if (visitState.workbenchEntry != null) {
				resolvedVisitStates++;
				if (visitState.lastExceptionClass != null) {
					brokenVisitStatesOnWorkbench++;
					distBroken[Fast.mostSignificantBit(size) + 1]++;
					brokenPathQueryCount += size;
				}
				else {
					dist[Fast.mostSignificantBit(size) + 1]++;
					if (size != 0) nonEmptyResolvedVisitStates++;
				}
			}
			else {
				distUnresolved[Fast.mostSignificantBit(size) + 1]++;
				unresolved += size;
			}
		}

		this.dist = dist;
		this.unresolved = unresolved;
		this.brokenPathQueryCount = brokenPathQueryCount;

		LOGGER.info("Queue dist: " + toString(dist) + " (unresolved: " + unresolved + ", broken: " + brokenPathQueryCount + ")");
		LOGGER.info("BrokenQueue dist: " + toString(distBroken));
		LOGGER.info("UnresolvedQueue dist: " + toString(distUnresolved));

		if (nonEmptyResolvedVisitStates != 0) frontier.averageSpeed = totalSpeed / nonEmptyResolvedVisitStates;

		assert checkState();

		final SummaryStats entrySummaryStats = new SummaryStats();

		for(Iterator<WorkbenchEntry> iterator = frontier.workbench.iterator(); iterator.hasNext();) { // Concurrency-safe iterator by documentation.
			final int numVisitStates = iterator.next().size(); // Synchronized method.
			if (numVisitStates != 0) entrySummaryStats.add(numVisitStates); // Might be zero by asynchronous modifications.
		}

		this.entrySummaryStats = entrySummaryStats;
		this.resolvedVisitStates = resolvedVisitStates;
		this.brokenVisitStatesOnWorkbench = brokenVisitStatesOnWorkbench;

		LOGGER.info("Entry stats: " + entrySummaryStats);
		LOGGER.info("Virtualizer stats: " + frontier.virtualizer);

		LOGGER.info("Visit states: " + distributor.schemeAuthority2VisitState.size()
				+ "; resolved: " + resolvedVisitStates
				+ "; on workbench (IP): " + frontier.workbench.approximatedSize()
				+ "; broken on workbench (IP): " + frontier.workbench.broken.get()
				+ "; on workbench (S+A): " + (long)entrySummaryStats.sum()
				+ "; to do: " + frontier.todo.size()
				+ "; active: " + (frontier.rc.fetchingThreads - frontier.results.size())
				+ "; ready to parse: " + frontier.results.size()
				+ "; unknown hosts: " + frontier.unknownHosts.size()
				+ "; broken: " + frontier.brokenVisitStates.get() + " (" + brokenVisitStatesOnWorkbench + " on workbench)"
				+ "; waiting: " + frontier.newVisitStates.size()
				+ "; on disk: " + frontier.virtualizer.onDisk());
		LOGGER.info("Speed dist: " + toString(frontier.speedDist));
		for(int i = frontier.speedDist.length(); i-- != 0;) frontier.speedDist.set(i, 0); // Cleanup
		LOGGER.info("Cache hits: " + frontier.urlCache.hits() + " misses: " + frontier.urlCache.misses());

		distributor.lastHighCostStat = System.currentTimeMillis();
	}

	/** Returns the number of visit states on disk.
	 *
	 * @return the number of visit states on disk.
	 */
	public long getVisitStatesOnDisk(){
		return frontier.virtualizer.onDisk();
	}

	/** Returns the overall number of visit states.
	 *
	 * @return the overall number of visit states.
	 */
	public int getVisitStates(){
		return  distributor.schemeAuthority2VisitState.size();
	}

	/** Terminates the statistics, {@linkplain ProgressLogger#done closing} all the progress loggers.  */
	public void done() {
		requestLogger.done();
		resourceLogger.done();
		transferredBytesLogger.done();
		receivedURLsLogger.done();
	}
}
