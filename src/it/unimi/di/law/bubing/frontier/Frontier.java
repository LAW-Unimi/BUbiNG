package it.unimi.di.law.bubing.frontier;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xbill.DNS.DClass;
import org.xbill.DNS.Lookup;

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

import it.unimi.di.law.bubing.Agent;
import it.unimi.di.law.bubing.RuntimeConfiguration;
import it.unimi.di.law.bubing.sieve.AbstractSieve;
import it.unimi.di.law.bubing.sieve.ByteArrayListByteSerializerDeserializer;
import it.unimi.di.law.bubing.sieve.ByteSerializerDeserializer;
import it.unimi.di.law.bubing.sieve.IdentitySieve;
import it.unimi.di.law.bubing.sieve.MercatorSieve;
import it.unimi.di.law.bubing.store.Store;
import it.unimi.di.law.bubing.util.BURL;
import it.unimi.di.law.bubing.util.BubingJob;
import it.unimi.di.law.bubing.util.ByteArrayDiskQueue;
import it.unimi.di.law.bubing.util.ConcurrentCountingMap;
import it.unimi.di.law.bubing.util.FastApproximateByteArrayCache;
import it.unimi.di.law.bubing.util.FetchData;
import it.unimi.di.law.bubing.util.LockFreeQueue;
import it.unimi.di.law.bubing.util.MurmurHash3;
import it.unimi.di.law.bubing.util.Util;
import it.unimi.di.law.warc.io.ParallelBufferedWarcWriter;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.jai4j.Job;
import it.unimi.dsi.jai4j.JobListener;
import it.unimi.dsi.jai4j.JobManager;
import it.unimi.dsi.jai4j.NoSuchJobManagerException;
import it.unimi.dsi.stat.SummaryStats;
import it.unimi.dsi.sux4j.mph.AbstractHashFunction;
import it.unimi.dsi.util.BloomFilter;
import it.unimi.dsi.util.Properties;
import net.htmlparser.jericho.Config;
import net.htmlparser.jericho.LoggerProvider;

//RELEASE-STATUS: DIST

/** The BUbiNG frontier: a class structure that encompasses most of the logic behind the way BUbiNG
 * fetches URLs. To understand how this class works, we invite the reader to consult the
 * documentation of {@link VisitState} first.
 *
 * <p>Every BUbiNG agent contains an instance of this class, which is responsible for starting and
 * orchestrating the mutual interaction of many different elements: <ul> <li>the
 * {@linkplain #workbench workbench} (described below), that contains the {@link VisitState}s to be
 * visited next; <li>the {@link Distributor}, that fills the workbench, possibly virtualizing part
 * of it through a {@link WorkbenchVirtualizer}; <li>the {@link TodoThread} and the
 * {@link DoneThread}, that move around {@link VisitState}s during the crawl; <li>a number of worker
 * threads doing useful work, like {@link DNSThread}, {@link FetchingThread} and
 * {@link ParsingThread}. </ul>
 *
 * <h2>How the frontier handles URLs</h2>
 *
 * <p>URLs in BUbiNG belong to one of four states:
 *
 * <ul>
 *
 * <li><em>unknown</em>: we have never seen the URL as yet;
 *
 * <li><em>waiting (to be filtered)</em>: we have seen the URL and we have enqueued it in the
 * {@linkplain AbstractSieve sieve}, but the URL is waiting to be {@linkplain AbstractSieve#flush()
 * flushed} (i.e., to be poured from the sieve to the {@link #readyURLs} queue, which happens only
 * the first time for every given URL);
 *
 * <li><em>ready</em>: the URL has come out of the sieve, so it will be visited at some point in the
 * future unless some limiting parameters (e.g.,
 * {@link RuntimeConfiguration#maxUrlsPerSchemeAuthority}) forbid it;
 *
 * <li><em>visited</em>: the URL became ready and was somehow handled (e.g., fetched and possibly
 * stored).
 *
 * </ul>
 *
 * <p>Note that waiting URLs that happen to be ready or visited will never come out of the sieve: it
 * is the logic of the sieve itself that inhibits the same object to be emitted twice.
 *
 * <p>All ready URLs are initially stored in a {@link ByteArrayDiskQueue} called {@link #readyURLs},
 * from which they are moved to the FIFO queue of their {@link VisitState} by the
 * {@link Distributor}. Inside a {@link VisitState}, we only store a byte-array represention of the
 * path+query of ready URLs. Some of them may be stored outside of the visit state, through
 * {@linkplain WorkbenchVirtualizer virtualization}. Keeping path+queries in byte-array form in all
 * components reduces enormously object creation, and provides a simple form of compression by
 * prefix omission.
 *
 * <h2>The lifecycle of a URL</h2>
 *
 * <p>URLs are {@linkplain Frontier#enqueue(ByteArrayList) enqueued to the frontier} either because
 * they are part of the visit seed, or because a {@link ParsingThread} has found them, or because
 * they have been {@linkplain JobManager#submit(Job) submitted using JAI4J} (this includes both
 * manual submission and URLs sent by other agents). The method {@link #enqueue(ByteArrayList)} will
 * first check whether we have stored already too many URLs for the URL scheme+authority, in which
 * case the URL is discarded; then, it will check whether the URL already appears in the URL cache
 * (also in this case, the URL is discarded); finally it will check whether there is another agent
 * responsible for the URL, and in this case the URL will be sent to the appropriate agent using the
 * JAI4J infrastructure. If all checks have passed, the URL will be put in the sieve.
 *
 * <p>The URL might never be dequeued from the sieve (because we have seen it already), but if it
 * is, it will become ready by going through the {@link #append(long, ByteArrayList)} method, which
 * will again check that we do not have too many URLs for the URL authority, and then will enqueue
 * the URL to the {@linkplain #readyURLs disk queue of ready URLs}.
 *
 * <p>Note that we expect that <em>the vast majority of URLs will be of the first kind</em>. This is
 * very important, as there is much less contention on visit state locks than on the frontier lock. */
public class Frontier implements JobListener<BubingJob>, AbstractSieve.NewFlowReceiver<ByteArrayList> {
	private static final Logger LOGGER = LoggerFactory.getLogger(Frontier.class);

	/** The size of the buffer used for {@link Frontier#readyURLs}. */
	public static final int READY_URLS_BUFFER_SIZE = 64 * 1024 * 1024;

	/** Names of the scalar fields saved by {@link #snap()}. */
	public static enum PropertyKeys {
		PATHQUERIESINQUEUES,
		WEIGHTOFPATHQUERIESINQUEUES,
		BROKENVISITSTATES,
		NUMBEROFRECEIVEDURLS,
		REQUIREDFRONTSIZE,
		FETCHINGTHREADWAITS,
		FETCHINGTHREADWAITINGTIMESUM,
		ARCHETYPESOTHERS,
		ARCHETYPES1XX,
		ARCHETYPES2XX,
		ARCHETYPES3XX,
		ARCHETYPES4XX,
		ARCHETYPES5XX,
		DUPLICATES,
		FETCHEDRESOURCES,
		FETCHEDROBOTS,
		TRANSFERREDBYTES,
		AVERAGESPEED,
		// ALERT: we should keep also:
		// outDegree, outHostDegree, contentLength, contentTypeText, contentTypeImage,
		// contentTypeApplication, contentTypeOthers;
		// EXTRAS
		CURRENTQUEUE,
		VIRTUALQUEUESIZES,
		VIRTUALQUEUESBIRTHTIME,
		READYURLSSIZE,
		RECEIVEDURLSSIZE,
		// DISTRIBUTOR EXTRAS
		DISTRIBUTORWARMUP,
		DISTRIBUTORVISITSTATESONDISK,
		// METADATA
		EPOCH,
		CRAWLDURATION,
		VISITSTATESETSIZE,
		WORKBENCHENTRYSETSIZE
	};

	/** The loopback address, cached. */
	public static final InetAddress[] LOOPBACK;

	static {
		try {
			LOOPBACK = new InetAddress[] { InetAddress.getByAddress(new byte[] { 127, 0, 0, 1 }) };
		} catch (final UnknownHostException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}

	/** The name of the file in {@link RuntimeConfiguration#storeDir} storing downloaded
	 * <code>robots.txt</code> files. */
	private static final String ROBOTS_STORE = "robots.warc.gz";

	/** The minimum number of milliseconds between two flushes. */
	public static final long MIN_FLUSH_INTERVAL = 10000;

	/** The increase of the front size used by {@link #updateRequestedFrontSize()}. */
	public static final long FRONT_INCREASE = 1000;

	/** A hash function using {@link MurmurHash3}. */
	public final static AbstractHashFunction<byte[]> BYTE_ARRAY_HASHING_STRATEGY = new AbstractHashFunction<byte[]>() {
		private static final long serialVersionUID = 1L;

		@Override
		public long getLong(final Object key) {
			return MurmurHash3.hash((byte[])key);
		}
	};

	/** A hash function using {@link MurmurHash3}. */
	public final static AbstractHashFunction<ByteArrayList> BYTE_ARRAY_LIST_HASHING_STRATEGY = new AbstractHashFunction<ByteArrayList>() {
		private static final long serialVersionUID = 1L;

		@Override
		public long getLong(final Object key) {
			return MurmurHash3.hash((ByteArrayList)key);
		}
	};


	/** The store. */
	protected final Store store;

	/** The agent that created this frontier. */
	protected final Agent agent;

	/** The runtime configuration. */
	public final RuntimeConfiguration rc;

	/** An instance of a {@link MercatorSieve}. */
	public AbstractSieve<ByteArrayList, Void> sieve;

	/** A queue to store URLs coming out of the {@link #sieve}. */
	public ByteArrayDiskQueue readyURLs;

	/** A queue to quickly buffer URLs communicated by {@link #receive(BubingJob)}. */
	public ArrayBlockingQueue<ByteArrayList> quickReceivedURLs;

	/** A queue to buffer in the long run URLs communicated by {@link #receive(BubingJob)}. */
	public ByteArrayDiskQueue receivedURLs;

	/** The parsing threads. */
	public final ObjectArrayList<ParsingThread> parsingThreads;

	/** The Warc file where to write (if so requested) the downloaded <code>robots.txt</code> files. */
	public final ParallelBufferedWarcWriter robotsWarcParallelOutputStream;

	/** The overall number of path+queries stored in {@link VisitState} queues. */
	public final AtomicLong pathQueriesInQueues;

	/** The overall {@linkplain BURL#memoryUsageOf(byte[]) memory} (in bytes) used by path+queries
	 * stored in {@link VisitState} queues. */
	public final AtomicLong weightOfpathQueriesInQueues;

	/** The number of broken {@linkplain VisitState visit states}. */
	// TODO: maybe we can eliminate this--it is used only in stats, where it can be recomputed
	public final AtomicLong brokenVisitStates;

	/** The overall number of URLs sent by other agents. */
	public final AtomicLong numberOfReceivedURLs;

	/** The time at which the next flush can happen. */
	protected volatile long nextFlush;

	/** The workbench. */
	public final Workbench workbench;

	/** The queue of unknown hosts. */
	public final DelayQueue<VisitState> unknownHosts;

	/** The queue of new {@linkplain VisitState visit states}; filled by the
	 * {@linkplain #distributor} and emptied by the {@linkplain #dnsThreads DNS threads}. */
	public final LinkedBlockingQueue<VisitState> newVisitStates;

	/** A Bloom filter storing page digests for duplicate detection. */
	public BloomFilter<Void> digests;

	/** The threads resolving DNS for new {@linkplain VisitState visit states}. */
	protected final ObjectArrayList<DNSThread> dnsThreads;

	/** The threads fetching data. */
	private final ObjectArrayList<FetchingThread> fetchingThreads;

	/** The thread constantly moving ready URLs into the {@linkplain #workbench}. */
	protected final Distributor distributor;

	/** The URL cache. This cache stores the most recent URLs that have been
	 * {@linkplain Frontier#enqueue(ByteArrayList) enqueued}. */
	public final FastApproximateByteArrayCache urlCache;

	/** The workbench virtualizer used by this frontier. */
	protected final WorkbenchVirtualizer virtualizer;

	/** A lock-free list of visit states ready to be visited; it is filled by the {@link TodoThread}
	 * and emptied by the {@linkplain FetchingThread fetching threads}. */
	public final LockFreeQueue<VisitState> todo;

	/** A lock-free list of visit states ready to be released; it is filled by
	 * {@linkplain FetchingThread fetching threads} and emptied by the {@link DoneThread}. */
	public final LockFreeQueue<VisitState> done;

	/** A queue of visit states ready to be reilled; it is filled by {@linkplain DoneThread fetching
	 * threads} and emptied by the {@link Distributor}. */
	protected final LockFreeQueue<VisitState> refill;

	/** The current estimation for the size of the front in IP addresses. It is adaptively increased
	 * when a {@link FetchingThread} has to wait to retrieve a {@link VisitState} from the
	 * {@link #todo} queue. It is never more than half the {@linkplain #workbenchSizeInPathQueries
	 * number of path+queries that the workbench can hold}. */
	public final AtomicLong requiredFrontSize;

	/** The number of waits performed by fetching threads; every time the statistics are printed this
	 * value is reset. */
	public final AtomicLong fetchingThreadWaits;
	/** The sum of the waiting time of the waiting fetching threads: every time a fetching thread
	 * waits this sum is updated; every time the statistics are printed this value is reset. */
	public final AtomicLong fetchingThreadWaitingTimeSum;

	/** A lock-free list of {@link FetchData} to be parsed; it is filled by
	 * {@linkplain FetchingThread fetching threads} and emptied by the {@linkplain ParsingThread
	 * parsing threads}. */
	public final LockFreeQueue<FetchData> results;

	/** The thread moving {@linkplain VisitState visit states} ready to be visited from the
	 * {@linkplain #workbench} to the {@link #todo} queue. */
	private TodoThread todoThread;

	/** The thread moving {@linkplain VisitState visit states} that have been visited from the
	 * {@link #done} queue to the {@linkplain #workbench}. */
	private DoneThread doneThread;

	/** In position <var>i</var>, with 0 &lt; <var>i</var> &lt;6, the number of pages stored (does
	 * not include duplicates) having status <var>i</var>xx. In position 0, the number of pages with
	 * other status. */
	public final AtomicLong[] archetypesStatus;

	/** Statistics about the number of out-links of each archetype */
	public final SummaryStats outdegree;

	/** Statistics about the number of out-links of each archetype, without considering the links to
	 * the same corresponding host */
	public final SummaryStats externalOutdegree;

	/** Statistic about the content length of each archetype */
	public final SummaryStats contentLength;

	/** Number of archetypes whose indicated content type starts with text (case insensitive) **/
	public final AtomicLong contentTypeText;

	/** Number of archetypes whose indicated content type starts with image (case insensitive) **/
	public final AtomicLong contentTypeImage;

	/** Number of archetypes whose indicated content type starts with application (case insensitive) **/
	public final AtomicLong contentTypeApplication;

	/** Number of archetypes whose indicated content type does not start with text, image, or
	 * application (case insensitive) **/
	public final AtomicLong contentTypeOthers;

	/** The number of duplicate pages. */
	public final AtomicLong duplicates;

	/** The number of fetched resources (updated by {@link ParsingThread} instances). */
	public final AtomicLong fetchedResources;

	/** The number of fetched <code>robots.txt</code> files (updated by {@link ParsingThread}
	 * instances). */
	public final AtomicLong fetchedRobots;

	/** The overall number of transferred bytes. */
	public final AtomicLong transferredBytes;

	/** A synchronized, highly concurrent map from scheme+authorities to number of stored URLs. */
	public ConcurrentCountingMap schemeAuthority2Count;

	/** The logarithmically binned statistics of download speed in bits/s. */
	public final AtomicLongArray speedDist;

	/** The average speeds of all visit states. */
	protected double averageSpeed;
	/** An estimation of the number of path+query objects that the workbench can store. */
	public volatile long workbenchSizeInPathQueries;
	/** The default configuration for a non-<code>robots.txt</code> request. */
	public final RequestConfig defaultRequestConfig;
	/** The default configuration for a <code>robots.txt</code> request. */
	public final RequestConfig robotsRequestConfig;

	/** Creates the frontier.
	 *
	 * @param rc the configuration to be used to set all parameters.
	 * @param store the place where fetched pages will be stored.
	 * @param agent the BUbiNG agent possessing this frontier. */
	public Frontier(final RuntimeConfiguration rc, final Store store, final Agent agent) throws IOException, IllegalArgumentException, ConfigurationException, ClassNotFoundException,
			InterruptedException {
		this.rc = rc;

		schemeAuthority2Count = new ConcurrentCountingMap();
		workbenchSizeInPathQueries = rc.workbenchMaxByteSize / 100;
		averageSpeed = 1. / rc.schemeAuthorityDelay;

		final File robotsFile = new File(rc.storeDir, ROBOTS_STORE);
		LOGGER.info("Opening file " + robotsFile + " to write robots.txt");
		robotsWarcParallelOutputStream = new ParallelBufferedWarcWriter(new FastBufferedOutputStream(new FileOutputStream(robotsFile, !rc.crawlIsNew)), true);

		urlCache = new FastApproximateByteArrayCache(rc.urlCacheMaxByteSize);

		this.store = store;

		if (rc.sieveSize == 0) sieve = new IdentitySieve<>(this, new ByteArrayListByteSerializerDeserializer(), ByteSerializerDeserializer.VOID,
				BYTE_ARRAY_LIST_HASHING_STRATEGY, null);
		else sieve = new MercatorSieve<>(rc.crawlIsNew, rc.sieveDir, rc.sieveSize, rc.sieveStoreIOBufferByteSize, rc.sieveAuxFileIOBufferByteSize, this,
				new ByteArrayListByteSerializerDeserializer(), ByteSerializerDeserializer.VOID, BYTE_ARRAY_LIST_HASHING_STRATEGY, null);

		this.agent = agent;
		this.workbench = new Workbench();
		this.unknownHosts = new DelayQueue<>();
		this.virtualizer = new WorkbenchVirtualizer(this);

		pathQueriesInQueues = new AtomicLong();
		weightOfpathQueriesInQueues = new AtomicLong();
		brokenVisitStates = new AtomicLong();
		fetchedResources = new AtomicLong();
		fetchedRobots = new AtomicLong();
		transferredBytes = new AtomicLong();
		speedDist = new AtomicLongArray(40);
		archetypesStatus = new AtomicLong[6];
		for (int i = 0; i < 6; i++) archetypesStatus[i] = new AtomicLong();
		outdegree = new SummaryStats();
		externalOutdegree = new SummaryStats();
		contentLength = new SummaryStats();
		contentTypeText = new AtomicLong();
		contentTypeImage = new AtomicLong();
		contentTypeApplication = new AtomicLong();
		contentTypeOthers = new AtomicLong();
		duplicates = new AtomicLong();
		numberOfReceivedURLs = new AtomicLong();
		requiredFrontSize = new AtomicLong(1000);
		fetchingThreadWaits = new AtomicLong();
		fetchingThreadWaitingTimeSum = new AtomicLong();

		defaultRequestConfig = RequestConfig.custom()
				.setSocketTimeout(rc.socketTimeout)
				.setConnectTimeout(rc.connectionTimeout)
				.setConnectionRequestTimeout(rc.connectionTimeout)
				.setCookieSpec(rc.cookiePolicy)
				.setRedirectsEnabled(false)
				.setProxy(rc.proxyHost.length() > 0 ? new HttpHost(rc.proxyHost, rc.proxyPort) : null)
				.build();


		robotsRequestConfig = RequestConfig.custom()
				.setSocketTimeout(rc.socketTimeout)
				.setConnectTimeout(rc.connectionTimeout)
				.setConnectionRequestTimeout(rc.connectionTimeout)
				.setCookieSpec(rc.cookiePolicy)
				.setRedirectsEnabled(true)
				.setMaxRedirects(5) // Google's policy
				.setProxy(rc.proxyHost.length() > 0 ? new HttpHost(rc.proxyHost, rc.proxyPort) : null)
				.build();


		dnsThreads = new ObjectArrayList<>();
		fetchingThreads = new ObjectArrayList<>();
		parsingThreads = new ObjectArrayList<>();
		newVisitStates = new LinkedBlockingQueue<>();
		todo = new LockFreeQueue<>();
		done = new LockFreeQueue<>();
		refill = new LockFreeQueue<>();
		results = new LockFreeQueue<>();
		distributor = new Distributor(this);

		// Configures Jericho to use SLF4J
		Config.LoggerProvider = LoggerProvider.SLF4J;

		quickReceivedURLs = new ArrayBlockingQueue<>(1024);

		if (rc.crawlIsNew) {
			digests = BloomFilter.create(Math.max(1, rc.maxUrls), rc.bloomFilterPrecision);
			readyURLs = ByteArrayDiskQueue.createNew(new File(rc.frontierDir, "ready"), READY_URLS_BUFFER_SIZE, true);
			receivedURLs = ByteArrayDiskQueue.createNew(new File(rc.frontierDir, "received"), 16 * 1024, true);
			distributor.statsThread.start(0);
		}
		else {
			restore();
		}

		// Never start child threads before every data structure is created or restored
		distributor.start();
		(todoThread = new TodoThread(this)).start();
		(doneThread = new DoneThread(this)).start();

		// These must be coordinated with bind's settings, if any.
		Lookup.getDefaultCache(DClass.IN).setMaxEntries(rc.dnsCacheMaxSize);
		Lookup.getDefaultCache(DClass.IN).setMaxCache((int)Math.min(rc.dnsPositiveTtl, Integer.MAX_VALUE));
		Lookup.getDefaultCache(DClass.IN).setMaxNCache((int)Math.min(rc.dnsNegativeTtl, Integer.MAX_VALUE));
		Lookup.getDefaultResolver().setTimeout(60);
	}

	/** Changes the number of DNS threads.
	 *
	 * <p>Note that when the number of thread is reduced, the stopped thread will actually terminate
	 * their execution as soon as they check the {@link DNSThread#stop} field.
	 *
	 * @param newDnsThreads the new number of threads. */
	public void dnsThreads(final int newDnsThreads) throws IllegalArgumentException {
		if (newDnsThreads <= 0) throw new IllegalArgumentException();

		synchronized (dnsThreads) {
			if (newDnsThreads < dnsThreads.size()) {
				for (int i = newDnsThreads; i < dnsThreads.size(); i++)
					dnsThreads.get(i).stop = true;
				dnsThreads.size(newDnsThreads);
				return;
			}

			for (int i = newDnsThreads - dnsThreads.size(); i-- != 0;) {
				final DNSThread thread = new DNSThread(this, dnsThreads.size());
				thread.start();
				dnsThreads.add(thread);
			}
		}

		LOGGER.info("Number of DNS Threads set to " + newDnsThreads);
	}


	/** Changes the number of fetching threads.
	 *
	 * <p>Note that when the number of thread is reduced, the stopped thread will actually terminate
	 * their execution as soon as they check the {@link ParsingThread#stop} field.
	 *
	 * @param numFetchingThreads the new number of threads. */
	public void fetchingThreads(final int numFetchingThreads) throws IllegalArgumentException, NoSuchAlgorithmException, IOException {
		if (numFetchingThreads <= 0) throw new IllegalArgumentException();

		synchronized (fetchingThreads) {
			if (numFetchingThreads < fetchingThreads.size()) {
				// ALERT: add emergency interrupt
				for (int i = numFetchingThreads; i < fetchingThreads.size(); i++)
					fetchingThreads.get(i).stop = true;
				fetchingThreads.size(numFetchingThreads);
				return;
			}

			for (int i = numFetchingThreads - fetchingThreads.size(); i-- != 0;) {
				final FetchingThread thread = new FetchingThread(this, fetchingThreads.size());
				thread.start();
				fetchingThreads.add(thread);
			}
		}

		LOGGER.info("Number of Fetching Threads set to " + numFetchingThreads);
	}


	/** Changes the number of parsing threads.
	 *
	 * <p>Note that when the number of thread is reduced, the stopped thread will actually terminate
	 * their execution as soon as they check the {@link ParsingThread#stop} field.
	 *
	 * @param newParsingThreads the new number of threads. */
	public void parsingThreads(final int newParsingThreads) throws IllegalArgumentException {
		if (newParsingThreads <= 0) throw new IllegalArgumentException();

		synchronized (parsingThreads) {
			if (newParsingThreads < parsingThreads.size()) {
				for (int i = newParsingThreads; i < parsingThreads.size(); i++)
					parsingThreads.get(i).stop = true;
				parsingThreads.size(newParsingThreads);
				return;
			}

			for (int i = newParsingThreads - parsingThreads.size(); i-- != 0;) {
				final ParsingThread thread = new ParsingThread(this, store, parsingThreads.size());
				thread.start();
				parsingThreads.add(thread);
			}
		}
		LOGGER.info("Number of Parsing Threads set to " + newParsingThreads);
	}

	/** Enqueues a URL to the BUbiNG crawl.
	 *
	 * <p>Before {@linkplain AbstractSieve#enqueue(Object, Object) enqueueing the URL to the sieve}
	 * we perform a number of checks:
	 *
	 * <ul>
	 *
	 * <li>if there are too many URLs for the URL scheme+authority, we discard the URL;
	 *
	 * <li>if the URL appears in the URL cache, we discard it; otherwise, we add it to the cache;
	 *
	 * <li>if another agent is responsible for the URL, we
	 * {@linkplain JobManager#submit(it.unimi.dsi.jai4j.Job) submit} it to the agent.
	 *
	 * </ul>
	 *
	 * <p>The difference between this method and {@link #enqueueLocal(ByteArrayList)} is that the
	 * latter does not check whether the argument is
	 * {@linkplain JobManager#local(it.unimi.dsi.jai4j.Job) local}.
	 *
	 * @param url a {@linkplain BURL BUbiNG URL} to be enqueued to the BUbiNG crawl.
	 * @throws InterruptedException from {@link AbstractSieve#enqueue(Object, Object)}. */
	public void enqueue(final ByteArrayList url) throws IOException, InterruptedException {
		final byte[] urlBuffer = url.elements();
		final int inStore = schemeAuthority2Count.get(urlBuffer, 0, BURL.startOfpathAndQuery(urlBuffer));
		if (inStore >= rc.maxUrlsPerSchemeAuthority) return;

		if (!urlCache.add(url)) return;

		final BubingJob job = new BubingJob(url);

		if (agent.local(job)) {
			if (sieve.enqueue(url, null)) nextFlush = System.currentTimeMillis() + MIN_FLUSH_INTERVAL;
		}
		else try {
			if (LOGGER.isTraceEnabled()) LOGGER.trace("Sending out scheme+authority {} with path+query {}", it.unimi.di.law.bubing.util.Util.toString(BURL.schemeAndAuthorityAsByteArray(urlBuffer)), it.unimi.di.law.bubing.util.Util.toString(BURL.pathAndQueryAsByteArray(url)));
			agent.submit(job);
		}
		catch (final IllegalStateException e) {
			// This just shouldn't happen.
			LOGGER.warn("Impossible to submit URL " + BURL.fromNormalizedByteArray(url.toByteArray()), e);
		}
		catch (final NoSuchJobManagerException e) {
			// This just shouldn't happen.
			LOGGER.warn("Impossible to submit URL " + BURL.fromNormalizedByteArray(url.toByteArray()), e);
		}

		return;
	}

	/** Returns whether the workbench is full.
	 *
	 * @return whether the workbench is full. */
	public boolean workbenchIsFull() {
		return weightOfpathQueriesInQueues.get() >= rc.workbenchMaxByteSize;
	}


	/** Enqueues a local URL represented by a byte array to the crawl of this agent.
	 *
	 * <p>Before {@linkplain AbstractSieve#enqueue(Object, Object) enqueueing the URL to the sieve}
	 * we perform a number of checks:
	 *
	 * <ul>
	 *
	 * <li>if there are too many URLs for the URL scheme+authority, we discard the URL;
	 *
	 * <li>if the URL appears in the URL cache, we discard it; otherwise, we add it to the cache.
	 *
	 * </ul>
	 *
	 * <p>The difference between this method and {@link #enqueue(ByteArrayList)} is that the latter
	 * checks whether the argument is {@linkplain JobManager#local(it.unimi.dsi.jai4j.Job) local}.
	 *
	 * @param url a BUbiNG URL to be enqueued to the BUbiNG crawl, in byte-array representation.
	 * @throws InterruptedException from {@link AbstractSieve#enqueue(Object, Object)}. */
	public void enqueueLocal(final ByteArrayList url) throws IOException, InterruptedException {
		final byte[] urlBuffer = url.elements();
		final int inStore = schemeAuthority2Count.get(urlBuffer, 0, BURL.startOfpathAndQuery(urlBuffer));
		if (inStore >= rc.maxUrlsPerSchemeAuthority) return;

		if (!urlCache.add(url)) return;

		if (sieve.enqueue(url, null)) nextFlush = System.currentTimeMillis() + MIN_FLUSH_INTERVAL;
	}

	@Override
	public void receive(final BubingJob job) {
		if (LOGGER.isDebugEnabled()) LOGGER.debug("Receiving job {}", job.url);
		try {
			// Note that this is blocking, but blocking should be very rare and short.
			quickReceivedURLs.put(job.url);
		}
		catch (final Exception e) {
			LOGGER.error("Error while enqueueing " + job.url, e);
		}
	}

	/** Closes the frontier: threads are stopped (if necessary, aborted), sieve and store and robots
	 * stream are closed.
	 *
	 * @throws IOException
	 * @throws InterruptedException */
	public void close() throws IOException, InterruptedException {
		/* First we wait for all high-level threads to complete. Note that only the workbench thread
		 * needs to be interrupted--all other threads check regularly for rc.stopping. Note that
		 * visit states in the todo and done list will be moved by snap() back into the workbench. */
		todoThread.interrupt();

		distributor.join();
		LOGGER.info("Joined distributor");
		todoThread.join();
		LOGGER.info("Joined todo thread");

		/* First we stop DNS threads; note that we have to set explicitly stop. */
		for (final DNSThread t : dnsThreads) t.stop = true;
		for (final DNSThread t : dnsThreads) t.join();
		LOGGER.info("Joined DNS threads");

		/* We wait for all fetching activity to come to a stop. */
		for (final FetchingThread t : fetchingThreads) t.stop = true;

		/* This extremely poor form of timeout waiting for fetching threads is motivated by threads
		 * hanging in ininterruptible, native socket I/O, and by the difficult to perform sensible
		 * joins with timeouts with thousands of threads. */
		long time = System.currentTimeMillis();
		boolean someAlive;

		do {
			Thread.sleep(1000);
			someAlive = false;
			for (final FetchingThread t : fetchingThreads) someAlive |= t.isAlive();
		} while (someAlive && System.currentTimeMillis() - time < rc.socketTimeout * 2);

		if (someAlive) for (final FetchingThread t : fetchingThreads) t.abort(); // Abort any still open requests.

		time = System.currentTimeMillis();
		do {
			Thread.sleep(1000);
			someAlive = false;
			for (final FetchingThread t : fetchingThreads) someAlive |= t.isAlive();
		} while (someAlive && System.currentTimeMillis() - time < rc.socketTimeout * 2);

		if (someAlive) {
			LOGGER.error("Some fetching threads are still alive");
			for (final FetchingThread t : fetchingThreads) t.interrupt();
		}

		// This catches fetching threads stuck because all parsing threads crashed
		for (final FetchingThread t : fetchingThreads) t.join();

		LOGGER.info("Joined fetching threads");

		// We wait to be sure that the done thread wakes up and released all remaining visit states.
		Thread.sleep(2000);
		doneThread.stop = true;
		doneThread.join();
		LOGGER.info("Joined done thread");

		// Wait for all results to be parsed, unless there are no more parsing threads alive
		while (results.size() != 0) {
			someAlive = false;
			for (final ParsingThread t : parsingThreads) someAlive |= t.isAlive();
			if (! someAlive) {
				LOGGER.error("No parsing thread alive: some results might not have been parsed");
				break;
			}
			Thread.sleep(1000);
		}
		if (results.size() == 0) LOGGER.info("All results have been parsed");

		/* Then we stop parsing threads; note that we have to set explicitly stop. */
		for (final ParsingThread t : parsingThreads) t.stop = true;
		for (final ParsingThread t : parsingThreads) t.join();

		robotsWarcParallelOutputStream.close();
		store.close();
		LOGGER.info("Joined parsing threads and closed stores");

		for (final FetchingThread t : fetchingThreads) t.close();
		LOGGER.info("Closed fetching threads");

		// Move the todo list back into the workbench
		for (VisitState visitState; (visitState = todo.poll()) != null;) workbench.release(visitState);
		// Move the done list back into the workbench (here we catch visit states released by the interrupts on the fetching threads, if any)
		for (VisitState visitState; (visitState = done.poll()) != null;) {
			// We do not schedule for refill purged visit states
			if (visitState.nextFetch != Long.MAX_VALUE && virtualizer.count(visitState) > 0 && visitState.isEmpty()) refill.add(visitState);
			workbench.release(visitState);
		}

		// Fix all visit states in the refill queue
		for (VisitState visitState; (visitState = refill.poll()) != null;) {
			// Note that this might make temporarily the workbench too big by a little bit.
			final int dequeuedURLs = virtualizer.dequeuePathQueries(visitState, visitState.pathQueryLimit());
			if (dequeuedURLs == 0) LOGGER.info("No URLs on disk during last refill: " + visitState);
			if (visitState.acquired) LOGGER.warn("Visit state in the poll queue is acquired: " + visitState);
		}

		// Finally, we close disk-based resources (sieve, queues, etc.).
		sieve.close();

		// We invoke done() here so the final stats are the last thing printed.
		distributor.statsThread.done();
	}

	// NewFlowReceiver implementation.

	/** A locked copy of the counting map used for quick analysis of candidate ready URLS. */
	private ConcurrentCountingMap.LockedMap lockedMap;

	@Override
	public void prepareToAppend() throws IOException {
		lockedMap = schemeAuthority2Count.lock();
	}

	@Override
	public void append(final long hash, final ByteArrayList list) throws IOException {
		final byte[] urlBuffer = list.elements();
		final int length = list.size();
		if (lockedMap.get(urlBuffer, 0, BURL.startOfpathAndQuery(urlBuffer)) < rc.maxUrlsPerSchemeAuthority) readyURLs.enqueue(urlBuffer, 0, length);
	}

	@Override
	public synchronized void finishedAppending() throws IOException {
		lockedMap.unlock();
	}

	@Override
	public void noMoreAppend() throws IOException {}

	/** Update, if necessary, the {@link #requiredFrontSize}. The current front size is the number of
	 * visit states present in the workbench and in the {@link #todo} queue. If this quantity is
	 * larged than the {@linkplain #requiredFrontSize currently-required front size}, the latter is
	 * increase by {@link #FRONT_INCREASE}, although it will never be set to a value larger than
	 * half of the workbench (two queries per visit state). */
	public void updateRequestedFrontSize() {
		final long currentRequiredFrontSize = requiredFrontSize.get();
		// If compareAndSet() returns false the value has already been updated.
		if (workbench.approximatedSize() + todo.size() - workbench.broken.get() >= currentRequiredFrontSize
				&& requiredFrontSize.compareAndSet(currentRequiredFrontSize, Math.min(currentRequiredFrontSize + FRONT_INCREASE, workbenchSizeInPathQueries / 2))) LOGGER
				.info("Required front size: " + requiredFrontSize.get());
	}

	/** Updates the statistics relative to the wait time of {@link FetchingThread}s.
	 *
	 * @param waitTime the length of a new pause by one of the fetching threads. */
	public void updateFetchingThreadsWaitingStats(final long waitTime) {
		fetchingThreadWaits.incrementAndGet();
		fetchingThreadWaitingTimeSum.addAndGet(waitTime);
	}

	/** Resets the statistics relative to the wait time of {@link FetchingThread}s. */
	public void resetFetchingThreadsWaitingStats() {
		fetchingThreadWaits.set(0);
		fetchingThreadWaitingTimeSum.set(0);
	}

	/** Snaps fields to files in the given directory. Fields that are of scalar are written into a
	 * single file named <code>frontier.data</code>. Other fields are written each in a file of its
	 * own, named with the name of the field. */
	public void snap() throws ConfigurationException, IllegalArgumentException, IOException {

		// Purge ROBOTS_PATH path+queries and set a fake last robots fetch so that ROBOTS_PATH will
		// be put back immediately after restart.
		for (final VisitState visitState : distributor.schemeAuthority2VisitState.visitStates())
			if (visitState != null) visitState.removeRobots();

		LOGGER.info("Final statistics");
		distributor.statsThread.emit();
		distributor.statsThread.run();

		final File snapDir = new File(rc.frontierDir, "snap");
		LOGGER.info("Started snapping to " + snapDir);
		if (snapDir.exists()) LOGGER.warn("Already existing snap directory " + snapDir + ": data will be overwritten (this shouldn't happen)");
		else if (!snapDir.mkdir()) {
			LOGGER.error("Could not create snap directory " + snapDir + ": will not produce snap");
			return;
		}

		LOGGER.info("Snapping scalar data");
		final Properties scalarData = new Properties();
		final long epoch = System.currentTimeMillis();
		scalarData.addProperty(PropertyKeys.EPOCH, epoch);
		// TODO: make this locale-independent
		scalarData.setHeader("Snap started at " + new Date());

		// Scalar properties
		scalarData.addProperty(PropertyKeys.PATHQUERIESINQUEUES, pathQueriesInQueues.get());
		scalarData.addProperty(PropertyKeys.WEIGHTOFPATHQUERIESINQUEUES, weightOfpathQueriesInQueues.get());
		scalarData.addProperty(PropertyKeys.BROKENVISITSTATES, brokenVisitStates.get());
		scalarData.addProperty(PropertyKeys.NUMBEROFRECEIVEDURLS, numberOfReceivedURLs.get());
		scalarData.addProperty(PropertyKeys.REQUIREDFRONTSIZE, requiredFrontSize.get());
		scalarData.addProperty(PropertyKeys.FETCHINGTHREADWAITS, fetchingThreadWaits.get());
		scalarData.addProperty(PropertyKeys.FETCHINGTHREADWAITINGTIMESUM, fetchingThreadWaitingTimeSum.get());
		scalarData.addProperty(PropertyKeys.ARCHETYPESOTHERS, archetypesStatus[0].get());
		scalarData.addProperty(PropertyKeys.ARCHETYPES1XX, archetypesStatus[1].get());
		scalarData.addProperty(PropertyKeys.ARCHETYPES2XX, archetypesStatus[2].get());
		scalarData.addProperty(PropertyKeys.ARCHETYPES3XX, archetypesStatus[3].get());
		scalarData.addProperty(PropertyKeys.ARCHETYPES4XX, archetypesStatus[4].get());
		scalarData.addProperty(PropertyKeys.ARCHETYPES5XX, archetypesStatus[5].get());
		scalarData.addProperty(PropertyKeys.DUPLICATES, duplicates.get());
		scalarData.addProperty(PropertyKeys.FETCHEDRESOURCES, fetchedResources.get());
		scalarData.addProperty(PropertyKeys.FETCHEDROBOTS, fetchedRobots.get());
		scalarData.addProperty(PropertyKeys.TRANSFERREDBYTES, transferredBytes.get());
		scalarData.addProperty(PropertyKeys.AVERAGESPEED, averageSpeed);
		// scalarData.addProperty(PropertyKeys.DISTRIBUTORWARMUP, distributor.warmup);
		scalarData.addProperty(PropertyKeys.CRAWLDURATION, distributor.statsThread.requestLogger.millis());

		scalarData.addProperty(PropertyKeys.VISITSTATESETSIZE, distributor.schemeAuthority2VisitState.size());
		scalarData.addProperty(PropertyKeys.WORKBENCHENTRYSETSIZE, workbench.numberOfWorkbenchEntries());


		LOGGER.info("Storing virtualizer states");
		virtualizer.close();

		// readyURLs and receivedURLs
		LOGGER.info("Freezing byte disk queues");
		scalarData.addProperty(PropertyKeys.READYURLSSIZE, readyURLs.size64());
		readyURLs.freeze();
		scalarData.addProperty(PropertyKeys.RECEIVEDURLSSIZE, receivedURLs.size64());
		receivedURLs.freeze();

		scalarData.save(new File(snapDir, "frontier.data"));

		// TODO makes this optional
		LOGGER.info("Storing digests");
		BinIO.storeObject(digests, new File(snapDir, "digests"));

		LOGGER.info("Storing counts");
		BinIO.storeObject(schemeAuthority2Count, new File(snapDir, "schemeAuthority2Count"));

		LOGGER.info("Storing visit states");
		final ObjectOutputStream workbenchStream = new ObjectOutputStream(new FastBufferedOutputStream(new FileOutputStream(new File(snapDir, "workbench"))));

		long c = 0;
		for (final VisitState visitState : distributor.schemeAuthority2VisitState.visitStates())
			if (visitState != null) {
				if (visitState.acquired) LOGGER.error("Acquired visit state: " + visitState);
				c++;
			}

		workbenchStream.writeLong(c);

		for (final VisitState visitState : distributor.schemeAuthority2VisitState.visitStates())
			if (visitState != null) {
				workbenchStream.writeObject(visitState);
				workbenchStream.writeBoolean(visitState.workbenchEntry != null);
				if (visitState.workbenchEntry != null) Util.writeByteArray(visitState.workbenchEntry.ipAddress, workbenchStream);
			}

		workbenchStream.close();
	}


	/** Restores data from the given directory.
	 *
	 * @throws InterruptedException
	 * @see #snap() */
	@SuppressWarnings("unchecked")
	public void restore() throws ConfigurationException, IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		final File snapDir = new File(rc.frontierDir, "snap");
		if (!snapDir.exists() || !snapDir.isDirectory()) {
			LOGGER.error("Trying to restore state from snap directory " + snapDir + ", but it does not exist or is not a directory");
			return;
		}

		LOGGER.info("Restoring data from " + snapDir);

		LOGGER.info("Restoring scalar data");
		final Properties scalarData = new Properties(new File(snapDir, "frontier.data"));
		final long epoch = scalarData.getLong(PropertyKeys.EPOCH);

		// Scalar properties
		transferredBytes.set(scalarData.getLong(PropertyKeys.TRANSFERREDBYTES));
		pathQueriesInQueues.set(scalarData.getLong(PropertyKeys.PATHQUERIESINQUEUES));
		weightOfpathQueriesInQueues.set(scalarData.getLong(PropertyKeys.WEIGHTOFPATHQUERIESINQUEUES));
		brokenVisitStates.set(scalarData.getLong(PropertyKeys.BROKENVISITSTATES));
		numberOfReceivedURLs.set(scalarData.getLong(PropertyKeys.NUMBEROFRECEIVEDURLS));
		requiredFrontSize.set(scalarData.getLong(PropertyKeys.REQUIREDFRONTSIZE));
		fetchingThreadWaits.set(scalarData.getLong(PropertyKeys.FETCHINGTHREADWAITS));
		fetchingThreadWaitingTimeSum.set(scalarData.getLong(PropertyKeys.FETCHINGTHREADWAITINGTIMESUM));
		archetypesStatus[0].set(scalarData.getLong(PropertyKeys.ARCHETYPESOTHERS));
		archetypesStatus[1].set(scalarData.getLong(PropertyKeys.ARCHETYPES1XX));
		archetypesStatus[2].set(scalarData.getLong(PropertyKeys.ARCHETYPES2XX));
		archetypesStatus[3].set(scalarData.getLong(PropertyKeys.ARCHETYPES3XX));
		archetypesStatus[4].set(scalarData.getLong(PropertyKeys.ARCHETYPES4XX));
		archetypesStatus[5].set(scalarData.getLong(PropertyKeys.ARCHETYPES5XX));
		duplicates.set(scalarData.getLong(PropertyKeys.DUPLICATES));
		fetchedResources.set(scalarData.getLong(PropertyKeys.FETCHEDRESOURCES));
		fetchedRobots.set(scalarData.getLong(PropertyKeys.FETCHEDROBOTS));
		transferredBytes.set(scalarData.getLong(PropertyKeys.TRANSFERREDBYTES));
		averageSpeed = scalarData.getDouble(PropertyKeys.AVERAGESPEED);
		// distributor.warmup = scalarData.getBoolean(PropertyKeys.DISTRIBUTORWARMUP);

		distributor.schemeAuthority2VisitState.ensureCapacity(scalarData.getInt(PropertyKeys.VISITSTATESETSIZE));
		workbench.address2WorkbenchEntry.ensureCapacity(scalarData.getInt(PropertyKeys.WORKBENCHENTRYSETSIZE));

		// TODO makes this optional
		LOGGER.info("Restoring digests");
		digests = (BloomFilter<Void>)BinIO.loadObject(new File(snapDir, "digests"));

		/* LOGGER.info("Restoring virtualizer states and defreezing virtual queues");
		 * virtualizer.currentQueue = scalarData.getInt(PropertyKeys.CURRENTQUEUE); String[]
		 * virtualQueueSizes = scalarData.getStringArray(PropertyKeys.VIRTUALQUEUESIZES); final
		 * int numVirtualQueues = virtualQueueSizes.length; virtualizer.virtualQueue = new
		 * ByteArrayDiskQueue[numVirtualQueues]; virtualizer.virtualQueuesBirthTime =
		 * scalarData.getLong(PropertyKeys.VIRTUALQUEUESBIRTHTIME); for(int i = 0; i <
		 * numVirtualQueues; i++) { long virtualQueueSize = Long.parseLong(virtualQueueSizes[i]
		 *); virtualizer.virtualQueue[i] = virtualQueueSize == -1? null :
		 * WorkbenchVirtualizer.createOrOpenQueue(this, virtualizer.virtualQueuesBirthTime, i,
		 * numVirtualQueues, false, virtualQueueSize); } */

		LOGGER.info("Restoring counts");
		schemeAuthority2Count = (ConcurrentCountingMap)BinIO.loadObject(new File(snapDir, "schemeAuthority2Count"));

		LOGGER.info("Restoring workbench");
		final ObjectInputStream workbenchStream = new ObjectInputStream(new FastBufferedInputStream(new FileInputStream(new File(snapDir, "workbench"))));

		final long workbenchSize = workbenchStream.readLong();
		long w = workbenchSize;
		try {
			while(w-- != 0) {
				final VisitState visitState = (VisitState)workbenchStream.readObject();
				visitState.frontier = this;
				distributor.schemeAuthority2VisitState.add(visitState);
				final boolean nonNullWorkbenchEntry = workbenchStream.readBoolean();
				if (visitState.lastRobotsFetch == Long.MAX_VALUE) visitState.forciblyEnqueueRobotsFirst();
				if (nonNullWorkbenchEntry) visitState.setWorkbenchEntry(workbench.getWorkbenchEntry(Util.readByteArray(workbenchStream)));
				else newVisitStates.add(visitState);

				if (visitState.isEmpty() && virtualizer.count(visitState) > 0) {
					LOGGER.error("Empty visit state, URLs on disk: " + visitState);
					refill.add(visitState);
				}
			}
		}
		catch(final EOFException e) {
			LOGGER.error("Workbench stream too short: " + w + " visit states missing out of " + workbenchSize);
		}
		workbenchStream.close();

		virtualizer.readMetadata();

		// readyURLs and receivedURLs
		LOGGER.info("Defreezing byte disk queues");
		final long readyURLsSize = scalarData.getLong(PropertyKeys.READYURLSSIZE);
		readyURLs = ByteArrayDiskQueue.createFromFile(readyURLsSize, new File(rc.frontierDir, "ready"), READY_URLS_BUFFER_SIZE, true);
		final long receivedURLsSize = scalarData.getLong(PropertyKeys.RECEIVEDURLSSIZE);
		receivedURLs = ByteArrayDiskQueue.createFromFile(receivedURLsSize, new File(rc.frontierDir, "received"), 16 * 1024, true);

		// Move away snap directory, as its contents will become unsynchronized with queue data.
		final File renameDir = new File(snapDir + "-" + epoch);
		LOGGER.info("Renaming snap directory " + snapDir + " to " + renameDir);
		if (!snapDir.renameTo(renameDir)) LOGGER.error("Could not rename snap directory");

		// Starting stats
		distributor.statsThread.start(scalarData.getLong(PropertyKeys.CRAWLDURATION));
		LOGGER.info("Starting statistics");
		distributor.statsThread.emit();
		distributor.statsThread.run();
	}

	/** Returns the {@link StatsThread}.
	 *
	 * @return the stats thread. */
	public StatsThread getStatsThread() {
		return distributor.statsThread;
	}

	/** The number of pages stored (does not include duplicates).
	 *
	 * @return the number of pages stored. */
	public long archetypes() {
		long rst = 0;
		for (int i = 0; i < archetypesStatus.length; i++)
			rst += archetypesStatus[i].get();
		return rst;
	}
}
