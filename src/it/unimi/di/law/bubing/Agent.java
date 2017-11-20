package it.unimi.di.law.bubing;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.jgroups.JChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.softee.management.annotation.Description;
import org.softee.management.annotation.MBean;
import org.softee.management.annotation.ManagedAttribute;
import org.softee.management.annotation.ManagedOperation;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

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


import it.unimi.di.law.bubing.frontier.Frontier;
import it.unimi.di.law.bubing.frontier.MessageThread;
import it.unimi.di.law.bubing.frontier.QuickMessageThread;
import it.unimi.di.law.bubing.store.Store;
import it.unimi.di.law.bubing.util.BURL;
import it.unimi.di.law.bubing.util.BubingJob;
import it.unimi.di.law.bubing.util.Link;
import it.unimi.di.law.warc.filters.URIResponse;
import it.unimi.di.law.warc.filters.parser.FilterParser;
import it.unimi.di.law.warc.filters.parser.ParseException;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.jai4j.ConsistentHashAssignmentStrategy;
import it.unimi.dsi.jai4j.NoSuchJobManagerException;
import it.unimi.dsi.jai4j.RemoteJobManager;
import it.unimi.dsi.jai4j.dropping.DiscardMessagesStrategy;
import it.unimi.dsi.jai4j.dropping.TimedDroppingThreadFactory;
import it.unimi.dsi.jai4j.jgroups.JGroupsJobManager;

//RELEASE-STATUS: DIST

/** A BUbiNG agent. This class contains the main method used to start a BUbiNG agent, and exposes on JMX a number of methods
 * that expose internal settings. In many cases settings can be changed while BUbiNG is running. */

@MBean @Description("A BUbiNG agent")
public class Agent extends JGroupsJobManager<BubingJob> {
	private final static Logger LOGGER = LoggerFactory.getLogger(Agent.class);
	/** The name of the standard Java system property that sets the JMX service port (it must be set for the agent to start). */
	public static final String JMX_REMOTE_PORT_SYSTEM_PROPERTY = "com.sun.management.jmxremote.port";
	/** The name of the system property that, if set, makes it possible to choose a JGroups configuration file. */
	public static final String JGROUPS_CONFIGURATION_PROPERTY_NAME = "it.unimi.di.law.bubing.jgroups.configurationFile";

	/** The only instance of global data in this agent. */
	private final RuntimeConfiguration rc;
	/** The frontier of this agent. */
	private final Frontier frontier;
	/** The store of this agent. */
	private final Store store;
	/** @see MessageThread */
	protected final MessageThread messageThread;
	/** @see QuickMessageThread */
	protected final QuickMessageThread quickMessageThread;

	public Agent(final String hostname, final int jmxPort, final RuntimeConfiguration rc) throws Exception {
		// TODO: configure strategies

		super(rc.name, rc.weight, new InetSocketAddress(hostname,  jmxPort),
				new JChannel(System.getProperty(JGROUPS_CONFIGURATION_PROPERTY_NAME) != null ? (InputStream)new FileInputStream(System.getProperty(JGROUPS_CONFIGURATION_PROPERTY_NAME)) : JGroupsJobManager.class.getResourceAsStream('/' + JGroupsJobManager.class.getPackage().getName().replace('.',  '/') + "/jgroups.xml")),
				 rc.group, new ConsistentHashAssignmentStrategy<BubingJob>(), new LinkedBlockingQueue<BubingJob>(),
				 new TimedDroppingThreadFactory<BubingJob>(1800000), new DiscardMessagesStrategy<BubingJob>());

		LOGGER.info("Creating Agent instance with properties {}", rc);

		// TODO: check crawlIsNew for all components.
		this.rc = rc;

		store = rc.storeClass.getConstructor(RuntimeConfiguration.class).newInstance(rc);

		register();

		frontier = new Frontier(rc, store, this);
		setListener(frontier);

		(messageThread = new MessageThread(frontier)).start();
		(quickMessageThread = new QuickMessageThread(frontier)).start();

		connect();

		// It is important that threads are allocated here, that is, after the agent has been connected.
		frontier.dnsThreads(rc.dnsThreads);
		frontier.parsingThreads(rc.parsingThreads);
		frontier.fetchingThreads(rc.fetchingThreads);

		frontier.rc.ensureNotPaused();
		final ByteArrayList list = new ByteArrayList();
		while(rc.seed.hasNext()) {
			final URI nextSeed = rc.seed.next();
			if (nextSeed != null) frontier.enqueue(BURL.toByteArrayList(nextSeed, list));
		}

		// We wait for the notification of a stop event, usually caused by a call to stop().
		synchronized(this) {
			if (! rc.stopping) wait();
		}

		// Stuff to be done at stopping time

		// The message thread uses the sieve, which will be closed by the frontier.
		messageThread.stop = true;
		messageThread.join();
		LOGGER.info("Joined message thread");

		frontier.close();

		LOGGER.info("Going to close job manager " + this);
		close();
		LOGGER.info("Job manager closed");

		// We stop here the quick message thread. Messages in the receivedURLs queue will be snapped.
		quickMessageThread.stop = true;
		quickMessageThread.join();
		LOGGER.info("Joined quick message thread");

		frontier.snap();

		LOGGER.info("Agent " + this + " exits");
	}

	/* Methods required to extend JGroupsJobManager. */

	@Override
	public BubingJob fromString(final String s) {
		final URI url = BURL.parse(s);
		if (url != null && url.isAbsolute()) return new BubingJob(ByteArrayList.wrap(BURL.toByteArray(url)));
		throw new IllegalArgumentException();
	}

	@Override
	public byte[] toByteArray(final BubingJob job) throws IllegalArgumentException {
		return job.url.toByteArray();
	}

	@Override
	public BubingJob fromByteArray(byte[] array, int offset) throws IllegalArgumentException {
		return new BubingJob(ByteArrayList.wrap(Arrays.copyOfRange(array, offset, array.length)));
	}

	/** Returns the number of agents currently known to the JAI4J {@link RemoteJobManager}.
	 *
	 * <p>Note that this number will be larger than that returned by {@link #getAliveCount()}
	 * if there are {@linkplain #getSuspectedCount() suspected agents}.
	 *
	 * @return the number of agents currently known to the JAI4J {@link RemoteJobManager}.
	 */
	public int getKnownCount() {
		return identifier2RemoteJobManager.size();
	}

	/* Main Managed Operations */

	@ManagedOperation @Description("Stop this agent")
	public synchronized void stop() {
		LOGGER.info("Going to stop the agent...");
		rc.stopping = true;
		notify();
	}


	@ManagedOperation @Description("Pause this agent")
	public void pause() {
		LOGGER.info("Going to pause the agent...");
		rc.paused = true;
	}

	@ManagedOperation @Description("Resume a paused agent")
	public void resume() {
		if (rc.paused) {
			LOGGER.info("Resuming the agent...");
			synchronized(rc) {
				rc.paused = false;
				rc.notifyAll();
			}
		}
		else LOGGER.warn("Agent not paused: not resuming");
	}

	@ManagedOperation @Description("Flush the sieve")
	public void flush() throws IOException, InterruptedException {
		frontier.sieve.flush();
	}

	@ManagedOperation @Description("Add a new IPv4 to the black list; it can be a single IP address or a file (prefixed by file:)")
	public void addBlackListedIPv4(@org.softee.management.annotation.Parameter("address") @Description("An IPv4 address to be blacklisted") String address) throws ConfigurationException, FileNotFoundException {
		final Lock lock = rc.blackListedIPv4Lock.writeLock();
		lock.lock();
		try {
			rc.addBlackListedIPv4(address);
		} finally {
			lock.unlock();
		}
	}

	@ManagedOperation @Description("Add a new host to the black list; it can be a single host or a file (prefixed by file:)")
	public void addBlackListedHost(@org.softee.management.annotation.Parameter("host") @Description("A host to be blacklisted") String host) throws ConfigurationException, FileNotFoundException {
		final Lock lock = rc.blackListedHostHashesLock.writeLock();
		lock.lock();
		try {
			rc.addBlackListedHost(host);
		} finally {
			lock.unlock();
		}
	}


	@ManagedOperation @Description("Get manager for this URL")
	public String getManager(@org.softee.management.annotation.Parameter("url") @Description("A URL") final String url) throws NoSuchJobManagerException {
		return assignmentStrategy.manager(new BubingJob(ByteArrayList.wrap(BURL.toByteArray(BURL.parse(url))))).toString();
	}

	/* Properties, the same as RuntimeConfiguration: final fields in RuntimeConfiguration are not reported since they can be seen in the file .properties;
	 * while volatile fields in RuntimeConfiguration can be get and set (except for paused and stopping)
	 * */

	@ManagedAttribute
	public void setDnsThreads(final int dnsThreads) throws IllegalArgumentException {
		frontier.dnsThreads(rc.dnsThreads = dnsThreads);
	}

	@ManagedAttribute @Description("Number of DNS threads")
	public int getDnsThreads() {
		return rc.dnsThreads;
	}

	@ManagedAttribute
	public void setFetchingThreads(final int fetchingThreads) throws IllegalArgumentException, NoSuchAlgorithmException, IOException {
		frontier.fetchingThreads(rc.fetchingThreads = fetchingThreads);
	}

	@ManagedAttribute @Description("Number of fetching threads")
	public int getFetchingThreads() {
		return rc.fetchingThreads;
	}

	@ManagedAttribute
	public void setParsingThreads(final int parsingThreads) throws IllegalArgumentException {
		frontier.parsingThreads(rc.parsingThreads = parsingThreads);
	}

	@ManagedAttribute @Description("Number of parsing threads (usually, no more than the number of available cores)")
	public int getParsingThreads() {
		return rc.parsingThreads;
	}

	@ManagedAttribute
	public void setFetchFilter(String spec) throws ParseException {
		rc.fetchFilter = new FilterParser<>(URI.class).parse(spec);
	}

	@ManagedAttribute @Description("Filters that will be applied to all URLs out of the frontier to decide whether to fetch them")
	public String getFetchFilter() {
		return rc.fetchFilter.toString();
	}

	@ManagedAttribute
	public void setScheduleFilter(String spec) throws ParseException {
		rc.scheduleFilter = new FilterParser<>(Link.class).parse(spec);
	}

	@ManagedAttribute @Description("Filter that will be applied to all URLs obtained by parsing a page before scheduling them")
	public String getScheduleFilter() {
		return rc.scheduleFilter.toString();
	}

	@ManagedAttribute
	public void setParseFilter(String spec) throws ParseException {
		rc.parseFilter = new FilterParser<>(URIResponse.class).parse(spec);
	}

	@ManagedAttribute @Description("Filter that will be applied to all fetched responses to decide whether to parse them")
	public String getParseFilter() {
		return rc.parseFilter.toString();
	}

	@ManagedAttribute
	public void setFollowFilter(String spec) throws ParseException {
		rc.followFilter = new FilterParser<>(URIResponse.class).parse(spec);
	}

	@ManagedAttribute @Description("Filter that will be applied to all fetched responses to decide whether to follow their links")
	public String getFollowFilter() {
		return rc.followFilter.toString();
	}

	@ManagedAttribute
	public void setStoreFilter(String spec) throws ParseException {
		rc.storeFilter = new FilterParser<>(URIResponse.class).parse(spec);
	}

	@ManagedAttribute @Description("Filter that will be applied to all fetched responses to decide whether to store them")
	public String getStoreFilter() {
		return rc.storeFilter.toString();
	}

	@ManagedAttribute
	public void setResponseBodyMaxByteSize(final int responseBodyMaxByteSize) {
		rc.responseBodyMaxByteSize = responseBodyMaxByteSize;
	}

	@ManagedAttribute @Description("The maximum size (in bytes) of a response body (the exceeding part will not be stored)")
	public long getResponseBodyMaxByteSize () {
		return rc.responseBodyMaxByteSize;
	}

	@ManagedAttribute
	public void setKeepAliveTime(final int keepAliveTime) {
		rc.keepAliveTime = keepAliveTime;
	}

	@ManagedAttribute @Description("If zero, connections are closed at each downloaded resource. Otherwise, the time span to download continuously from the same site using the same connection")
	public long getKeepAliveTime () {
		return rc.keepAliveTime;
	}

	@ManagedAttribute
	public void setSchemeAuthorityDelay(final long schemeAuthorityDelay) {
		rc.schemeAuthorityDelay = schemeAuthorityDelay;
	}

	@ManagedAttribute @Description("Delay in milliseconds between two consecutive fetches from the same scheme+authority")
	public long getUrlDelay() {
		return rc.schemeAuthorityDelay;
	}

	@ManagedAttribute
	public void setIpDelay(final long ipDelay) {
		rc.ipDelay = ipDelay;
	}

	@ManagedAttribute @Description("Delay in milliseconds between two consecutive fetches from the same IP address")
	public long getIpDelay() {
		return rc.ipDelay;
	}

	@ManagedAttribute
	public void setMaxUrls(final long maxUrls) {
		rc.maxUrls = maxUrls;
	}

	@ManagedAttribute @Description("Maximum number of URLs to crawl")
	public long getMaxUrls() {
		return rc.maxUrls;
	}

	@ManagedAttribute
	public void setSocketTimeout(final int socketTimeout) {
		rc.socketTimeout = socketTimeout;
	}

	@ManagedAttribute @Description("Timeout in milliseconds for opening a socket")
	public int getSocketTimeout() {
		return rc.socketTimeout;
	}

	@ManagedAttribute
	public void setConnectionTimeout(final int connectionTimeout) {
		rc.connectionTimeout = connectionTimeout;
	}

	@ManagedAttribute @Description("Socket connection timeout in milliseconds")
	public int getConnectionTimeout() {
		return rc.connectionTimeout;
	}

	@ManagedAttribute
	public void setRobotsExpiration(final long robotsExpiration) {
		rc.robotsExpiration = robotsExpiration;
	}

	@ManagedAttribute @Description("Milliseconds after which the robots.txt file is no longer considered valid")
	public long getRobotsExpiration() {
		return rc.robotsExpiration;
	}

	@ManagedAttribute
	public void setWorkbenchMaxByteSize(final long workbenchSize) {
		rc.workbenchMaxByteSize = workbenchSize;
	}

	@ManagedAttribute @Description("Maximum size of the workbench in bytes")
	public long getWorkbenchMaxByteSize() {
		return rc.workbenchMaxByteSize;
	}

	@ManagedAttribute
	public void setUrlCacheMaxByteSize(final long urlCacheSize) {
		rc.urlCacheMaxByteSize = urlCacheSize;
	}

	@ManagedAttribute @Description("Size in bytes of the URL cache")
	public long getUrlCacheMaxByteSize() {
		return rc.urlCacheMaxByteSize;
	}

	/*Statistical Properties, as reported by StatsThread */

	/*@ManagedAttribute @Description("The time elapsed since the start of the crawl")
	public long getTime() {
		return statsThread.requestLogger.millis();
	}*/

	@ManagedAttribute @Description("Approximate size of the workbench in bytes")
	public long getWorkbenchByteSize() {
		return frontier.weightOfpathQueriesInQueues.get();
	}

	@ManagedAttribute @Description("Overall size of the store (includes archetypes and duplicates)")
	public long getStoreSize() {
		return frontier.archetypes() + frontier.duplicates.get();
	}

	@ManagedAttribute @Description("Number of stored archetypes")
	public long getArchetypes() {
		return frontier.archetypes();
	}

	@ManagedAttribute @Description("Number of stored archetypes having other status")
	public long getArchetypesOther() {
		return frontier.archetypesStatus[0].get();
	}

	@ManagedAttribute @Description("Number of stored archetypes having status 1xx")
	public long getArchetypes1xx() {
		return frontier.archetypesStatus[1].get();
	}

	@ManagedAttribute @Description("Number of stored archetypes having status 2xx")
	public long getArchetypes2xx() {
		return frontier.archetypesStatus[2].get();
	}

	@ManagedAttribute @Description("Number of stored archetypes having status 3xx")
	public long getArchetypes3xx() {
		return frontier.archetypesStatus[3].get();
	}

	@ManagedAttribute @Description("Number of stored archetypes having status 4xx")
	public long getArchetypes4xx() {
		return frontier.archetypesStatus[4].get();
	}

	@ManagedAttribute @Description("Number of stored archetypes having status 5xx")
	public long getArchetypes5xx() {
		return frontier.archetypesStatus[5].get();
	}

	@ManagedAttribute @Description("Statistics about the number of outlinks of each archetype")
	public String getArchetypeOutdegree(){
		return frontier.outdegree.toString();
	}

	@ManagedAttribute @Description("Statistics about the number of outlinks of each archetype, without considering the links to the same corresponding host")
	public String getArchetypeExternalOutdegree(){
		return frontier.externalOutdegree.toString();
	}

	@ManagedAttribute @Description("Statistic about the content length of each archetype")
	public String getArchetypeContentLength(){
		return frontier.contentLength.toString();
	}

	@ManagedAttribute @Description("Number of archetypes whose indicated content type starts with text (case insensitive)")
	public long getArchetypeContentTypeText(){
		return frontier.contentTypeText.get();
	}

	@ManagedAttribute @Description("Number of archetypes whose indicated content type starts with image (case insensitive)")
	public long getArchetypeContentTypeImage(){
		return frontier.contentTypeImage.get();
	}

	@ManagedAttribute @Description("Number of archetypes whose indicated content type starts with application (case insensitive)")
	public long getArchetypeContentTypeApplication(){
		return frontier.contentTypeApplication.get();
	}

	@ManagedAttribute @Description("Number of archetypes whose indicated content type does not start with text, image, or application (case insensitive)")
	public long getArchetypeContentTypeOthers(){
		return frontier.contentTypeOthers.get();
	}

	@ManagedAttribute @Description("Number of requests")
	public long getRequests() {
		return frontier.fetchedResources.get() + frontier.fetchedRobots.get();
	}

	@ManagedAttribute @Description("Number of responses")
	public long getResources() {
		return frontier.fetchedResources.get();
	}

	@ManagedAttribute @Description("Number of transferred bytes")
	public long getBytes() {
		return frontier.transferredBytes.get();
	}

	@ManagedAttribute @Description("Number of URLs received from other agents")
	public long getReceivedURLs() {
		return frontier.numberOfReceivedURLs.get();
	}

	@ManagedAttribute @Description("Number of duplicates")
	public long getDuplicates() {
		return frontier.duplicates.get();
	}

	@ManagedAttribute @Description("Percentage of duplicates")
	public double getDuplicatePercentage() {
		return 100.0 * frontier.duplicates.get() / (1 + frontier.archetypes());
	}

	@ManagedAttribute @Description("Number of ready URLs")
	public long getReadyURLs() {
		return frontier.readyURLs.size64();
	}

	@ManagedAttribute @Description("Number of FetchingThread waits")
	public long getFetchingThreadWaits() {
		return frontier.fetchingThreadWaits.get();
	}

	@ManagedAttribute @Description("Overall FetchingThread waiting time")
	public long getFetchingThreadTotalWaitTime() {
		return frontier.fetchingThreadWaitingTimeSum.get();
	}

	@ManagedAttribute @Description("URLs in VisitState queues")
	public long getURLsInQueues() {
		return frontier.pathQueriesInQueues.get();
	}

	@ManagedAttribute @Description("Percentage of workbench maximum size in used")
	public double getURLsInQueuesPercentage() {
		return 100.0 * frontier.weightOfpathQueriesInQueues.get() / frontier.rc.workbenchMaxByteSize;
	}

	@ManagedAttribute @Description("Distribution of URL among all VisitState instances (in position i, number of instances having 2^i URLs)")
	public int[] getQueueDistribution() {
		return frontier.getStatsThread().dist;
	}

	@ManagedAttribute @Description("Number of unresolved VisitState instances")
	public long getUnresolved() {
		return frontier.getStatsThread().unresolved;
	}

	@ManagedAttribute @Description("Number of path+queries in broken VisitState instances")
	public long getBroken() {
		return frontier.getStatsThread().brokenPathQueryCount;
	}

	@ManagedAttribute @Description("Average number of VisitState instances in a WorkbenchEntry")
	public double getEntryAverage() {
		return frontier.getStatsThread().entrySummaryStats.mean();
	}

	@ManagedAttribute @Description("Maximum number of VisitState instances in a WorkbenchEntry")
	public double getEntryMax() {
		return frontier.getStatsThread().entrySummaryStats.max();
	}

	@ManagedAttribute @Description("Minimum number of VisitState instances in a WorkbenchEntry")
	public double getEntryMin() {
		return frontier.getStatsThread().entrySummaryStats.min();
	}

	@ManagedAttribute @Description("Variance of the number of VisitState instances in a WorkbenchEntry")
	public double getEntryVariance() {
		return frontier.getStatsThread().entrySummaryStats.variance();
	}

	@ManagedAttribute @Description("Number of VisitState instances")
	public int getVisitStates() {
		return frontier.getStatsThread().getVisitStates();
	}

	@ManagedAttribute @Description("Number of resolved VisitState instances")
	public long getResolvedVisitStates() {
		return frontier.getStatsThread().resolvedVisitStates;
	}

	@ManagedAttribute @Description("Number of entries on the workbench")
	public long getIPOnWorkbench() {
		return frontier.workbench.approximatedSize();
	}

	@ManagedAttribute @Description("Number of VisitState instances on the workbench")
	public long getVisitStatesOnWorkbench() {
		return (long)frontier.getStatsThread().entrySummaryStats.sum();
	}

	@ManagedAttribute @Description("Number of VisitState instances on the todo list")
	public long getToDoSize() {
		return frontier.todo.size();
	}

	@ManagedAttribute @Description("Number of FetchingThread instances downloading data")
	public int getActiveFecthingThreads() {
		return  (int)(frontier.rc.fetchingThreads - frontier.results.size());
	}

	@ManagedAttribute @Description("Number of FetchingThread instances waiting for parsing")
	public int getReadyToParse() {
		return  (int)frontier.results.size();
	}

	@ManagedAttribute @Description("Number of unknown hosts")
	public int getUnknownHosts() {
		return  frontier.unknownHosts.size();
	}

	@ManagedAttribute @Description("Number of broken VisitState instances")
	public long getBrokenVisitStates() {
		return  frontier.brokenVisitStates.get();
	}

	@ManagedAttribute @Description("Number of broken VisitState instances on the workbench")
	public long getBrokenVisitStatesOnWorkbench() {
		return  frontier.getStatsThread().brokenVisitStatesOnWorkbench;
	}

	@ManagedAttribute @Description("Number of new VisitState instances waiting to be resolved")
	public int getWaitingVisitStates() {
		return frontier.newVisitStates.size();
	}

	@ManagedAttribute @Description("Number of VisitState instances with path+queries on disk")
	public long getVisitStatesOnDisk() {
		return frontier.getStatsThread().getVisitStatesOnDisk();
	}

	@ManagedAttribute @Description("Current required front size")
	public long getRequiredFrontSize() {
		return frontier.requiredFrontSize.get();
	}

	public static void main(final String arg[]) throws Exception {
		final SimpleJSAP jsap = new SimpleJSAP(Agent.class.getName(), "Starts a BUbiNG agent (note that you must enable JMX by means of the standard Java system properties).",
				new Parameter[] {
					new FlaggedOption("weight", JSAP.INTEGER_PARSER, "1", JSAP.NOT_REQUIRED, 'w', "weight", "The agent weight."),
					new FlaggedOption("group", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'g', "group", "The JGroups group identifier (must be the same for all cooperating agents)."),
					new FlaggedOption("jmxHost", JSAP.STRING_PARSER, InetAddress.getLocalHost().getHostAddress(), JSAP.REQUIRED, 'h', "jmx-host", "The IP address (possibly specified by a host name) that will be used to expose the JMX RMI connector to other agents."),
					new FlaggedOption("rootDir", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'r', "root-dir", "The root directory."),
					new Switch("new", 'n', "new", "Start a new crawl"),
					new FlaggedOption("properties", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'P', "properties", "The properties used to configure the agent."),
					new UnflaggedOption("name", JSAP.STRING_PARSER, JSAP.REQUIRED, "The agent name (an identifier that must be unique across the group).")
			});

			final JSAPResult jsapResult = jsap.parse(arg);
			if (jsap.messagePrinted()) System.exit(1);

			// JMX *must* be set up.
			final String portProperty = System.getProperty(JMX_REMOTE_PORT_SYSTEM_PROPERTY);
			if (portProperty == null) throw new IllegalArgumentException("You must specify a JMX service port using the property " + JMX_REMOTE_PORT_SYSTEM_PROPERTY);

			final String name = jsapResult.getString("name");
			final int weight = jsapResult.getInt("weight");
			final String group = jsapResult.getString("group");
			final String host = jsapResult.getString("jmxHost");
			final int port = Integer.parseInt(portProperty);

			final BaseConfiguration additional = new BaseConfiguration();
			additional.addProperty("name", name);
			additional.addProperty("group", group);
			additional.addProperty("weight", Integer.toString(weight));
			additional.addProperty("crawlIsNew", Boolean.valueOf(jsapResult.getBoolean("new")));
			if (jsapResult.userSpecified("rootDir")) additional.addProperty("rootDir", jsapResult.getString("rootDir"));

			new Agent(host, port, new RuntimeConfiguration(new StartupConfiguration(jsapResult.getString("properties"), additional)));
			System.exit(0); // Kills remaining FetchingThread instances, if any.
	}
}
