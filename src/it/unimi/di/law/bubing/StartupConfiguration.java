package it.unimi.di.law.bubing;

/*
 * Copyright (C) 2012-2013 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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

import it.unimi.di.law.bubing.frontier.DNSThread;
import it.unimi.di.law.bubing.frontier.FetchingThread;
import it.unimi.di.law.bubing.frontier.Frontier;
import it.unimi.di.law.bubing.frontier.ParsingThread;
import it.unimi.di.law.bubing.frontier.dns.DnsJavaResolver;
import it.unimi.di.law.bubing.parser.Parser;
import it.unimi.di.law.bubing.sieve.AbstractSieve;
import it.unimi.di.law.bubing.spam.SpamDetector;
import it.unimi.di.law.bubing.store.Store;
import it.unimi.di.law.bubing.util.ByteArrayDiskQueue;
import it.unimi.di.law.bubing.util.FetchData;
import it.unimi.di.law.warc.filters.Filter;
import it.unimi.di.law.warc.filters.URIResponse;
import it.unimi.di.law.warc.filters.parser.FilterParser;
import it.unimi.dsi.fastutil.io.InspectableFileCachedInputStream;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.lang.ObjectParser;
import it.unimi.dsi.util.BloomFilter;

import java.io.File;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.conn.DnsResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martiansoftware.jsap.ParseException;
import com.martiansoftware.jsap.stringparsers.IntSizeStringParser;
import com.martiansoftware.jsap.stringparsers.LongSizeStringParser;

//RELEASE-STATUS: DIST

/** A class whose public fields represent the configuration of BUbiNG at startup.
 *
 * <p>All public fields of this class represent a configuration property: <ul> <li>their type must be
 * either a primitive type or <code>String</code>; <li>their name coincides (case-sensitively) with
 * the name of the keys of the property file used to configure BUbiNG (see below);
 * <li>although fields are all public and non-final, they should only be written by the constructors
 * of this class; every other access should be read only. </ul>
 *
 * <p>For integer or long fields, the configuration values are
 * passed to a {@link IntSizeStringParser} or {@link LongSizeStringParser}, respectively. Thus, you
 * can use any size specification allowed by those parsers such as
 * <code>K</code> (10<sup>3</sup>), <code>Ki</code> (2<sup>10</sup>), <code>M</code> (10<sup>6</sup>), <code>Mi</code> (2<sup>20</sup>), etc.
 *
 * <p><strong>Note that all fields are
 * mandatory</strong> unless marked with a {@link OptionalSpecification} annotation: if a non-annotated
 * field is missing, or if it is of the wrong type, or if it
 * fails to satisfy the extra condition required for that field, a
 * {@link ConfigurationException} will be thrown. The same is happen if
 * one specifies an unknown field.
 *
 * <p>Fields marked with a {@link ManyValuesSpecification} annotation can be specified multiple times. As usual,
 * you can also enumerate several values separated by a comma.
 *
 * <h2>Internals</h2>
 *
 * <p>If a field named <code>xxYyyZzzz</code> requires some additional check (e.g., that its value
 * is set in a certain range), a method named <code>checkXxYyyZzzz</code> with signature <pre>
 * public void checkXxYyyZzzz() throws ConfigurationException </pre> should be added to this class:
 * this method should throw an exception if the field fails to satisfy the required condition; it
 * may also perform some logging.
 *
 * <p>A configuration is constructed by reflection using one of the class constructors. The
 * {@link StartupConfiguration#StartupConfiguration(Configuration)} takes a
 * {@link org.apache.commons.configuration.Configuration} and sets the fields by reflection (field
 * names are used to look up for properties in the given configuration, and the values found are
 * used to initialize the corresponding fields).
 *
 * <p>A number of facility constructors are provided that read the configuration from a file
 * (provided as a {@linkplain #StartupConfiguration(File) file} or as a
 * {@linkplain #StartupConfiguration(String) filename}) in the
 * {@linkplain PropertiesConfiguration classic properties format}.
 *
 * <p>Moreover, some additional constructors are provided that read the configuration from a file
 * and possibly change some of them; see for example
 * {@link #StartupConfiguration(String, Configuration)}.
 */

public class StartupConfiguration {
	private final static Logger LOGGER = LoggerFactory.getLogger(StartupConfiguration.class);

	// WARNING: Since this class is mainly manipulated by reflection, modifications require great care.

	/** A marker for optional specifications with a default parameter. */
	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.FIELD)
	public @interface OptionalSpecification{ String value(); };

	/** A marker for specifications that may have multiple values. You can use multiple keys or associate with a key several values separated by a comma. */
	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.FIELD)
	public @interface ManyValuesSpecification{};

	/** A marker for time specifications; such specification are by default in milliseconds, but it is possible
	 * to use suffixes <code>ms</code> (milliseconds), <code>s</code> (seconds),  <code>m</code> (minutes),  <code>h</code> (hours) and  <code>d</code> (days). */
	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.FIELD)
	public @interface TimeSpecification{};

	/** A marker for {@linkplain Filter filter} specifications. */
	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.FIELD)
	public @interface FilterSpecification{
		@SuppressWarnings("rawtypes")
		Class type();
	};

	/** A marker for the {@link Store} class specification. */
	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.FIELD)
	public @interface StoreSpecification{}

	/** A marker for the {@link DnsResolver} class specification. */
	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.FIELD)
	public @interface DnsResolverSpecification{}

	/** The name of this agent; it must be unique within its group. */
	public String name;

	/** The group of this agent; all agents belonging to the same group will coordinate their crawling activity. */
	public String group;

	/** The weight of this agent; agents are assigned a part of the crawl that is proportional to their weight. */
	public int weight;

	/** The maximum number of URLs we shall download from each scheme+authority. */
	public int maxUrlsPerSchemeAuthority;

	/** The number of {@linkplain FetchingThread fetching threads} (hundreds or even thousands). */
	public int fetchingThreads;

	/** The number of {@linkplain ParsingThread parsing threads} (usually, the number of available cores). */
	public int parsingThreads;

	/** The number of {@linkplain DNSThread DNS threads} (usually few dozens, depending on the server). */
	public int dnsThreads;

	/** A filter that will be applied to all ready URLs to decide whether to fetch them. */
	@FilterSpecification(type = URI.class)
	public Filter<URI> fetchFilter;

	/** A filter that will be applied to all URLs obtained by parsing a page before scheduling them. */
	@FilterSpecification(type = URI.class)
	public Filter<URI> scheduleFilter;

	/** A filter that will be applied to all fetched resources to decide whether to parse them. */
	@FilterSpecification(type = URIResponse.class)
	public Filter<URIResponse> parseFilter;

	/** A filter that will be applied to all parsed resources to decide whether to follow their links. */
	@FilterSpecification(type = URIResponse.class)
	public Filter<URIResponse> followFilter;

	/** A filter that will be applied to all fetched resources to decide whether to store them. */
	@FilterSpecification(type = URIResponse.class)
	public Filter<URIResponse> storeFilter;

	/** If zero, connections are closed at each downloaded resource.
	 * Otherwise, the time span to download continuously from
	 * the same site using the same connection. */
	@OptionalSpecification(value="0")
	@TimeSpecification
	public long keepAliveTime;

	/** The minimum delay between two consecutive fetches from the same scheme+authority. */
	@TimeSpecification
	public long schemeAuthorityDelay;

	/** The minimum delay between two consecutive fetches from the same IP address. */
	@TimeSpecification
	public long ipDelay;

	/** The maximum number of URLs to crawl. */
	public long maxUrls;

	/** The precision of the {@linkplain BloomFilter Bloom filter} used for duplicate detection (usually, at least 1/{@link #maxUrls}). */
	public double bloomFilterPrecision;

	/** A URL from which BUbiNG will start crawling. If it starts with <code>file:</code>,
	 * it is assumed to point to an ASCII file containing on each line a seed URL. */
	@ManyValuesSpecification
	public String[] seed;

	/** An IPv4 address that should be blacklisted (i.e., not crawled). If it starts with <code>file:</code>,
	 * it is assumed to point to an ASCII file containing on each line a blacklisted IPv4 address. */
	@ManyValuesSpecification
	@OptionalSpecification(value="")
	public String[] blackListedIPv4Addresses;

	/** A host that should be blacklisted (i.e., not crawled). If it starts with <code>file:</code>,
	 * it is assumed to point to an ASCII file containing on each line a blacklisted host. */
	@ManyValuesSpecification
	@OptionalSpecification(value="")
	public String[] blackListedHosts;

	/** The socket timeout. */
	@TimeSpecification
	public int socketTimeout;

	/** The socket connection timeout in milliseconds. */
	@TimeSpecification
	public int connectionTimeout;

	/** Size of the buffer for {@link InspectableFileCachedInputStream} instances, in bytes. Each {@linkplain FetchingThread fetching thread} holds such a buffer. */
	public int fetchDataBufferByteSize;

	/** The proxy host, if a proxy should be used; an empty value means that the proxy should not be set. */
	@OptionalSpecification(value="")
	public String proxyHost;

	/** The proxy port, meaningful only if {@link #proxyHost} is not empty. */
	@OptionalSpecification(value="8080")
	public int proxyPort;

	/** The cookie policy to be used. See {@link CookieSpecs}. */
	public String cookiePolicy;

	/** The maximum overall size for the (external form of) the cookies accepted from a single host. */
	public int cookieMaxByteSize;

	/** The User Agent header used for HTTP requests. */
	public String userAgent;

	/** The From header used for HTTP requests. It can be empty, in which case no <code>From</code> header will be emitted. */
	@OptionalSpecification(value="")
	public String userAgentFrom;

	/** The delay after which the <code>robots.txt</code> file is no longer considered valid. */
	@TimeSpecification
	public long robotsExpiration;

	/** A root directory from which the remainig one will be stemmed, if
	 * they are relative. Note that this directory can be preexisting, and can be
	 * just a dot, denoting the current directory. */
	public String rootDir;

	/** A directory where the retrieved content will be written. It must <em>not</em> exist. */
	@OptionalSpecification(value="store")
	public String storeDir;

	/** A directory where the content overflowing the in-memory buffers of {@link FetchData} instances
	 *  (of {@link #fetchDataBufferByteSize} bytes) will be stored using an {@link InspectableFileCachedInputStream}.  It must <em>not</em> exist. */
	@OptionalSpecification(value="cache")
	public String responseCacheDir;

	/** A directory for storing files related to the {@linkplain AbstractSieve sieve}.  It must <em>not</em> exist. */
	@OptionalSpecification(value="sieve")
	public String sieveDir;

	/** A directory for storing files (mainly queues managed by {@link ByteArrayDiskQueue}) related to the {@linkplain Frontier frontier}.  It must <em>not</em> exist. */
	@OptionalSpecification(value="frontier")
	public String frontierDir;

	/** The maximum size (in bytes) of a response body. The exceeding part will not be stored. */
	public int responseBodyMaxByteSize;

	/** The algorithm used for digesting pages (for duplicate filtering). */
	public String digestAlgorithm;

	/** A {@link Parser} specification that will be parsed using an {@link ObjectParser}. */
	@ManyValuesSpecification
	public String[] parserSpec;

	/** Whether we should start in paused state. */
	public boolean startPaused;

	/** The class used to {@link Store} the resources. */
	@StoreSpecification
	@OptionalSpecification(value="it.unimi.di.law.bubing.store.WarcStore")
	public Class<? extends Store> storeClass;

	/** The maximum size of the workbench in bytes. */
	public long workbenchMaxByteSize;

	/** The maximum size of the virtualizer in bytes; this field is ignored if the virtualizer does not need to be sized. */
	@OptionalSpecification(value="1Gi")
	public long virtualizerMaxByteSize;

	/** The maximum size of the URL cache in bytes. */
	public long urlCacheMaxByteSize;

	/** The number of slots in the sieve. A flush happen when this space is filled with 64-bit hashes. Note that due the needs
	 * of indirect sorting 12 bytes will be allocated for each slot. */
	public int sieveSize;

	/** The size of the two buffers used to read the 64-bit hashes stored by the sieve during flushes. Will be allocated using {@link ByteBuffer#allocateDirect(int)}. */
	@OptionalSpecification(value="64Ki")
	public int sieveStoreIOBufferByteSize;

	/** The I/O buffer used to write the auxiliary file (containing URLs) and to read it back during flushes. */
	@OptionalSpecification(value="64Ki")
	public int sieveAuxFileIOBufferByteSize;

	/** A {@link DnsResolver}.
	 * @see it.unimi.di.law.bubing.frontier.dns
	 */
	@DnsResolverSpecification
	@OptionalSpecification(value="it.unimi.di.law.bubing.frontier.dns.DnsJavaResolver")
	public Class<? extends DnsResolver> dnsResolverClass;

	/** Maximum number of entries cached by the DNS resolutor when using {@link DnsJavaResolver}. */
	@OptionalSpecification(value="10000")
	public int dnsCacheMaxSize;

	/** Expiration time for positive DNS answers when using {@link DnsJavaResolver}. */
	@OptionalSpecification(value="1h")
	@TimeSpecification
	public long dnsPositiveTtl;

	/** Expiration time for negative DNS answers when using {@link DnsJavaResolver}. */
	@OptionalSpecification(value="1m")
	@TimeSpecification
	public long dnsNegativeTtl;

	/** Whether this is a new crawl. */
	public boolean crawlIsNew;

	/** An optional {@link SpamDetector}; this {@link URI} should point to a serialized instance. */
	@OptionalSpecification(value="")
	public String spamDetectorUri;

	/** The number of pages per scheme+authority after which spam detection is performed. */
	@OptionalSpecification(value="100")
	public int spamDetectionThreshold;

	/** The number of pages per scheme+authority after which spam detection is performed again periodically. If {@link Integer#MAX_VALUE}, spam detection is performed only
	 * after {@link #spamDetectionThreshold} pages. */
	@OptionalSpecification(value="2147483647")
	public int spamDetectionPeriodicity;


	/* Checks */

	@SuppressWarnings("unused")
	private void checkUrlDelay() throws ConfigurationException {
		if (schemeAuthorityDelay < 1000) LOGGER.warn("You selected a small URL delay (" + schemeAuthorityDelay + "); this is going to disturb people");
	}

	@SuppressWarnings("unused")
	private void checkIpDelay() throws ConfigurationException {
		if (ipDelay < 100) LOGGER.warn("You selected a small IP delay (" + ipDelay + "); this is going to disturb people");
	}

	@SuppressWarnings("unused")
	private void checkBloomFilterPrecision() throws ConfigurationException {
		if (bloomFilterPrecision > 1) {
			LOGGER.error("Bloom-filter precision must be smaller than one");
			throw new IllegalArgumentException("Bloom-filter precision must be smaller than one");
		}
		if (bloomFilterPrecision > 1E-6) LOGGER.warn("You selected a low Bloom-filter precision (" + bloomFilterPrecision + "); you are going to get a lot of false duplicates");
	}

	/** If true, {@link #checkRootDir()} has already been called. This flag is necessary because we cannot guarantee
	 * the order in which fields are enumerated by {@link Class#getDeclaredFields()}. */
	private boolean rootDirChecked;

	private void checkRootDir() throws ConfigurationException {
		if (rootDirChecked) return;
		final File d = new File(rootDir);
		if (crawlIsNew) {
			if (d.exists()) throw new ConfigurationException("Root directory " + d + " exists");
			if (! d.mkdirs()) throw new ConfigurationException("Cannot create root directory " + d);
		}
		else if (! d.exists()) throw new ConfigurationException("Cannot find root directory " + rootDir + " for the crawl");
		rootDirChecked = true;
	}

	@SuppressWarnings("unused")
	private void checkRobotsExpiration() throws ConfigurationException {
		// At least one hour.
		if (robotsExpiration < 3600000) LOGGER.warn("You selected a low robots expiration time (" + robotsExpiration + " ms)");
	}

	/** Returns a {@link File} object representing a child relative
	 * to a parent, or just the child, if absolute.
	 *
	 * @param parent a parent directory.
	 * @param child a (possibly absolute) child file or directory.
	 * @return {@code child}, if it is absolute; {@code child} relative to {@code parent}, if it is relative.
	 */
	public static File subDir(final String parent, final String child) {
		final File d = new File(child);
		return d.isAbsolute() ? d : new File(parent, child);
	}

	private void chkSubDir(final String dir) throws ConfigurationException {
		final File d = subDir(rootDir, dir);
		if (crawlIsNew) {
			if (d.exists()) throw new ConfigurationException("Directory " + d + " exists");
			if (! d.mkdirs()) throw new ConfigurationException("Cannot create directory " + d);
		}
		else if (! d.exists()) throw new ConfigurationException("Directory " + d + " does not exist");
	}

	@SuppressWarnings("unused")
	private void checkStoreDir() throws ConfigurationException {
		checkRootDir();
		chkSubDir(storeDir);
	}

	@SuppressWarnings("unused")
	private void checkResponseCacheDir() throws ConfigurationException {
		checkRootDir();
		chkSubDir(responseCacheDir);
	}

	@SuppressWarnings("unused")
	private void checkSieveDir() throws ConfigurationException {
		checkRootDir();
		chkSubDir(sieveDir);
	}

	@SuppressWarnings("unused")
	private void checkFrontierDir() throws ConfigurationException {
		checkRootDir();
		chkSubDir(frontierDir);
	}

	/** Populate the object fields starting from the given configuration. The configuration should contain
	 *  at least one entry for every public instance field of this class, whose name and type coincide with the name
	 *  and type of the given field.
	 *
	 * <p>Field are parsed in the standard way (i.e., we invoke {@link Configuration}'s methods such as {@link Configuration#getDouble(String)})
	 * with the exception of integer and long fields, which are passed to a
	 * {@link IntSizeStringParser} and {@link LongSizeStringParser}, respectively. Thus, you can use any size specification
	 * allowed by those parsers.
	 *
	 * @param configuration the configuration on the basis of which this object should be constructed.
	 * @throws ConfigurationException if some field is not set, or if it has the wrong type, or if it fails to satisfy the
	 *   corresponding <code>check...</code> method.
	 * @throws IllegalArgumentException
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public StartupConfiguration(final Configuration configuration) throws ConfigurationException, ClassNotFoundException {
		if (LOGGER.isDebugEnabled()) LOGGER.debug("Required configuration: " + ConfigurationUtils.toString(configuration));
		for (Field f : getClass().getDeclaredFields()) {
			if ((f.getModifiers() & Modifier.PUBLIC) == 0 || (f.getModifiers() & Modifier.STATIC) != 0) continue;
			final String name = f.getName();
			final Class<?> type = f.getType();
			if (! configuration.containsKey(name)) {
				final OptionalSpecification optional = f.getAnnotation(OptionalSpecification.class);
				if (optional != null) configuration.setProperty(name, optional.value());
				else throw new ConfigurationException("No property for field '" + name  + "'");
			}
			if (f.getAnnotation(ManyValuesSpecification.class) == null && configuration.getStringArray(name).length > 1) throw new ConfigurationException("Field '" + name + "' has been specified multiple times");
			f.setAccessible(true);
			try {
				if (type == boolean.class)
					f.setBoolean(this, configuration.getBoolean(name));
				else if (type == byte.class)
					f.setByte(this, configuration.getByte(name));
				else {
					final String value = configuration.getString(name);
					if (value.isEmpty() && type != String.class && type != String[].class) throw new ConfigurationException("Empty string specified for configuration property " + name);
					if (type == char.class)
						f.setChar(this, value.charAt(0));
					else if (type == double.class)
						f.setDouble(this, configuration.getDouble(name));
					else if (type == float.class)
						f.setFloat(this, configuration.getFloat(name));
					else if (type == int.class) {
						if (f.getAnnotation(TimeSpecification.class) != null) {
							final long time = StartupConfiguration.parseTime(value);
							if (time > Integer.MAX_VALUE) throw new IllegalArgumentException("Time specification exceeds integer maximum value: " + value);
							f.setInt(this, (int)time);
						}
						else f.setInt(this, ((Integer)IntSizeStringParser.getParser().parse(value)).intValue());
					}
					else if (type == long.class) {
						if (f.getAnnotation(TimeSpecification.class) != null) {
							f.setLong(this, StartupConfiguration.parseTime(value));
						}
						else f.setLong(this, ((Long)LongSizeStringParser.getParser().parse(value)).longValue());
					}
					else if (type == short.class)
						f.setShort(this, configuration.getShort(name));
					else if (type == String.class)
						f.set(this, value);
					else if (type == String[].class)
						f.set(this, configuration.getStringArray(name));
					else if (f.getAnnotation(StoreSpecification.class) != null)
						f.set(this, Class.forName(value));
					else if (f.getAnnotation(DnsResolverSpecification.class) != null)
						f.set(this, Class.forName(value));
					else {
						Class filterType = (f.getAnnotation(FilterSpecification.class)).type();
						f.set(this, new FilterParser(filterType).parse(value));
					}
				}
			} catch (IllegalAccessException impossible) { // due to setAccessible
				throw new RuntimeException(impossible);
			} catch (ParseException jsap) { // due to a JSAP parser
				throw new ConfigurationException(jsap);
			} catch (it.unimi.di.law.warc.filters.parser.ParseException filterException) {
				throw new ConfigurationException(filterException);
			}
			f.setAccessible(false);
		}
		for (Method m : getClass().getDeclaredMethods()) {
			final String name = m.getName();
			if (name.startsWith("check"))
				try {
					m.invoke(this);
				} catch (IllegalAccessException impossible) {
					throw new RuntimeException(impossible);
				} catch (InvocationTargetException e) {
					if (e.getCause() instanceof ConfigurationException) throw (ConfigurationException)e.getCause();
					throw new ConfigurationException(e.getCause());
				}

		}

		for(Iterator<String> keys = configuration.getKeys(); keys.hasNext();) {
			final String key = keys.next();
			try {
				final Field field = getClass().getField(key);
				if ((field.getModifiers() & Modifier.PUBLIC) == 0 || (field.getModifiers() & Modifier.STATIC) != 0) throw new NoSuchFieldException();
			}
			catch (NoSuchFieldException e) {
				throw new RuntimeException("There is no configuration parameter named \"" + key +"\"");
			}
		}
	}

	/** Creates a configuration starting from a given file and possibly adding and/or overriding some
	 *  properties with new values.
	 *
	 * @param file the file whence the base configuration should be read.
	 * @param additionalProp the set of additional properties, some of which may override the values found in the file.
	 * @throws ConfigurationException  if some field is not set, or if it has the wrong type, or if it fails to satisfy the
	 *   corresponding <code>check...</code> method.
	 */
	public StartupConfiguration(File file, Configuration additionalProp) throws ConfigurationException, IllegalArgumentException, ClassNotFoundException {
		this(append(new PropertiesConfiguration(file), additionalProp));
	}

	/** Creates a configuration starting from a given file and possibly adding and/or overriding some
	 *  properties with new values.
	 *
	 * @param fileName the name of the file whence the base configuration should be read.
	 * @param additionalProp the set of additional properties, some of which may override the values found in the file.
	 * @throws ConfigurationException  if some field is not set, or if it has the wrong type, or if it fails to satisfy the
	 *   corresponding <code>check...</code> method.
	 */
	public StartupConfiguration(String fileName, Configuration additionalProp) throws ConfigurationException, IllegalArgumentException, ClassNotFoundException {
		this(append(new PropertiesConfiguration(fileName), additionalProp));
	}

	/** Creates a configuration starting from a given file.
	 *
	 * @param file the file whence the configuration should be read.
	 * @throws ConfigurationException  if some field is not set, or if it has the wrong type, or if it fails to satisfy the
	 *   corresponding <code>check...</code> method.
	 */
	public StartupConfiguration(File file) throws ConfigurationException, ClassNotFoundException {
		this(new PropertiesConfiguration(file));
	}

	/** Creates a configuration starting from a given file.
	 *
	 * @param fileName the name of the file whence the configuration should be read.
	 * @throws ConfigurationException  if some field is not set, or if it has the wrong type, or if it fails to satisfy the
	 *   corresponding <code>check...</code> method.
	 */
	public StartupConfiguration(String fileName) throws ConfigurationException, ClassNotFoundException {
		this(new PropertiesConfiguration(fileName));
	}

	/** Takes two configuration and returns their union, with the second overriding the first.
	 *
	 * @param base the base configuration.
	 * @param additional the additional set of properties, some of which may override those specified in <code>base</code>.
	 * @return the union of the two configurations, as specified above.
	 */
	private static Configuration append(Configuration base, Configuration additional) {
		CompositeConfiguration result = new CompositeConfiguration();
		result.addConfiguration(additional);
		result.addConfiguration(base);
		return result;
	}

	@Override
	public String toString() {
		Class<?> thisClass = getClass();
		Map<String,Object> values = new Object2ObjectOpenHashMap<>();
		for (Field f : thisClass.getDeclaredFields()) {
			if ((f.getModifiers() & Modifier.PUBLIC) == 0 || (f.getModifiers() & Modifier.STATIC) != 0) continue;
			try {
				values.put(f.getName(), f.get(this));
			} catch (IllegalAccessException e) {
				values.put(f.getName(), "<THIS SHOULD NOT HAPPEN>");
			}
		}
		return values.toString();
	}

	public static long parseTime(final String timeSpec) {
		StringTokenizer tokenizer = new StringTokenizer(timeSpec, " dhsm", true);
		double result = 0;
		long previousMultiplierValue = Long.MAX_VALUE;
		char previousMultiplier = '?';
		while (tokenizer.hasMoreElements()) {
			String token = tokenizer.nextToken();
			if (Character.isWhitespace(token.charAt(0))) continue;
			double element = 0;
			try {
				element = Double.parseDouble(token);
			} catch (NumberFormatException e) {
				throw new IllegalArgumentException("Number expected, " + token + " found");
			}
			if (tokenizer.hasMoreElements()) {
				do {
					token = tokenizer.nextToken();
				} while (tokenizer.hasMoreElements() && Character.isWhitespace(token.charAt(0)));
				if (! Character.isWhitespace(token.charAt(0))) {
					long multiplierValue;
					String mergedToken = token;
					String nextToken = tokenizer.hasMoreElements()? tokenizer.nextToken() : "";
					if (nextToken.length() > 0 && ! Character.isWhitespace(nextToken.charAt(0))) mergedToken += nextToken;
					if ("d".equals(mergedToken)) multiplierValue = TimeUnit.DAYS.toMillis(1);
					else if ("h".equals(mergedToken)) multiplierValue = TimeUnit.HOURS.toMillis(1);
					else if ("m".equals(mergedToken)) multiplierValue = TimeUnit.MINUTES.toMillis(1);
					else if ("s".equals(mergedToken)) multiplierValue = TimeUnit.SECONDS.toMillis(1);
					else if ("ms".equals(mergedToken)) multiplierValue = TimeUnit.MILLISECONDS.toMillis(1);
					else throw new IllegalArgumentException("Unknown time specifier " + mergedToken);
					if (multiplierValue >= previousMultiplierValue) throw new IllegalArgumentException("Cannot specify " + token + " after " + previousMultiplier);
					previousMultiplier = token.charAt(0);
					previousMultiplierValue = multiplierValue;
					element *= multiplierValue;
				}
			}
			result += element;
		}
		return (long)result;
	}
	}
