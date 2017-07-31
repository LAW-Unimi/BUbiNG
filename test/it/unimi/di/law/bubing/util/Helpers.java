package it.unimi.di.law.bubing.util;

/*
 * Copyright (C) 2004-2013 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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

//RELEASE-STATUS: DIST

import it.unimi.di.law.TestUtil;
import it.unimi.di.law.bubing.RuntimeConfiguration;
import it.unimi.di.law.bubing.StartupConfiguration;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;


/** A bunch of static methods useful for testing purposes. */
public class Helpers {

	public static final boolean DEBUG = false;
	public static final Logger LOGGER = LoggerFactory.getLogger(Helpers.class);

	/** A property that should be set iff one wants to run also
	 *  tests that access the network.
	 */
	public static final String TEST_WITH_NET_PROPERTY_NAME = "ubi.test.with.net";


	/** A method that tests whether the {@link #TEST_WITH_NET_PROPERTY_NAME} system
	 *  property is set. If it is not, before returning <code>true</code> the
	 *  name of the calling method is output so that the user knows that the
	 *  test was not run. The common usage is the following:
	 *
	 *  <pre>
	 *		public void testSomethingUsingNet(...) {
	 *  		if (TestHelpers.networkAccessDenied()) return;
	 *  		...go on with the test...
	 *  	}
	 *  </pre>
	 *
	 * @return true iff accessing the network is not allowed.
	 */
	public static boolean networkAccessDenied() {
		if (System.getProperty(TEST_WITH_NET_PROPERTY_NAME) == null) {
			try {
				throw new Exception();
			} catch (Exception e) {
				System.out.println("Test " + e.getStackTrace()[1] + " was not executed, because the system property " + TEST_WITH_NET_PROPERTY_NAME + " is not set");
				return true;
			}
		} else
			return false;
	}

	/** Returns a test configuration, contained in the file <code>bubing-test.properties</code> in the data directory.
	 *
	 * @param self the test that is requiring the configuration.
	 * @param prop the additional properties that override (some of) those contained in the file.
	 * @param newCrawl whether this configuration simulates a new crawl or not.
	 * @return the configuration.
	 * @throws ConfigurationException if some configuration error occurs.
	 */
	public static RuntimeConfiguration getTestConfiguration(Object self, BaseConfiguration prop, boolean newCrawl) throws ConfigurationException, IOException, IllegalArgumentException, ClassNotFoundException {
		prop.addProperty("crawlIsNew", Boolean.valueOf(newCrawl));
		return new RuntimeConfiguration(getTestStartupConfiguration(self, prop)) ;
	}

	/** Returns a test configuration, contained in the file <code>bubing-test.properties</code> in the data directory.
	 *
	 * @param self the test that is requiring the configuration.
	 * @return the configuration.
	 * @throws ConfigurationException if some configuration error occurs.
	 * @throws FileNotFoundException
	 */
	public static RuntimeConfiguration getTestConfiguration(Object self) throws ConfigurationException, IOException, IllegalArgumentException, ClassNotFoundException {
		return getTestConfiguration(self, new BaseConfiguration(), true);
	}

	/** Returns a test startup configuration, contained in the file <code>bubing-test.properties</code> in the data directory.
	 *
	 * @param self the test that is requiring the configuration.
	 * @param prop the additional properties that override (some of) those contained in the file.
	 * @return the configuration.
	 * @throws ConfigurationException if some configuration error occurs.
	 */
	public static StartupConfiguration getTestStartupConfiguration(Object self, BaseConfiguration prop) throws ConfigurationException, IllegalArgumentException, ClassNotFoundException {

		final File dir = Files.createTempDir();
		if (! dir.delete()) throw new AssertionError(); // It will be created by StartupConfiguration
		if (prop == null) prop = new BaseConfiguration();
		prop.setProperty("rootDir", dir.getAbsolutePath());

		StartupConfiguration conf = new StartupConfiguration(TestUtil.getTestFile(Helpers.class, "bubing-test.properties", true), prop);
		if (LOGGER.isDebugEnabled()) LOGGER.debug("Test configuration: " + conf);
		return conf;
	}

}
