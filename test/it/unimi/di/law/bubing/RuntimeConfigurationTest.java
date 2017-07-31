package it.unimi.di.law.bubing;

/*
 * Copyright (C) 2013 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import it.unimi.di.law.bubing.util.BURL;
import it.unimi.di.law.bubing.util.Helpers;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.commons.configuration.ConfigurationException;
import org.junit.Test;

import com.google.common.collect.Sets;
import com.martiansoftware.jsap.ParseException;
import com.martiansoftware.jsap.stringparsers.IntSizeStringParser;

public class RuntimeConfigurationTest {

	@Test
	public void testOptional() throws ConfigurationException, IllegalArgumentException, ClassNotFoundException, NoSuchFieldException, SecurityException, ParseException {
		StartupConfiguration startupConf = Helpers.getTestStartupConfiguration(this, null);
		assertEquals(((Integer)IntSizeStringParser.getParser().parse(StartupConfiguration.class.getDeclaredField("dnsCacheMaxSize").getAnnotation(StartupConfiguration.OptionalSpecification.class).value())).intValue(), startupConf.dnsCacheMaxSize);
	}

	@Test
	public void testSeed1() throws ConfigurationException, IOException, IllegalArgumentException, ClassNotFoundException {
		StartupConfiguration startupConf = Helpers.getTestStartupConfiguration(this, null);
		String[] expectedSeedStrings = new String[] { "http://foo.bar/", "http://foo.foo/" };
		startupConf.seed = expectedSeedStrings;
		RuntimeConfiguration conf = new RuntimeConfiguration(startupConf);
		HashSet<URI> actualSeed = Sets.newHashSet(conf.seed);
		Set<URI> expectedSeed = new HashSet<>();
		for (String s: expectedSeedStrings) expectedSeed.add(BURL.parse(s));
		assertEquals(expectedSeed, actualSeed);
	}

	@Test
	public void testSeedWithErrors() throws ConfigurationException, IOException, IllegalArgumentException, ClassNotFoundException {
		StartupConfiguration startupConf = Helpers.getTestStartupConfiguration(this, null);
		String[] expectedSeedStrings = new String[] { "http://foo.bar/", "foo.foo/", "http://gna.gna.gna/", "�����" };
		startupConf.seed = expectedSeedStrings;
		RuntimeConfiguration conf = new RuntimeConfiguration(startupConf);
		HashSet<URI> actualSeed = Sets.newHashSet(conf.seed);
		Set<URI> expectedSeed = new HashSet<>();
		for (String s: expectedSeedStrings) {
			URI uri = BURL.parse(s);
			if (uri != null && uri.isAbsolute()) expectedSeed.add(uri);
			else expectedSeed.add(null);
		}
		assertEquals(expectedSeed, actualSeed);
	}

	@Test
	public void testSeed2() throws ConfigurationException, IOException, IllegalArgumentException, ClassNotFoundException {
		StartupConfiguration startupConf = Helpers.getTestStartupConfiguration(this, null);
		URI[] expectedSeed = new URI[100];
		ArrayList<String> spec = new ArrayList<>();
		Random rand = new Random(0);
		File currentFile;
		PrintWriter writer = null;
		for (int i = 0; i < 100; i++) {
			String url = "http://0.0.0.0/" + rand.nextInt();
			expectedSeed[i] = BURL.parse(url);
			if (i % 10 == 0 && (i / 10) % 2 != 0) {
				currentFile = File.createTempFile("seed", String.valueOf(i / 10));
				currentFile.deleteOnExit();
				writer = new PrintWriter(currentFile);
				spec.add("file://" + currentFile.getAbsolutePath());
			}
			if ((i / 10) % 2 != 0) writer.println(url);
			else spec.add(url.toString());
			if (i % 10 == 9 && (i / 10) % 2 != 0) writer.close();
		}
		startupConf.seed = new String[spec.size()];
		spec.toArray(startupConf.seed);
		System.out.println(Arrays.toString(startupConf.seed));
		RuntimeConfiguration conf = new RuntimeConfiguration(startupConf);
		HashSet<URI> actualSeed = Sets.newHashSet(conf.seed);
		assertEquals(Sets.newHashSet(expectedSeed), actualSeed);
	}

	@Test
	public void testParseTime() {
		assertEquals(3500, StartupConfiguration.parseTime("  3500"));
		assertEquals(3500000000L, StartupConfiguration.parseTime("3500000000"));
		assertEquals(50000L, StartupConfiguration.parseTime("50 s"));
		assertEquals(5000L, StartupConfiguration.parseTime("5s"));
		assertEquals(3, StartupConfiguration.parseTime("3ms"));
		assertEquals(3 * 60 * 60 * 1000 + 4 * 1000, StartupConfiguration.parseTime("3h 4s"));
		assertEquals(3L * 24 * 60 * 60 * 1000, StartupConfiguration.parseTime(" 3   d   "));
		assertEquals((long)(1.5 * 24 * 60 * 60 * 1000 + 5.5 * 60 * 1000), StartupConfiguration.parseTime("1.5d 5.5m"));
		assertEquals(3 * 60 * 60 * 1000 + 4 * 1000 + 5, StartupConfiguration.parseTime("3h 4s 5   ms "));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testParseTimeWithExc1() {
		assertEquals(3L * 24 * 60 * 60 * 1000, StartupConfiguration.parseTime("3h 4d"));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testParseTimeWithExc2() {
		assertEquals(3L * 24 * 60 * 60 * 1000, StartupConfiguration.parseTime("3h 4u"));
	}

	@Test
	public void testMatcher() {
		assertTrue(RuntimeConfiguration.DOTTED_ADDRESS.matcher("0.0.0.0").matches());
		assertTrue(RuntimeConfiguration.DOTTED_ADDRESS.matcher("192.0.2.235").matches());
		assertTrue(RuntimeConfiguration.DOTTED_ADDRESS.matcher("0300.0000.0002.0353").matches());
		assertTrue(RuntimeConfiguration.DOTTED_ADDRESS.matcher("3221226219").matches());
		assertTrue(RuntimeConfiguration.DOTTED_ADDRESS.matcher("030000001353").matches());
		assertTrue(RuntimeConfiguration.DOTTED_ADDRESS.matcher("2001:0db8:0000:0000:0000:ff00:0042:8329").matches());
		assertTrue(RuntimeConfiguration.DOTTED_ADDRESS.matcher("2001:db8:0:0:0:ff00:42:8329").matches());

		assertTrue(RuntimeConfiguration.DOTTED_ADDRESS.matcher("0xC00002EB").matches());
		assertTrue(RuntimeConfiguration.DOTTED_ADDRESS.matcher("0xC0.0x00.0x02.0xEB").matches());
		assertTrue(RuntimeConfiguration.DOTTED_ADDRESS.matcher("0xC0.00.02.0xEB").matches());

		assertFalse(RuntimeConfiguration.DOTTED_ADDRESS.matcher("0xC0.00.02.EB").matches());
	}

}
