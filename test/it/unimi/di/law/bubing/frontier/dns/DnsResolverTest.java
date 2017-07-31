package it.unimi.di.law.bubing.frontier.dns;

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

import static org.junit.Assert.assertArrayEquals;

import java.net.UnknownHostException;

import org.junit.Test;

import com.google.common.primitives.Ints;

//RELEASE-STATUS: DIST

public class DnsResolverTest {

	@Test
	public void testJavaResolver() throws UnknownHostException {
		final JavaResolver resolver = new JavaResolver();
		assertArrayEquals(new byte[] { 0x7F, 0, 0, 1 }, resolver.resolve("127.0.0.1")[0].getAddress());
		assertArrayEquals(new byte[] { (byte)0xC0, 0, 0, 1 }, resolver.resolve("192.0.0.1")[0].getAddress());
		// TODO: fix test with a simple but correct IPv6 address
		//assertArrayEquals(new byte[] { (byte)0x20, 0x1, 1, 1, (byte)0x80, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1 }, resolver.resolve("2001:0101:8000:0101:0000:0000:0000:0001")[0].getAddress());
	}

	@Test
	public void testDnsJavaResolver() throws UnknownHostException {
		final DnsJavaResolver resolver = new DnsJavaResolver();
		assertArrayEquals(new byte[] { 0x7F, 0, 0, 1 }, resolver.resolve("127.0.0.1")[0].getAddress());
		assertArrayEquals(new byte[] { (byte)0xC0, 0, 0, 1 }, resolver.resolve("192.0.0.1")[0].getAddress());
		// TODO: fix test with a simple but correct IPv6 address
		//assertArrayEquals(new byte[] { (byte)0x20, 0x1, 1, 1, (byte)0x80, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1 }, resolver.resolve("2001:0101:8000:0101:0000:0000:0000:0001")[0].getAddress());
	}

	@Test
	public void testFakeResolver() throws UnknownHostException {
		final FakeResolver resolver = new FakeResolver();
		assertArrayEquals(Ints.toByteArray("127.0.0.1".hashCode()), resolver.resolve("127.0.0.1")[0].getAddress());
		assertArrayEquals(Ints.toByteArray("192.0.0.1".hashCode()), resolver.resolve("192.0.0.1")[0].getAddress());
		assertArrayEquals(Ints.toByteArray("8000:0101:8000:0101:0000:0000:0000:0000".hashCode()), resolver.resolve("8000:0101:8000:0101:0000:0000:0000:0000")[0].getAddress());
	}
}
