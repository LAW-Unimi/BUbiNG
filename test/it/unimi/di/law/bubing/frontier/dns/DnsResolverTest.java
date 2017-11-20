package it.unimi.di.law.bubing.frontier.dns;

/*
 * Copyright (C) 2004-2017 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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
