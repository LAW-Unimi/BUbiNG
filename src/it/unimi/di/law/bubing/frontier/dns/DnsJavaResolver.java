package it.unimi.di.law.bubing.frontier.dns;

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

import it.unimi.di.law.bubing.RuntimeConfiguration;
import it.unimi.di.law.bubing.frontier.Frontier;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.http.conn.DnsResolver;
import org.xbill.DNS.Address;

//RELEASE-STATUS: DIST

/** A resolver based on <a href="http://www.xbill.org/dnsjava/">dnsjava</a>. */

public final class DnsJavaResolver implements DnsResolver {
	@Override
	public InetAddress[] resolve(String hostname) throws UnknownHostException {
		if ("localhost".equals(hostname)) return Frontier.LOOPBACK;
		// DnsJava does not understand dotted-notation IP addresses with additional zeroes (e.g., 127.0.0.01).
		if (RuntimeConfiguration.DOTTED_ADDRESS.matcher(hostname).matches()) return InetAddress.getAllByName(hostname);
		// This avoid expensive trials with domain suffixes (but must not be applied to dotted-notation IP addresses).
		hostname = hostname.endsWith(".") ? hostname : hostname + ".";
		return Address.getAllByName(hostname);
	}
}
