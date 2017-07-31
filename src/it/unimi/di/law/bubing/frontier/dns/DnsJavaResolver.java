package it.unimi.di.law.bubing.frontier.dns;

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
