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

import it.unimi.di.law.bubing.frontier.Frontier;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.http.conn.DnsResolver;

//RELEASE-STATUS: DIST

/** A fake resolver that returns a four-byte representation of the host {@linkplain Object#hashCode() hashcode}. */

public final class FakeResolver implements DnsResolver {
	@Override
	public InetAddress[] resolve(final String hostname) throws UnknownHostException {
		if ("localhost".equals(hostname)) return Frontier.LOOPBACK;
		return new InetAddress[] { InetAddress.getByAddress(com.google.common.primitives.Ints.toByteArray(hostname.hashCode())) };
	}
}
