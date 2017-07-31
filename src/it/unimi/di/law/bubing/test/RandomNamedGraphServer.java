package it.unimi.di.law.bubing.test;

/*
 * Copyright (C) 2012-2013 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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

import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.util.XorShift1024StarRandomGenerator;

import org.apache.commons.lang.StringUtils;

//RELEASE-STATUS: DIST

/** A {@link NamedGraphServer} exposing a random graph.
 *
 * <p>The graph is generated using the following criteria:
 * <ul>
 * <li>the graph has the specified number of sites;
 * <li>each site has the specified depth;
 * <li>all pages are named <code>index.html</code>;
 * <li>the degree of a page is chosen with Gaussian distribution centred around the specified degree;
 * <li>about one tenth of the links are external (i.e., point at another site);
 * <li>of the remaining links, about one fourth point at the parent;
 * <li>in general, one legal page every one hundred returns {@code null}.
 * </ul>
 *
 * <p>The name of a page is <code>http://<var>site</var>/<var>n</var><sub>0</sub>/<var>n</var><sub>1</sub>/&hellip;/index.html</code>,
 * where every component is a non-negative integer.
 */
public class RandomNamedGraphServer implements NamedGraphServer {
	public final static CharSequence[] EMPTY_CHARSEQUENCE_ARRAY = {};

	/** The degree of each node. */
	private final int degree;
	/** The maximum depth of a site. */
	private final int maxDepth;
	/** Multiplicative factor for the uniform [0..1] deviate. */
	private final double factor;
	/** Correction factor for the exponentiated deviate. */
	private final long correction;
	/** PaddingOption */
	private final boolean padding;

	/** Builds the server.
	 *
	 * @param sites the number of sites in the graph.
	 * @param degree the degree of each node.
	 * @param maxDepth the maximum depth of a site.
	 */
	public RandomNamedGraphServer(final int sites, final int degree, final int maxDepth) {
		this(sites, degree, maxDepth, false);
	}

	public RandomNamedGraphServer(final int sites, final int degree, final int maxDepth, final boolean padding) {
		this.degree = degree;
		this.maxDepth = maxDepth;
		this.factor = 3 * Math.log(sites);
		this.correction = (long)sites * sites;
		this.padding = padding;
	}

	private String paddedByte(final int b, final XorShift1024StarRandomGenerator random) {
		return padding ? "" + b  : StringUtils.repeat("0", random.nextInt(2)) + b;
	}

	private String host(final XorShift1024StarRandomGenerator random) {
		final int site = Math.min(Integer.MAX_VALUE - 1, (int)Math.floor(Math.exp(random.nextDouble() * factor) / correction)) + 1;
		// Scheme and host. This mess is used to generate valid IP addresses that won't make URI throw an exception for each URI.
		return new StringBuilder().append(site >>> 24 & 0xFF).append('.').append(site >>> 16 & 0xFF).append('.').append(paddedByte(site >>> 8 & 0xFF, random)).append('.').append(paddedByte(site & 0xFF, random)).toString();
	}

	@Override
	public CharSequence[] successors(final CharSequence name) {
		final MutableString s = new MutableString(name);
		// Find host
		final int startHost = s.indexOf('/', s.indexOf('/') + 1) + 1;
		int endHost = s.indexOf('/', startHost);
		if (endHost == -1) endHost = s.length();
		final MutableString host = s.substring(startHost, endHost);

		// The actual depth and base degree are distributed lognormally by host.
		final XorShift1024StarRandomGenerator random = new XorShift1024StarRandomGenerator(host.hashCode());
		final int maxDepth = Math.min(this.maxDepth * 3, (int)(Math.floor(Math.exp(random.nextGaussian() / 2 + (Math.log(this.maxDepth) - 1. / 8)))));
		final int meanDegree = Math.min(this.degree * 3, (int)(Math.floor(Math.exp(random.nextGaussian() / 2 + (Math.log(this.degree) - 1. / 8)))));

		if (s.length() > 0 && s.charAt(s.length() -1) == '/') s.append("index.html");

		if (s.substring(s.lastIndexOf('/') + 1).equals("robots.txt")) return EMPTY_CHARSEQUENCE_ARRAY;

		// Now we generate random numbers using the *page* as seed.
		random.setSeed(s.hashCode());
		final char[] a = s.array();
		int d = -3;
		for(int i = s.length(); i-- != 0;) if (a[i] == '/') d++;
		if (d > maxDepth) return null; // 404.
		if (d == maxDepth) return EMPTY_CHARSEQUENCE_ARRAY; // No outlinks at maximum depth.

		final int degree = Math.max(0, (int)(meanDegree + random.nextGaussian()));
		final CharSequence[] result = new CharSequence[degree];
		final MutableString u = new MutableString();

		for(int i = 0; i < degree; i++) {
			if (random.nextDouble() < 1./10) {
				// External
				int depth = (int)(maxDepth * (Math.min(1, Math.abs(random.nextGaussian()))));
				u.length(0);
				u.append("http://").append(host(random)).append('/');

				while(depth-- != 0) u.append(random.nextInt(degree)).append('/'); // path
			}
			else {
				// Internal
				u.replace(s);
				u.length(u.lastIndexOf('/') + 1);

				if (d > 0 && random.nextDouble() < 1./2) {
					// shallower
					u.length(u.lastIndexOf('/', u.length() - 2) + 1);
				}
				else {
					// deeper
					u.append(random.nextInt(degree)).append('/'); // increase path
				}
			}
			u.append("index.html"); // final element
			result[i] = u.copy();
		}

		return result;
	}

	@Override
	public RandomNamedGraphServer copy() {
		return this;
	}
}
