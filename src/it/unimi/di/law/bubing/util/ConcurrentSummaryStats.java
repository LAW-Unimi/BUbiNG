package it.unimi.di.law.bubing.util;

import it.unimi.dsi.Util;
import it.unimi.dsi.fastutil.Size64;
import it.unimi.dsi.fastutil.doubles.DoubleList;

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

//RELEASE-STATUS: DIST

public class ConcurrentSummaryStats implements Size64 {
	/** Mean */
	private double a;
	/** A statistics used to compute the variance (see <a
	 * href="http://en.wikipedia.org/wiki/Standard_deviation#Rapid_calculation_methods">here</a>). */
	private double q;
	/** The minimum value in the stream. */
	private double min = Double.POSITIVE_INFINITY;
	/** The maximum value in the stream. */
	private double max = Double.NEGATIVE_INFINITY;
	/** The number of elements in the stream. */
	private long size;

	/** Adds a value to the stream.
	 *
	 * @param x the new value. */
	public synchronized void add(double x) {
		final double oldA = a;
		a += (x - a) / ++size;
		q += (x - a) * (x - oldA);
		min = Math.min(min, x);
		max = Math.max(max, x);
	}

	/** Adds values to the stream.
	 *
	 * @param a an array of new values. */
	public void addAll(double[] a) {
		for (double x : a)
			add(x);
	}

	/** Adds values to the stream.
	 *
	 * @param l a list of new values. */
	public void addAll(DoubleList l) {
		for (double x : l)
			add(x);
	}

	/** Returns the mean of the values added so far.
	 *
	 * @return the mean of the values added so far. */
	public double mean() {
		return a;
	}

	/** Returns the sum of the values added so far.
	 *
	 * @return the sum of the values added so far. */
	public double sum() {
		return a * size;
	}

	/** Returns the <em>sample</em> variance of the values added so far.
	 *
	 * @return the sample variance of the values added so far.
	 * @see #variance() */
	public double sampleVariance() {
		return q / (size - 1);
	}

	/** Returns the variance of the values added so far.
	 *
	 * @return the variance of the values added so far.
	 * @see #sampleVariance() */
	public double variance() {
		return q / size;
	}

	/** Returns the <em>sample</em> standard deviation of the values added so far.
	 *
	 * @return the sample standard deviation of the values added so far.
	 * @see #standardDeviation() */
	public double sampleStandardDeviation() {
		return Math.sqrt(sampleVariance());
	}

	/** Returns the standard deviation of the values added so far.
	 *
	 * @return the standard deviation of the values added so far.
	 * @see #sampleStandardDeviation() */
	public double standardDeviation() {
		return Math.sqrt(variance());
	}

	/** Returns the <em>sample</em> relative standard deviation of the values added so far.
	 *
	 * @return the sample relative standard deviation of the values added so far.
	 * @see #relativeStandardDeviation() */
	public double sampleRelativeStandardDeviation() {
		return Math.sqrt(sampleVariance()) / mean();
	}

	/** Returns the relative standard deviation of the values added so far.
	 *
	 * @return the relative standard deviation of the values added so far.
	 * @see #sampleRelativeStandardDeviation() */
	public double relativeStandardDeviation() {
		return Math.sqrt(variance()) / mean();
	}

	/** Returns the minimum of the values added so far.
	 *
	 * @return the minimum of the values added so far. */
	public double min() {
		return min;
	}

	/** Returns the maximum of the values added so far.
	 *
	 * @return the maximum of the values added so far. */
	public double max() {
		return max;
	}

	/** Returns the number of values added so far.
	 *
	 * @return the number of values added so far. */
	@Override
	public long size64() {
		return size;
	}

	@Override
	@Deprecated
	public int size() {
		throw new UnsupportedOperationException();
	}

	@Override
	public String toString() {
		return "[size: " + Util.format(size) + " min: " + min + " max: " + max + " \u03BC: " + mean() + " \u03C3: " + sampleStandardDeviation() + " ("
				+ Util.format(100 * sampleRelativeStandardDeviation()) + " %)]";
	}

	public static void main(String[] args) {
		final ConcurrentSummaryStats css = new ConcurrentSummaryStats();

		long startTime = System.currentTimeMillis();
		for (int i = 0; i < Integer.parseInt(args[0]); i++) {
			Thread tmp = new Thread() {
				@Override
				public void run() {
					while (true) {
						double rr = Math.random();
						css.add(rr);
					}
				};
			};
			tmp.start();
			if (i % 100 == 0) System.out.println("i: " + i + " " + css.toString());
		}
		System.out.println("Starting to sleep");
		try {
			Thread.sleep(100000);
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
		}
		System.out.println(css.toString());

		System.out.println(System.currentTimeMillis() - startTime);


	}
}
