package it.unimi.di.law;

/*
 * Copyright (C) 2008-2017 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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


import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import it.unimi.dsi.fastutil.io.MeasurableInputStream;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;

import org.apache.commons.io.IOUtils;

import org.junit.Assert;

//RELEASE-STATUS: DIST

/** A static container of utility methods for test cases. */
public final class TestUtil {

    /** The property specifying the data directory. */
    public static final String DATA_DIR = "it.unimi.di.law.data";
	/** A threshold for equality testing. */
	public static final double EQUALS_THRESHOLD = 1E-12;

    /** Cannot be instantiated. */
    private TestUtil() {}

    /** Returns a derelativised version of the given filename.
     *
     * <P>The general data directory must be specified in the
     * property <code>it.unimi.di.law.data</code>. The directory
     * must contain a <code>test</code> subdirectory (in which
     * <code>name</code> will be searched for).
     *
     * <P>If <code>name</code> starts with a slash, it will
     * be derelativised w.r.t. the directory <code>test</code>
     * in <code>it.unimi.di.law.data</code>. Otherwise,
     * the package name of the provided class (with dots replaced by directory separator)
     * will be used additionally. If <code>exactMatch</code> is false, the
     * resulting filename is returned independently of whether the file exists or not.
     * If <code>exactMatch</code> is true, and the file is not found,
     * then another attempt is done to locate it one level above etc., until it is found
     * or until there are no more levels;
     *
     * <P>This class will directly {@link org.junit.Assert#fail()}
     * the current test if the property is not defined, or if the path that
     * is supposed to contain the file does not exist or does not correspond
     * to a directory.
     * It will return {@code null} if <code>it.unimi.di.law.data</code>
     * is the empty string. Thus, you should start your tests as follows:
     *
     * <pre>
     * String filename = TestUtil.getTestFile(...);
     * if (filename == null) return;
     * </pre>
     *
     * @param klass the class performing the derelativisation.
     * @param name a filename (to be found in the test data directory).
     * @param exactMatch if true, the file must exist; if it does not exist, the
     *   hierarchy is scanned up to look for the file, as explained above.
     * @return the derelativised filename, or {@code null}
     * if <code>it.unimi.di.law.data</code> is not set.
     */
    public static String getTestFile(final Class<?> klass, final String name, boolean exactMatch) {

        final String dataDirName = System.getProperty(DATA_DIR);

        if (dataDirName == null) Assert.fail(DATA_DIR + " is not defined");
        else if (dataDirName.length() == 0) return null;

        File testDir = new File(dataDirName);
        File result;

        if (name.charAt(0) != '/') {
            final String[] piece = klass.getName().split("\\.");
            int numberOfPieces = piece.length - 1;

            File actualDir;
            String firstAttempt = null;
            do {
            	actualDir = testDir;
            	// Note that we skip "test".
            	for(int i = 0; i < numberOfPieces; i++) actualDir = new File(actualDir, piece[i]);
            	result = new File(actualDir, name);
            	if (!exactMatch) return result.toString();
            	if (firstAttempt == null) firstAttempt = result.toString();
            	if (! result.exists() && numberOfPieces > 0) numberOfPieces--;
            } while (! result.exists() && numberOfPieces > 0);
            if (! result.exists()) Assert.fail(firstAttempt + " does not exist (not even in the rest of the hierarchy up to " + actualDir + ")");
        } else
        	result = new File(testDir, name);
        return result.toString();

    }

    /** Returns a random vector of given size, using the provided {@link java.util.Random} object.
     *
     *  @param n the vector size.
     *  @param random the random number generator to be used (its {@link Random#nextDouble()} method will be called).
     */
    public static int[] randomIntVector(final int n, final Random random) {
    	int[] a = new int[n];
    	for (int i = 0; i < n; i++)
    			a[i] = random.nextInt();
    	return a;
    }

    /** Returns a random vector of given size and whose values are all in the range [<var>min</var>,<var>max</var>),
     *  using the provided {@link java.util.Random} object.
     *
     *  @param n the vector size.
     *  @param min the minimum of the range.
     *  @param max the maximum of the range.
     *  @param seed the seed to be used to create random numbers.
     */
    public static int[] randomIntVector(final int n, final int min, final int max, final int seed) {
    	class MyRandom extends Random {
			private static final long serialVersionUID = 1L;
			public MyRandom() {
    			super(seed);
    		}
    		@Override
			public int nextInt() {
    			return min + super.nextInt(max - min);
    		}
    	}
    	return randomIntVector(n, new MyRandom());
    }

    /** Returns a random vector of given size, using the provided {@link java.util.Random} object.
     *
     *  @param n the vector size.
     *  @param random the random number generator to be used (its {@link Random#nextDouble()} method will be called).
     */
    public static double[] randomDoubleVector(final int n, final Random random) {
    	double[] a = new double[n];
    	for (int i = 0; i < n; i++)
    			a[i] = random.nextDouble();
    	return a;
    }

    /** Returns a random vector of given size and whose values are all in the range [<var>min</var>,<var>max</var>),
     *  using the provided {@link java.util.Random} object.
     *
     *  @param n the vector size.
     *  @param min the minimum of the range.
     *  @param max the maximum of the range.
     *  @param seed the seed to be used to create random numbers.
     */
    public static double[] randomDoubleVector(final int n, final double min, final double max, final int seed) {
    	class MyRandom extends Random {
			private static final long serialVersionUID = 1L;
			public MyRandom() {
    			super(seed);
    		}
    		@Override
			public double nextDouble() {
    			return min + super.nextDouble() * (max - min);
    		}
    	}
    	return randomDoubleVector(n, new MyRandom());
    }

    /** Returns a random matrix with given size, using the provided {@link java.util.Random} object.
     *
     *  @param rows the number of rows (first index).
     *  @param columns the number of columns (second index).
     *  @param random the random number generator to be used (its {@link Random#nextDouble()} method will be called).
     */
    public static double[][] randomDoubleMatrix(int rows, int columns, Random random) {
    	double[][] a = new double[rows][columns];
    	for (int r = 0; r < rows; r++)
    		for (int c = 0; c < columns; c++)
    			a[r][c] = random.nextDouble();
    	return a;
    }

    /** Returns a random matrix with given size and whose values are all in the range [<var>min</var>,<var>max</var>).
     *
     *  @param rows the number of rows (first index).
     *  @param columns the number of columns (second index).
     *  @param min the minimum of the range.
     *  @param max the maximum of the range.
     *  @param seed the seed to be used to create random numbers.
     */
    public static double[][] randomDoubleMatrix(final int rows, final int columns, final double min, final double max, final int seed) {
    	class MyRandom extends Random {
		private static final long serialVersionUID = 1L;
		public MyRandom() {
    			super(seed);
    		}
    		@Override
		public double nextDouble() {
    			return min + super.nextDouble() * (max - min);
    		}
    	}
    	return randomDoubleMatrix(rows, columns, new MyRandom());
    }

	/** Returns the norm of the componentwise difference between two vectors.
	 *
	 * @param v0 the first vector.
	 * @param v1 the second vector.
	 * @return the norm.
	 */
	public static double normOfDifference(final double[] v0, final double[] v1) {
		if (v0.length != v1.length) throw new IllegalArgumentException();
		double s = 0.0;
		for (int i = v0.length - 1; i >= 0; i--) {
			double d = v0[i] - v1[i];
			s += d * d;
		}
		return Math.sqrt(s);
	}

	/**
	 * Duplicates a given input {@link MeasurableInputStream} both coping it to
	 * a given {@link OutputStream} and also returning it as a
	 * {@link FastByteArrayInputStream}.
	 *
	 * @param in the input stream.
	 * @param out where to copy.
	 * @return a byte array buffered copy of the input stream.
	 * @throws IOException
	 */
	public static MeasurableInputStream tee(MeasurableInputStream in, OutputStream out) throws IOException {
		FastByteArrayOutputStream tmp = new FastByteArrayOutputStream();
		IOUtils.copy(in, tmp);
		FastByteArrayInputStream copy = new FastByteArrayInputStream(tmp.array, 0, tmp.length);
		IOUtils.copy(copy, out);
		copy.position(0);
		return copy;
	}


}
