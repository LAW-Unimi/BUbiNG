package it.unimi.di.law.bubing.spam;

import it.unimi.dsi.lang.FlyweightPrototype;

//RELEASE-STATUS: DIST

/** A detector for spam sites.
 *
 * <p>An instance of this class accept an object representing features of a web page or set of web pages,
 * and returns an estimate for the spammicity.
 */

public interface SpamDetector<T> extends FlyweightPrototype<SpamDetector<T>>, java.io.Serializable {
	/** Estimates the spam score associated with a given information object.
	 *
	 * @param t an object specify the spam-detection data.
	 * @return An estimate of the spammicity associated with {@code t} (a number between 0 and 1, where 1 means surely spam).
	 */
	public double estimate(T t);
}
