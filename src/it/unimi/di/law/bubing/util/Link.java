package it.unimi.di.law.bubing.util;

/*
 * Copyright (C) 2017 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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

import java.net.URI;

import it.unimi.di.law.bubing.StartupConfiguration;

// RELEASE-STATUS: DIST

/** A class representing a link, to be used by {@linkplain StartupConfiguration#scheduleFilter schedule filters}. */
public final class Link {
	public final URI source;
	public final URI target;

	/** Creates a new link with given source and target.
	 *
	 * @param source the source {@link URI}.
	 * @param target the target {@link URI}.
	 */
	public Link(final URI source, final URI target) {
		this.source = source;
		this.target = target;
	}
}
