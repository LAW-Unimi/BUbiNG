//RELEASE-STATUS: DIST

/**
	The system of parsers used for analyzing HTTP responses.

	<p>A parser is an object that may be able to parse the content of a HTTP response (a.k.a. page), in order to
	extract links from it (provided that the format allows for links), to compute a suitable digest
	of the response (for duplicate detection) etc. Typically, a {@link it.unimi.di.law.bubing.frontier.ParsingThread} will try
	in turn with a sequence of parsers: each parser acts as a filter that can decide whether it is
	able to parse a given page or not (e.g., based on the <code>content-type</code> header).

	<p>The first parser that declares to be suitable for the given response will be used, if any.
	If no parser was available with this property, or if the parser used failed, a catch-all
	{@link it.unimi.di.law.bubing.parser.BinaryParser} will be used instead.

	<p>Parsers should be written so that they can be easily re-used, should be lightweight and
	should be very robust to errors in the parsed responses.
*/
package it.unimi.di.law.bubing.parser;
