// RELEASE-STATUS: DIST

/**
 	A comprehensive filtering system.

	<p>A filter
	is a strategy to decide whether a certain object should be accepted or not; the type of objects
	a filter considers is called the <em>base type</em> of the filter. The base type
	is usually a {@link java.net.URI}, a {@link it.unimi.di.law.warc.filters.URIResponse} or a {@link it.unimi.dsi.law.bubing.util.Link}.
	The type depends on the phase in which the filter will be applied.</p>

	<p>Various kinds of filters are available, and moreover they can be composed with boolean operators
	using the static methods specified in the {@link it.unimi.di.law.warc.filters.Filters Filters} class. Additionally, a filter parser is
	provided in the <tt>it.unimi.dsi.law.ubi.filters.parser</tt> package; since the parser itself is written
	using <a href="https://javacc.dev.java.net/">JavaCC</a>, we provide a description of it here.</p>

	<p>Two filters are called <em>homogeneous</em> if they filter the same kind of objects, <em>heterogeneous</em>
	otherwise.</p>

	<p>A filter parser is instantiated on the basis of the kind of filters it will actually return; more precisely a
	<code>FilterParser&lt;T&gt;</code> is a filter parser that will return a <code>Filter&lt;T&gt;</code>; for technical
	reasons, the class <code>T</code> must be provided as unique parameter when the parser is constructed. A parser
	can be used many times. Every time a filter is sought, the <code>parse(String x)</code> method of the parser
	is called, which returns a filter of the correct kind, or throws a parse exception.</p>

	<p>The syntax used by the filter parser is <a href="parser/FilterParser.doc.html">available</a>. Basically,
	it is a propositional calculus, with and (denoted by infix <tt>and</tt> or <tt>&amp;</tt>), or (denoted by infix <tt>or</tt> or <tt>|</tt>)
	and not (denoted by prefix <tt>not</tt> or <tt>!</tt>), whose ground terms have the same form as returned
	by the <tt>toString()</tt> method of the <tt>Filter</tt> class.</p>

	<p>Here are some examples:</p>

	<ul>
		<li><code>HostEquals(www.foo.bar)</code>
		<li><code>(HostEndsWith(foo.bar) and not ForbiddenHost(http://xxx.yyy.zzz/list-of-forbidden-hosts)) or NoMoreSlashThan(10)</code>
	</ul>

	<p>Usually, an expression should only contain references to homogeneous filters of type <code>T</code>, where
	<code>T</code> is the type used to instantiate the parser. Nonetheless, if some ground term refers to a
	filter of some other type <code>D</code>, the parser will try to find a static method in the <code>Filters</code> class
	having the following signature:</p>

	<pre>
	public static Filter&lt;T&gt; adaptFilterD2T(Filter&lt;D&gt; f)
	</pre>

	<p>that adapts the given filter <code>f</code> to a filter of the correct type. If this method is missing,
	the parser will itself throw an exception. This is how a {@link java.net.URI} filter can be applied
	to a {@link it.unimi.di.law.warc.filters.URIResponse} instance (by extractying the associated
	{@link java.net.URI}), or to a {@link it.unimi.di.law.bubing.util.Link} instance
	(applying the filter to its {@linkplain it.unimi.di.law.bubing.util.Link#target target}).</p>

*/
package it.unimi.di.law.warc.filters;
