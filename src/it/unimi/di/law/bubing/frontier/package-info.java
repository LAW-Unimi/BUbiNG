//RELEASE-STATUS: DIST

/**
	A set of classes orchestrating the movement of URLs to be fetched next by a BUbiNG agent.

	<p>To start understanding how the frontier works, we should consider three levels of aggregation
	of the jobs that a BUbiNG agent is to perform:
		<ul>
			<li>at the ground level, we have URLs, as they come out of the {@linkplain it.unimi.di.law.bubing.sieve.AbstractSieve sieve}: these
			are known unvisited URLs that should eventually be considered;
			<li>URLs are grouped by their scheme+authority part, to form a {@link it.unimi.di.law.bubing.frontier.VisitState}:
			the visit state collects the statistics related to how the visit of that scheme+authority
			is going on;
			<li>{@link it.unimi.di.law.bubing.frontier.VisitState}s are grouped by IP, forming a {@link it.unimi.di.law.bubing.frontier.WorkbenchEntry}:
			the reason behind this is that we do not want to have more threads fetching pages
			from the same IP at the same time, even if the IP is accessed with different
			host names.
		</ul>

	<p>We now describe how these structures are created and move around, inviting the reader to consult
	the documentation of the approprate classes to get more information.

	<p><b>How visit states are born.</b>
	URLs come out of the {@linkplain it.unimi.di.law.bubing.sieve.AbstractSieve sieve} and are accumulated in the {@link it.unimi.di.law.bubing.frontier.Frontier#readyURLs} queue.
	URLs from the latter queue are read (by a process called distributor) and taken care of, one at a time.
	Every time a URL is considered, the distributor must know if the URL belongs to an already-known
	scheme+authority or not.
	This is done using the {@link it.unimi.di.law.bubing.frontier.Distributor#schemeAuthority2VisitState} map that contains (as values) all
	the visit states ever created.
	After a visit state is created, and in all moments of its life, new URL may arrive for that scheme+authority
	and are enqueued to the visit state (or, possibly, to an on-disk virtualization of its tail, if the visit
	state becomes too large; the virtualization aspects are not described in this document, but we invite
	the reader to consult the documentation of the distributor to know more about this).

	<p><b>When a new visit state is created.</b>
	If necessary, a new visit state is created, and it is added to a special queue of {@link it.unimi.di.law.bubing.frontier.Frontier#newVisitStates}
	for a DNS thread to eventually solve its hostname into an IP address (which is a necessary step to
	be able to aggregate the visit state to an appropriate workbench entry).
	Note that when the hostname has been resolved, the new visit state is either put in the appropriate
	workbench entry, or a new one is created for it.

	<p><b>Acquisition of visit states.</b>
	A visit state can be acquired only when its workbench entry is not acquired (i.e., no one is trying to
	fetch pages from any of the visit states contained in it), and in that case it is assumed to be nonempty
	(empty visit states are temporarily removed from their workbench entry until a new URL is put in them again).
	A further condition should be considered for a visit state to be acquired, which is related to politeness:
	enough time must have elapsed since the last fetch of a page for the same visit state (i.e., for the same
	scheme+authority) and moreover enough time must have elapsed since the last fetch of a page for the same
	workbench entry (i.e., for the same IP).
	When both conditions are satisfied, the visit state is moved onto a queue called {@link it.unimi.di.law.bubing.frontier.Frontier#todo} (by
	a special thread called {@link it.unimi.di.law.bubing.frontier.TodoThread}). At that point both the visit state and the workbench
	entry it belongs to are declared as being acquired.

	<p><b>Fetching threads.</b>
	When a fetching thread is free, it will try to get a visit state from the {@link it.unimi.di.law.bubing.frontier.Frontier#todo} queue.
	After this, it will fetch one or more URLs from the host. Every time a page has been fetched, it is
	put in the {@link it.unimi.di.law.bubing.frontier.Frontier#results} queue so that it can be parsed by a parsing thread. Only when this
	happens, the fetching thread can proceed to fetch another URL (because every fetching thread has a buffer
	to store the page, that is reused every time). When the fetching thread is done with fetching,
	it will put back the visit state in the {@link it.unimi.di.law.bubing.frontier.Frontier#done} queue, so that it is then released to the workbench.
*/
package it.unimi.di.law.bubing.frontier;
