package org.janusgraph.batchquery.util;

import java.util.Collection;

public abstract class Worker implements Runnable {
	private final Collection<String> records;
	private final String endpoint;

	public Worker(final Collection<String> records, final String endpoint) {
		this.records = records;
		this.endpoint = endpoint;
	}

	public Collection<String> getRecords() {
		return records;
	}

	public String getEndpoint() {
		return endpoint;
	}

}
