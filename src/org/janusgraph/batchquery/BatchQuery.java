package org.janusgraph.batchquery;

import org.janusgraph.batchquery.dataquery.DataQuery;

public class BatchQuery {

	public static void main(String args[]) throws Exception {
		if (null == args || args.length < 2) {
			System.err.println("Usage: BatchQuery <server_url> <query_file>");
			System.exit(1);
		}

		String endpoint = args[0];
		String queryFile = args[1];

		new DataQuery(endpoint).query(queryFile);
	}
}
