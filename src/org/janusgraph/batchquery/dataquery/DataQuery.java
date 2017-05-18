package org.janusgraph.batchquery.dataquery;

import org.apache.log4j.Logger;
import org.janusgraph.batchquery.util.Config;
import org.janusgraph.batchquery.util.Worker;
import org.janusgraph.batchquery.util.WorkerPool;
import org.janusgraph.batchquery.worker.QueryWorker;

public class DataQuery {
	private String endpoint;

	private Logger log = Logger.getLogger(DataQuery.class);

	public DataQuery(String endpoint) {
		this.endpoint = endpoint;
	}

	public void query(String queryFile) throws Exception {
		query(queryFile, (Class) QueryWorker.class);
	}

	public void query(String queryFile, Class<Worker> workerClass) throws Exception {
		log.info("Start loading data for " + queryFile);
		long startTime = System.nanoTime();

		int availWorkers = Config.getConfig().getWorkers();
		try (WorkerPool workers = new WorkerPool(availWorkers)) {
			new QueryFileLoader(endpoint, workerClass).startQuery(queryFile, workers);
		}

		// log elapsed time in seconds
		long totalTime = (System.nanoTime() - startTime) / 1000000000;
		log.info("Loaded " + queryFile + " in " + totalTime + " seconds!");
	}
}
