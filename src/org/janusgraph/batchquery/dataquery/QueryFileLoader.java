package org.janusgraph.batchquery.dataquery;

import java.lang.reflect.Constructor;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import org.apache.log4j.Logger;
import org.janusgraph.batchquery.util.Config;
import org.janusgraph.batchquery.util.Constants;
import org.janusgraph.batchquery.util.Worker;
import org.janusgraph.batchquery.util.WorkerPool;

public class QueryFileLoader {
	private String endpoint;
	private Class<Worker> workerClass;

	private Logger log = Logger.getLogger(QueryFileLoader.class);

	public QueryFileLoader(String endpoint, Class<Worker> workerClass) {
		this.endpoint = endpoint;
		this.workerClass = workerClass;
	}

	private void startWorkers(Iterator<String> iter, long targetRecordCount, WorkerPool workers)
			throws Exception {
		while (iter.hasNext()) {
			long currentRecord = 0;
			List<String> sub = new ArrayList<String>(); 
			while (iter.hasNext() && currentRecord < targetRecordCount) {
				sub.add(iter.next());
				currentRecord++;
			}
			Constructor<Worker> constructor = workerClass.getConstructor(Collection.class, String.class);
			Worker worker = constructor.newInstance(sub, endpoint);
			workers.submit(worker);
		}
	}

	public void startQuery(String queryFile, WorkerPool workers) throws Exception {
		log.info("Loading " + queryFile);

		try (Stream<String> stream = Files.lines(Paths.get(queryFile))) {
//			long freeMemory = Runtime.getRuntime().freeMemory()/1024/1024;
			// TODO Calculate targetThreadCount using the free memory and number of threads to execute
			//Max record count per thread
			startWorkers(stream.iterator(), Config.getConfig().getWorkersTargetRecordCount(), workers);
		}
	}
}
