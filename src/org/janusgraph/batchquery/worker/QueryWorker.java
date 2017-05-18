package org.janusgraph.batchquery.worker;

import java.util.Collection;
import java.util.UUID;
import java.util.function.Consumer;

import org.apache.log4j.Logger;
import org.janusgraph.batchquery.util.Worker;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class QueryWorker extends Worker {
	private final UUID myID = UUID.randomUUID();

	private WebResource webResource;
	private long currentRecord;

	private Logger log = Logger.getLogger(QueryWorker.class);

	public QueryWorker(final Collection<String> records, final String endpoint) {
		super(records, endpoint);

		this.currentRecord = 0;
	}
	
	private String convertRecord(String record) {
		return "{\"gremlin\":\"".concat(record).concat("\"}");
	}

	private void acceptRecord(String record) throws Exception {
		record=convertRecord(record);
		ClientResponse response = webResource.accept("*/*").type("application/json").post(ClientResponse.class, record);
		response.getEntity(String.class);

		if (response.getStatus() != 200 && response.getStatus() != 201) {
			throw new Exception("Failed to execute query. HTTP error code is " + response.getStatus());
		}
		currentRecord++;
	}

	public UUID getMyID() {
		return myID;
	}

	@Override
	public void run() {
		log.info("Starting new thread " + myID + " with " + getRecords().size() + " records.");
		// Start new graph transaction
		Client client = Client.create();
		webResource = client.resource(getEndpoint());

		getRecords().iterator().forEachRemaining(new Consumer<String>() {
			@Override
			public void accept(String record) {
				try {
					acceptRecord(record);
				} catch (Exception e) {
					log.error("Thread " + myID + ". Exception during query.", e);
				}
			}

		});
//		client.destroy();

		log.info("Thread " + myID + " finished executing " + currentRecord + " queries!");
	}

}
