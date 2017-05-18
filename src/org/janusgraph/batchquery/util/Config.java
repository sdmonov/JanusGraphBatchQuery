package org.janusgraph.batchquery.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Config {
	private Properties properties = null;
	private String availProcessors = new Integer(Runtime.getRuntime().availableProcessors()).toString();

	private static Config config = null;
	private static String propFile = "batch_query.properties";

	public static void setConfigFile(String propFile) {
		Config.propFile = propFile;
	}

	public static Config getConfig() throws Exception {
		if (config == null) {
			config = new Config();
			config.loadProperties(propFile);
		}

		return config;
	}

	public Config() {
		properties = new Properties();
	}

	public void loadProperties(String propFile) throws FileNotFoundException, IOException {
		InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFile);
		if (inputStream != null) {
			properties.load(inputStream);
		} else {
			properties = null;
		}
	}

	public Properties getProperties() {
		return properties;
	}

	public int getWorkersTargetRecordCount() {
		return properties == null ? null
				: new Integer(properties.getProperty("workers.target_record_count",
						Constants.DEFAULT_WORKERS_TARGET_RECORD_COUNT.toString()));
	}

	public int getWorkers() {
		return properties == null ? null : new Integer(properties.getProperty("workers", availProcessors));
	}
}