package de.medicalcolumbus.platform.solr.dih;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class EhCacheEntry implements Serializable {

	private List<Map<String, Object>> values;

	public EhCacheEntry(List<Map<String, Object>> values) {
		this.values = values;
	}

	public List<Map<String, Object>> getValues() {
		return values;
	}
}
