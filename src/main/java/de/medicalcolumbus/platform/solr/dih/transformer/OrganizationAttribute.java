package de.medicalcolumbus.platform.solr.dih.transformer;

import java.util.Map;

public class OrganizationAttribute {

	public Object transformRow(Map<String, Object> row) {
		String type = row.get("INDEXING_TYPE").toString().trim();
		String name = row.get("ATTRIBUTE_CODE").toString().trim() + type;
		row.put(name, row.get("ATTRIBUTE_VALUE").toString());

		return row;
	}
}
