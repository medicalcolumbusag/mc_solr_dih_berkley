package de.medicalcolumbus.platform.solr.dih.transformer;

import java.util.Map;

public class Attribute {
	public Object transformRow(Map<String, Object> row) {
		String unit = (row.get("ATTRIBUTE_UNIT") != null) ? " " + row.get("ATTRIBUTE_UNIT") : "";
		row.put(row.get("ATTRIBUTE_NAME") + "_ss", row.get("ATTRIBUTE_VALUE") + unit);
		return row;
	}
}
