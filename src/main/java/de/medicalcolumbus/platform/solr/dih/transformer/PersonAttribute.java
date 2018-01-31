package de.medicalcolumbus.platform.solr.dih.transformer;

import java.util.Map;

public class PersonAttribute {

	public Object transformRow(Map<String, Object> row) {
		String type = row.get("KIND") == null ? "" : row.get("KIND").toString();
		String name = row.get("ATTRIBUTE_NAME").toString() + getFieldExtensionByType(type.toLowerCase());
		row.put(name, row.get("VALUE"));

		return row;
	}

	private String getFieldExtensionByType(String type) {
		switch (type) {
			case "text":
				return "_ss";
			case "date":
				return "_dt";
			case "boolean":
				return "_b";
			default:
				return "";
		}
	}
}
