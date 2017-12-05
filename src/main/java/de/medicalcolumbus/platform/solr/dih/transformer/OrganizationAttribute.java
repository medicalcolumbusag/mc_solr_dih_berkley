package de.medicalcolumbus.platform.solr.dih.transformer;

import java.util.Map;

public class OrganizationAttribute {

	public Object transformRow(Map<String, Object> row) {
		String type = row.get("KIND") == null ? "" : row.get("KIND").toString();
		String name = row.get("NAME").toString() + getFieldExtensionByType(type.toLowerCase());
		row.put(name, row.get("VALUE"));

		if ("customer_type_ss".equals(name)) {
			row.put("_intrinsic_customer_type_ss", row.get("VALUE"));
		}
		return row;
	}

	private String getFieldExtensionByType(String type) {
		switch (type) {
			case "integer":
				return "_ti";
			case "text":
				return "_ss";
			case "decimal":
				return "_td";
			case "boolean":
				return "_b";
			default:
				return "";
		}
	}
}
