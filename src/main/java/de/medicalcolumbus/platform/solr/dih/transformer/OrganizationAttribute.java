package de.medicalcolumbus.platform.solr.dih.transformer;

import java.util.Map;

public class OrganizationAttribute {

	public Object transformRow(Map<String, Object> row) {
		String type = row.get("indexing_type").toString().trim();
		String name = row.get("attribute_code").toString().trim() + type;
		row.put(name, row.get("attribute_value").toString().trim());

		if ("customer_type_ss".equals(name)) {
			row.put("_intrinsic_customer_type_ss", row.get("attribute_value").toString().trim());
		}
		return row;
	}
}
