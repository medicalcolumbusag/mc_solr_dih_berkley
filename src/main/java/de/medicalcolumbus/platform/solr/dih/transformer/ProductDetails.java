package de.medicalcolumbus.platform.solr.dih.transformer;

import java.util.Map;

public class ProductDetails {

	public Object transformRow(Map<String, Object> row) {
		String locale = row.get("locale").toString();

		addLocalizedField(row, locale, "details_name", "name");
		addLocalizedField(row, locale, "details_original_name", "original_name");
		addLocalizedField(row, locale, "details_short_name", "short_name");
		addLocalizedField(row, locale, "details_original_short_name", "original_short_name");
		addLocalizedField(row, locale, "details_description", "description");
		addLocalizedField(row, locale, "details_original_description", "original_description");
		return row;
	}

	private void addLocalizedField(Map<String, Object> row, String locale, String fieldName, String columnName) {
		if (row.get(columnName) != null) {
			row.put(fieldName + "_mwt", locale + "###" + row.get(columnName));
		}
	}
}
