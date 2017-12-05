package de.medicalcolumbus.platform.solr.dih.transformer;

import java.util.Map;

public class Details {

	public Object transformRow(Map<String, Object> row) {
		String locale = row.get("LOCALE").toString();

		addLocalizedField(row, locale, "details_name", "NAME");
		addLocalizedField(row, locale, "details_original_name", "ORIGINAL_NAME");
		addLocalizedField(row, locale, "details_short_name", "SHORT_NAME");
		addLocalizedField(row, locale, "details_original_short_name", "ORIGINAL_SHORT_NAME");
		addLocalizedField(row, locale, "details_description", "DESCRIPTION");
		addLocalizedField(row, locale, "details_original_description", "ORIGINAL_DESCRIPTION");
		return row;
	}

	private void addLocalizedField(Map<String, Object> row, String locale, String fieldName, String columnName) {
		if (row.get(columnName) != null) {
			row.put(fieldName + "_mwt", locale + "###" + row.get(columnName));
		}
	}
}
