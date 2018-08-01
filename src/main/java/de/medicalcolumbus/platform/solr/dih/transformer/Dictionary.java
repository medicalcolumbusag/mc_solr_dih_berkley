package de.medicalcolumbus.platform.solr.dih.transformer;

import java.util.Map;

public abstract class Dictionary {

	static final String LANGUAGE = "LANGUAGE";
	static final String VALUE = "VALUE";
	static final String STRING_TYPE = "_s";

	public Object transformRow(Map<String, Object> row) {
		String language = row.get(LANGUAGE).toString().trim();
		row.put(getTranslation() + language + STRING_TYPE, row.get(VALUE).toString());

		return row;
	}

	protected abstract String getTranslation();
}
