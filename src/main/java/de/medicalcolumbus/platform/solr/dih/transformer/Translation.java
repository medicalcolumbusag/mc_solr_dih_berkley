package de.medicalcolumbus.platform.solr.dih.transformer;

import java.util.Map;

public abstract class Translation {

	private static final String LANGUAGE = "LANGUAGE";
	private static final String TRANSLATION = "TRANSLATION";
	private static final String STRING_TYPE = "_s";

	public Object transformRow(Map<String, Object> row) {
		String language = row.get(LANGUAGE).toString().trim();
		row.put(getTranslation() + language + STRING_TYPE, row.get(TRANSLATION).toString());

		return row;
	}

	protected abstract String getTranslation();
}
