package de.medicalcolumbus.platform.solr.dih.transformer;


import java.util.Map;

public class Market {

	private static final String MARKETS_FIELD = "markets";
	private static final String MARKET_VALUE = "MARKET";

	public Object transformRow(Map<String, Object> row) {
		row.put(MARKETS_FIELD + "_ss", row.get(MARKET_VALUE));
		return row;
	}
}