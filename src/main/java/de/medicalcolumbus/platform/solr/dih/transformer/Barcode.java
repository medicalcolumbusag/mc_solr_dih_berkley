package de.medicalcolumbus.platform.solr.dih.transformer;


import java.util.Map;

public class Barcode {

	private static final String BARCODES_FIELD = "barcodes";
	private static final String BARCODE_VALUE = "barcode";

	public Object transformRow(Map<String, Object> row) {
		row.put(BARCODES_FIELD + "_ss", row.get(BARCODE_VALUE));
		return row;
	}
}