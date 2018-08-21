package de.medicalcolumbus.platform.solr.dih.transformer;


import java.util.Map;

public class Barcode {

	private static final String BARCODES_FIELD = "barcodes";
	private static final String BARCODE_VALUE = "BARCODE";

	public Object transformRow(Map<String, Object> row) {
		row.put(getBarcodeFieldName() + "_ss", row.get(BARCODE_VALUE));
		return row;
	}

	protected String getBarcodeFieldName(){
		return BARCODES_FIELD;
	}
}