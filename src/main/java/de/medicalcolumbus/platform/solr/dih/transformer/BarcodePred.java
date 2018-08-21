package de.medicalcolumbus.platform.solr.dih.transformer;

import org.apache.solr.handler.dataimport.Context;
import org.apache.solr.handler.dataimport.Transformer;

import java.util.Map;

public class BarcodePred extends Transformer {
	private static final String BARCODES_FIELD = "barcodes_pred";
	private static final String BARCODE_VALUE = "BARCODE";

	@Override
	public Object transformRow(Map<String, Object> row, Context context) {

		row.put(BARCODES_FIELD + "_ss", row.get(BARCODE_VALUE));
		return row;
	}
}
