package de.medicalcolumbus.platform.solr.dih.transformer;

public class BarcodeSuc extends Barcode {

	private static final String BARCODES_FIELD = "barcodes_suc";

	@Override
	protected String getBarcodeFieldName() {
		return BARCODES_FIELD;
	}
}
