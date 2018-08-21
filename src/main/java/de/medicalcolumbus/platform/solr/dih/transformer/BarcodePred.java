package de.medicalcolumbus.platform.solr.dih.transformer;

public class BarcodePred extends Barcode{

	private static final String BARCODES_FIELD = "barcodes_pred";

	@Override
	protected String getBarcodeFieldName() {
		return BARCODES_FIELD;
	}
}
