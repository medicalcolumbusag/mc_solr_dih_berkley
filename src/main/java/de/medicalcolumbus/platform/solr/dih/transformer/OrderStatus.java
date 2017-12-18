package de.medicalcolumbus.platform.solr.dih.transformer;

public class OrderStatus extends Translation {

	private static final String ORDER_STATUS_TXT = "order_status_";

	@Override
	protected String getTranslation() {
		return ORDER_STATUS_TXT;
	}

}
