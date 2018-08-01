package de.medicalcolumbus.platform.solr.dih.transformer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * This transformer creates composed qualification names for every parent
 * and add all the parents in different facet values.
 * <p>
 * Example:
 * Product classified as:
 * <ul>
 * <li>Ward Supplies
 * <ul>
 * <li>Diagnostic appliances
 * <ul>
 * <li>Blood pressure measuring devices
 * <ul>
 * <li> / parts
 * <ul>
 * <li> / connecting pieces and tubes</li>
 * </ul>
 * </li>
 * </ul>
 * </li>
 * </ul>
 * </li>
 * </ul>
 * </li>
 * </ul>
 * Will result in the following Solr field:
 * <pre><code>
 * "classification_101_en": [
 * "1_Ward Supplies",
 * "2_Ward Supplies, Diagnostic appliances",
 * "3_Ward Supplies, Diagnostic appliances, Blood pressure measuring devices",
 * "4_Ward Supplies, Diagnostic appliances, Blood pressure measuring devices, / parts",
 * "5_Ward Supplies, Diagnostic appliances, Blood pressure measuring devices, / parts, / connecting pieces and tubes"
 * ]
 * </code></pre>
 */
public class Classification {

	static final String FULL_NAME = "FULL_NAME";
	static final String FULL_KEY = "FULL_KEY";
	static final String SYSTEM_ID = "SYSTEM_ID";
	static final String LOCALE = "LOCALE";

	public Object transformRow(Map<String, Object> row) {
		return addComposedKey(addComposedNames(row));
	}

	private Map<String, Object> addComposedNames(Map<String, Object> row) {
		// no full name or not a composed name
		if (row.get(FULL_NAME) == null) {
			return row;
		}

		String fullName = row.get(FULL_NAME).toString();
		if (!fullName.contains("_")) {
			return row;
		}
		String[] classifications = fullName.split("_", 2)[1].split(">");
		List<String> classificationValues = new ArrayList<>();
		int level = 1;

		for (String i : classifications) {
			StringBuilder rowValue = new StringBuilder().append(level++).append("_");
			for (int j = 0; j < level - 1; j++) {
				rowValue.append(classifications[j]).append(j < level - 2 ? "," : "");
			}
			classificationValues.add(rowValue.toString());
		}

		row.put("classification_" + row.get(SYSTEM_ID) + "_" + row.get(LOCALE), classificationValues);
		return row;
	}

	private Map<String, Object> addComposedKey(Map<String, Object> row) {
		row.put("composed_classification_key", row.get(FULL_KEY));
		return row;
	}
}
