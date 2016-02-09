package de.medicalcolumbus.platform.solr.dih.transformer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This transformer creates composed qualification names for every parent
 * and add all the parents in different facet values.
 *
 * Example:
 * Product classified as:
 * <ul>
 * <li>Ward Supplies
 *     <ul>
 *         <li>Diagnostic appliances
 *             <ul>
 *                 <li>Blood pressure measuring devices
 *                     <ul>
 *                         <li> / parts
 *                             <ul>
 *                                 <li> / connecting pieces and tubes</li>
 *                             </ul>
 *                         </li>
 *                     </ul>
 *                 </li>
 *             </ul>
 *         </li>
 *     </ul>
 * </li>
 * </ul>
 * Will result in the following Solr field:
 * <pre><code>
    "classification_101_en": [
	    "1_Ward Supplies",
	    "2_Ward Supplies, Diagnostic appliances",
	    "3_Ward Supplies, Diagnostic appliances, Blood pressure measuring devices",
	    "4_Ward Supplies, Diagnostic appliances, Blood pressure measuring devices, / parts",
	    "5_Ward Supplies, Diagnostic appliances, Blood pressure measuring devices, / parts, / connecting pieces and tubes"
    ]
 </code></pre>
 */
public class Classification {
	public Object transformRow(Map<String, Object> row) {
		if (row.get("FULL_NAME") == null) {
			return row;
		}
		String fullName = row.get("FULL_NAME").toString();
		if (!fullName.contains("_")) {
			return row;
		}
		String[] classifications = fullName.split("_", 2)[1].split(">");
		List<String> classificationValues = new ArrayList<>();
		int level = 1;
		for(String i : classifications) {
			String rowValue = (level++) + "_";
			for(int j=0; j< level-1; j++) {
				rowValue += classifications[j] + (j < level-2 ? "," : "");
			}
			classificationValues.add(rowValue);
		}

		row.put("classification_" + row.get("CLASSIFICATION_SYSTEM") + "_" + row.get("LOCALE"), classificationValues);
		return row;		
	}
}
