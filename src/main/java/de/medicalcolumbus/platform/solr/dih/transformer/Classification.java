package de.medicalcolumbus.platform.solr.dih.transformer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
