package de.medicalcolumbus.platform.solr.dih;

/**
 * <p>Use this enum to specify the data types of the entity fields which will be going to a DIH Cache
 * <ul>
 * <li>BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE, BOOLEAN, CHARACTER - java primitives / primitive wrappers</li>
 * <li>STRING - java.lang.String</li>
 * <li>DATE - java.util.Date</li>
 * <li>BIGINTEGER - java.math.BigInteger<li>
 * <li>BIGDECIMAL - java.math.BigDecimal</li>
 * <li>BIGDECIMAL_INTEGER - java.math.BigDecimal, but all values are guaranteed to be of integer precision (useful with the Oracle jdbc driver)</li>
 * <li>CLOB - java.sql.Clob</li>
 * <li>NULL - all values will be NULLs</li>
 * </ul>
 */
public enum DIHCacheTypes {
	BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE, BOOLEAN, CHARACTER, STRING, DATE, BIGINTEGER, BIGDECIMAL, BIGDECIMAL_INTEGER, CLOB, NULL
}
