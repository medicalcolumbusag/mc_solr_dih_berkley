package de.medicalcolumbus.platform.solr.dih;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.*;
import org.apache.solr.handler.dataimport.CachePropertyUtil;
import org.apache.solr.handler.dataimport.Context;
import org.apache.solr.handler.dataimport.DIHCache;
import org.apache.solr.handler.dataimport.DIHCacheSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.rowset.serial.SerialClob;
import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Pattern;


public class BerkleyBackedCache implements DIHCache {

	/**
	 * +   * <p>
	 * +   * Specify BDB-JEs internal cache eviction policy: DEFAULT, EVICT_LN or
	 * +   * EVICT_BIN
	 * +   *
	 * +
	 */
	public static final String BERKLEY_EVICT_POLICY = "berkleyInternalEvictPolicy";
	/**
	 * +   * <p>
	 * +   * Specify "true" if this cache should share its internal cache with all other
	 * +   * open BerkleyBackedCaches (that also specify shared=true)
	 * +   *
	 * +
	 */
	public static final String BERKLEY_SHARED = "berkleyInternalShared";
	/**
	 * +   * <p>
	 * +   * Set the # of bytes to allow for second-level internal caching. This is a
	 * +   * performance-tuning parameter.
	 * +   *
	 * +
	 */
	public static final String BERKLEY_INTERNAL_CACHE_SIZE = "berkleyInternalCacheSize";
	/**
	 * <p>
	 * Specify "true" if this cache will be accessed by multiple threads (using
	 * DIH "threads" parameter), in one of these situations:
	 * <ul>
	 * <li>the cache is used by a DIHCacheProcessor with the DIH Root Entity</li>
	 * <li>the cache is a used by a non-keyed entity processor (ex:
	 * SQLEntityProcessor without a "where" clause)</li>
	 * </ul>
	 */
	public static final String BERKLEY_TRANSACTIONAL = "berkleyTransactional";
	private static final Logger LOG = LoggerFactory.getLogger(BerkleyBackedCache.class);
	private static final String CACHE_PROP_FOR_NAMES = "CACHE_NAMES";
	private static final String CACHE_PROP_FOR_TYPES = "CACHE_TYPES";

	private static final Pattern splitMetadataPattern = Pattern.compile("\\s*,\\s*");
	private Environment env = null;
	private Database db = null;
	private boolean readOnly = false;
	private boolean transactional = false;
	private boolean rememberChangedKeys = false;
	private boolean changedKeysArriveInOrder = false;
	private boolean disableDuplicateAdds = false;
	private Set<Object> changedKeys = null;
	private Long internalCacheSize = null;
	private String[] columns;
	private String pkColumn;
	private int pkColumnIndex;
	private DIHCacheTypes[] types;
	private String[] passedInColumns;
	private DIHCacheTypes[] passedInTypes;
	private String baseLoc;
	private String cacheName;
	private CacheMode cacheMode = CacheMode.DEFAULT;
	private boolean sharedCache = false;
	private long totalTimeNano = 0;
	private Properties cacheProperties = null;
	private List<BerkleyBackedCacheIterator> iterators = new ArrayList<BerkleyBackedCacheIterator>();

	public BerkleyBackedCache() {
	}

	private void checkOpen(boolean shouldItBe) {
		if (env == null && shouldItBe) {
			throw new IllegalStateException(
					"Must call open() before using this cache.");
		}
		if (env != null && !shouldItBe) {
			throw new IllegalStateException("The cache is already open.");
		}
	}

	@Override
	public void open(Context context) {
		checkOpen(false);

		baseLoc = CachePropertyUtil.getAttributeValueAsString(context, DIHCachePersistProperties.CACHE_BASE_DIRECTORY);
		if (baseLoc == null) {
			baseLoc = System.getProperty("java.io.tmpdir");
		}
		cacheName = CachePropertyUtil.getAttributeValueAsString(context, DIHCachePersistProperties.CACHE_NAME);
		if (cacheName == null) {
			cacheName = "BerkleyBackedCache-" + System.currentTimeMillis();
		}
		String cacheDeletePriorData = CachePropertyUtil.getAttributeValueAsString(context,
				DIHCacheSupport.CACHE_DELETE_PRIOR_DATA);
		if ("true".equalsIgnoreCase(cacheDeletePriorData)) {
			destroy();
		}
		readOnly = false;
		String cacheReadOnly = CachePropertyUtil.getAttributeValueAsString(context, DIHCacheSupport.CACHE_READ_ONLY);
		if ("true".equalsIgnoreCase(cacheReadOnly)) {
			readOnly = true;
		}
		transactional = false;
		String cacheTransactional = CachePropertyUtil.getAttributeValueAsString(context, BERKLEY_TRANSACTIONAL);
		if ("true".equalsIgnoreCase(cacheTransactional)) {
			transactional = true;
		}

		rememberChangedKeys = false;
		if ("true".equalsIgnoreCase(
				CachePropertyUtil.getAttributeValueAsString(context, DIHCachePersistProperties.CACHE_DELTA_WITH_FULL_UPDATE_NO_CLEAN))) {
			rememberChangedKeys = true;
			changedKeys = new HashSet<Object>();
		}

		changedKeysArriveInOrder = false;
		if ("true".equalsIgnoreCase(
				CachePropertyUtil.getAttributeValueAsString(context, DIHCachePersistProperties.CACHE_ADDS_ARRIVE_IN_KEY_ORDER))) {
			changedKeysArriveInOrder = true;
		}
		disableDuplicateAdds = false;

		if ("true".equalsIgnoreCase(
				CachePropertyUtil.getAttributeValueAsString(context,
						DIHCachePersistProperties.CACHE_NO_DUPLICATE_KEYS))) {
			disableDuplicateAdds = true;
			rememberChangedKeys = false;
			changedKeys = null;
		}
		String bep = CachePropertyUtil.getAttributeValueAsString(context, BERKLEY_EVICT_POLICY);
		if ("EVICT_LN".equals(bep)) {
			cacheMode = CacheMode.EVICT_LN;
		} else if ("EVICT_BIN".equals(bep)) {
			cacheMode = CacheMode.EVICT_BIN;
		} else {
			cacheMode = CacheMode.DEFAULT;
		}

		String bsh = CachePropertyUtil.getAttributeValueAsString(context, BERKLEY_SHARED);

		if ("true".equalsIgnoreCase(bsh)) {
			sharedCache = true;
		} else {
			sharedCache = false;
		}

		String cics = CachePropertyUtil.getAttributeValueAsString(context, BERKLEY_INTERNAL_CACHE_SIZE);
		if (cics != null) {
			try {
				internalCacheSize = Long.parseLong(cics);
			} catch (NumberFormatException e) {
				throw new IllegalArgumentException("Value of "
						+ BERKLEY_INTERNAL_CACHE_SIZE + " must be a number");
			}
		}
		Object names = CachePropertyUtil.getAttributeValue(context,
				DIHCachePersistProperties.FIELD_NAMES);
		if (names != null) {
			if (names instanceof String[]) {
				passedInColumns = (String[]) names;
			} else if (names instanceof String) {
				String[] namesArr = splitMetadataPattern.split(((String) names));
				passedInColumns = new String[namesArr.length];
				for (int i = 0; i < namesArr.length; i++) {
					passedInColumns[i] = namesArr[i].trim();
				}
			}
		}
		Object fieldTypes = CachePropertyUtil.getAttributeValue(context,
				DIHCachePersistProperties.FIELD_TYPES);
		if (fieldTypes != null) {
			if (fieldTypes instanceof DIHCacheTypes[]) {
				passedInTypes = (DIHCacheTypes[]) fieldTypes;
			} else if (fieldTypes instanceof String) {
				String[] typesArr = splitMetadataPattern.split(((String) fieldTypes));
				passedInTypes = new DIHCacheTypes[typesArr.length];
				for (int i = 0; i < typesArr.length; i++) {
					passedInTypes[i] = DIHCacheTypes.valueOf(typesArr[i].trim());
				}
			}
		}
		pkColumn = CachePropertyUtil.getAttributeValueAsString(context,
				DIHCacheSupport.CACHE_PRIMARY_KEY);

		init();
	}

	private void init() {
		try {
			File f = new File(baseLoc + File.separator + cacheName);
			f.mkdirs();

			cacheProperties = new Properties();
			File propsFile = new File(baseLoc + File.separator + cacheName
					+ "_cache.properties");
			if (propsFile.exists()) {
				InputStream is = null;
				try {
					is = new FileInputStream(propsFile);
					cacheProperties.load(is);
				} catch (Exception e) {
					throw e;
				} finally {
					try {
						is.close();
					} catch (Exception ex) {
					}
				}
			} else if (readOnly) {
				throw new Exception(
						"The database is set to read-only, but the 'properties' file does not exist: "
								+ propsFile);
			}

			EnvironmentConfig envConfig = new EnvironmentConfig();
			if (transactional) {
				envConfig.setTransactional(true);
			}
			envConfig.setAllowCreate(true);
			if (internalCacheSize != null) {
				envConfig.setCacheSize(internalCacheSize);
			} else {
				envConfig.setCachePercent(2);
			}
			envConfig.setCacheMode(cacheMode);
			envConfig.setSharedCache(sharedCache);
			envConfig.setConfigParam("je.log.fileMax", "1000000000"); // 1gb max file
			// size
			env = new Environment(f, envConfig);

			DatabaseConfig dbConfig = new DatabaseConfig();
			if (transactional) {
				dbConfig.setTransactional(true);
			} else {
				dbConfig.setDeferredWrite(true);
			}
			dbConfig.setAllowCreate(true);
			dbConfig.setReadOnly(readOnly);
			dbConfig.setSortedDuplicates(false);
			db = env.openDatabase(null, cacheName, dbConfig);

			columns = passedInColumns;
			types = passedInTypes;

			String[] dbNames = getNames();
			DIHCacheTypes[] dbTypes = getTypes();
			if (dbNames != null) {
				types = dbTypes;
				columns = dbNames;
			} else if (!readOnly && columns != null) {
				putNames(columns);
				putTypes(types);
			}
			if (pkColumn == null && dbNames != null) {
				pkColumn = dbNames[0];
				LOG.info("Assuming " + pkColumn
						+ " is the primary key because it was not explicitly set...");
			}
			if (pkColumn != null && columns != null) {
				for (int i = 0; i < columns.length; i++) {
					if (columns[i].equals(pkColumn)) {
						pkColumnIndex = i;
						break;
					}
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	private DIHCacheTypes[] getTypes() throws Exception {
		String types = (String) cacheProperties.get(CACHE_PROP_FOR_TYPES);
		if (types != null) {
			String[] typesStr = splitMetadataPattern.split(types);
			DIHCacheTypes[] retTypes = new DIHCacheTypes[typesStr.length];
			for (int i = 0; i < typesStr.length; i++) {
				retTypes[i] = DIHCacheTypes.valueOf(typesStr[i]);
			}
			return retTypes;
		} else {
			return null;
		}
	}

	private String[] getNames() throws Exception {
		String names = (String) cacheProperties.get(CACHE_PROP_FOR_NAMES);
		if (names != null) {
			String[] namesStr = splitMetadataPattern.split(names);
			String[] retNames = new String[namesStr.length];
			for (int i = 0; i < namesStr.length; i++) {
				retNames[i] = namesStr[i];
			}
			return retNames;
		} else {
			return null;
		}
	}

	private void putNames(String[] putNames) throws Exception {
		String nameProp = "";
		if (putNames != null) {
			StringBuilder nameBuilder = new StringBuilder();
			for (int i = 0; i < putNames.length; i++) {
				if (nameBuilder.length() > 0) {
					nameBuilder.append(",");
				}
				nameBuilder.append(putNames[i]);
			}
			nameProp = nameBuilder.toString();
		}
		cacheProperties.put(CACHE_PROP_FOR_NAMES, nameProp);
		writeProperties();
	}

	private void writeProperties() throws Exception {
		File propsFile = new File(baseLoc + File.separator + cacheName
				+ "_cache.properties");
		Writer w = null;
		try {
			w = new FileWriter(propsFile);
			cacheProperties.store(w, "");
		} catch (Exception e) {
			throw e;
		} finally {
			try {
				w.close();
			} catch (Exception ex) {
			}
		}
	}

	private void putTypes(DIHCacheTypes[] putTypes) throws Exception {
		String typeProp = "";
		if (putTypes != null) {
			StringBuilder typeBuilder = new StringBuilder();
			for (int i = 0; i < putTypes.length; i++) {
				if (typeBuilder.length() > 0) {
					typeBuilder.append(",");
				}
				typeBuilder.append(putTypes[i].name());
			}
			typeProp = typeBuilder.toString();
		}
		cacheProperties.put(CACHE_PROP_FOR_TYPES, typeProp);
		writeProperties();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void add(Map<String, Object> rec) {
		checkOpen(true);
		long start = System.nanoTime();
		try {
			if (columns == null) {
				buildColumnsAndTypes(rec);
				if (!readOnly) {
					putNames(columns);
					putTypes(types);
					LOG.info("FIELD NAMES: " + Arrays.asList(getNames()).toString());
					LOG.info("FIELD TYPES: " + Arrays.asList(getTypes()).toString());
				}
			}

			TupleBinding<Object> keyBind = new PrimaryKeyTupleBinding();
			DatabaseEntry theKey = new DatabaseEntry();
			Object keyObj = null;
			Object[] data = new Object[types.length - 1];

			for (Map.Entry<String, Object> entry : rec.entrySet()) {
				int i = getColumnIndex(entry.getKey());
				if (i != pkColumnIndex) {
					if (i > pkColumnIndex) {
						i--;
					}
					if (i < 0) {
						LOG
								.warn("Skipped data element: "
										+ entry.getKey()
										+ " because either it was not specified in the first row of data or not in parameters: "
										+ DIHCachePersistProperties.FIELD_NAMES + " & "
										+ DIHCachePersistProperties.FIELD_TYPES);
					} else {
						if (entry.getValue() instanceof List
								&& ((List) entry.getValue()).size() == 1) {
							data[i] = ((List) entry.getValue()).get(0);
						} else {
							data[i] = entry.getValue();
						}
					}
				} else {
					keyObj = entry.getValue();
					keyBind.objectToEntry(keyObj, theKey);
				}
			}

			List<Object[]> priorData = null;
			if (disableDuplicateAdds
					|| (rememberChangedKeys && !changedKeys.contains(keyObj))) {
				delete(keyObj);
			} else {
				priorData = get(theKey);
			}
			if (priorData == null) {
				priorData = new ArrayList<Object[]>(1);
			}
			priorData.add(data);

			TupleBinding<List<Object[]>> bind = new CacheTupleBinding();
			DatabaseEntry theData = new DatabaseEntry();
			bind.objectToEntry(priorData, theData);
			db.put(null, theKey, theData);

			if (rememberChangedKeys) {
				if (changedKeysArriveInOrder) {
					changedKeys.clear();
				}
				changedKeys.add(keyObj);
			}
		} catch (Exception e) {
			LOG.warn("Exception thrown: " + e);
			throw new RuntimeException(e);
		}
		totalTimeNano += (System.nanoTime() - start);
	}

	@SuppressWarnings("unchecked")
	private void buildColumnsAndTypes(Map<String, Object> rec) {
		columns = new String[rec.size()];
		types = new DIHCacheTypes[rec.size()];
		int i = 0;
		for (Map.Entry<String, Object> entry : rec.entrySet()) {
			columns[i] = entry.getKey();
			Object o = entry.getValue();
			if (o instanceof Collection) {
				Collection coll = (Collection) o;
				if (coll.size() > 0) {
					o = coll.iterator().next();
				}
			}
			if (o instanceof Byte) {
				types[i] = DIHCacheTypes.BYTE;
			} else if (o instanceof Short) {
				types[i] = DIHCacheTypes.SHORT;
			} else if (o instanceof Integer) {
				types[i] = DIHCacheTypes.INTEGER;
			} else if (o instanceof Float) {
				types[i] = DIHCacheTypes.FLOAT;
			} else if (o instanceof Double) {
				types[i] = DIHCacheTypes.DOUBLE;
			} else if (o instanceof Boolean) {
				types[i] = DIHCacheTypes.BOOLEAN;
			} else if (o instanceof Character) {
				types[i] = DIHCacheTypes.CHARACTER;
			} else if (o instanceof Date) {
				types[i] = DIHCacheTypes.DATE;
			} else if (o instanceof BigInteger) {
				types[i] = DIHCacheTypes.BIGINTEGER;
			} else if (o instanceof BigDecimal) {
				types[i] = DIHCacheTypes.BIGDECIMAL;
			} else if (o instanceof Clob) {
				types[i] = DIHCacheTypes.CLOB;
			} else {
				types[i] = DIHCacheTypes.STRING;
			}
			i++;
		}
		if (pkColumn == null) {
			pkColumn = columns[0];
			pkColumnIndex = 0;
			LOG.info("Assuming " + pkColumn
					+ " is the primary key because it was not explicitly set...");
		}
	}

	@Override
	public void close() {
		checkOpen(true);
		if (!readOnly) {
			flush();
		}

		LOG.info("Total read/write time for cache: " + cacheName + " was "
				+ (totalTimeNano / 1000000) + " ms");

		for (BerkleyBackedCacheIterator iter : iterators) {
			iter.close();
		}
		iterators = new ArrayList<BerkleyBackedCacheIterator>();

		if (db != null) {
			try {
				db.close();
			} catch (Exception e) {
				LOG.warn("couldn't close db for cache: " + cacheName);
			}
		}
		if (env != null) {
			try {
				env.close();
			} catch (Exception e) {
				LOG.warn("couldn't close environment for cache: " + cacheName);
			}
		}
		env = null;
		db = null;

		if (rememberChangedKeys) {
			changedKeys.clear();
		} else {
			changedKeys = null;
		}

		totalTimeNano = 0;
	}

	@Override
	public void flush() {
		checkOpen(true);
		if (!readOnly && env != null) {
			long start = System.nanoTime();
			env.sync();
			long end = System.nanoTime();
			totalTimeNano += (end - start);
		}
	}

	@Override
	public void delete(Object key) {
		checkOpen(true);
		try {
			long start = System.nanoTime();

			TupleBinding<Object> keyBind = new PrimaryKeyTupleBinding();
			DatabaseEntry theKey = new DatabaseEntry();
			keyBind.objectToEntry(key, theKey);

			db.delete(null, theKey);
			totalTimeNano += (System.nanoTime() - start);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	@Override
	public void deleteAll() {
		checkOpen(true);
		destroy();
		init();
	}

	@Override
	public void destroy() {
		if (env != null) {
			close();
		}

		File f = new File(baseLoc + File.separator + cacheName);
		File[] filesToDelete = f.listFiles();
		if (filesToDelete != null) {
			for (File f1 : f.listFiles()) {
				f1.delete();
			}
			boolean deleted = f.delete();
			if (!deleted) {
				throw new RuntimeException("Could not delete cache: " + f);
			}
			File propsFile = new File(baseLoc + File.separator + cacheName
					+ "_cache.properties");
			deleted = propsFile.delete();
			if (!deleted) {
				LOG.warn("Could not delete cache Property File: " + propsFile);
			}
		}
	}

	private List<Map<String, Object>> parseData(DatabaseEntry theKey, DatabaseEntry theData) throws Exception {
		TupleBinding<Object> keyBind = new PrimaryKeyTupleBinding();
		Object keyObj = keyBind.entryToObject(theKey);

		TupleBinding<List<Object[]>> bind = new CacheTupleBinding();
		List<Object[]> objsList = bind.entryToObject(theData);
		List<Map<String, Object>> mapList = new ArrayList<Map<String, Object>>(
				objsList.size());

		for (Object[] objs : objsList) {
			Map<String, Object> theMap = new HashMap<String, Object>();
			theMap.put(pkColumn, keyObj);
			int j = 0;
			for (int i = 0; i < columns.length; i++) {
				if (!columns[i].equals(pkColumn)) {
					if (objs[j] != null) {
						theMap.put(columns[i], objs[j]);
					}
					j++;
				}
			}
			mapList.add(theMap);
		}
		return mapList;
	}

	private List<Object[]> get(DatabaseEntry theKey) {
		DatabaseEntry theData = new DatabaseEntry();
		OperationStatus os = db.get(null, theKey, theData, LockMode.DEFAULT);
		if (os != OperationStatus.SUCCESS) {
			return null;
		}

		TupleBinding<List<Object[]>> bind = new CacheTupleBinding();
		List<Object[]> objsList = bind.entryToObject(theData);
		return objsList;
	}

	@Override
	public Iterator<Map<String, Object>> iterator(Object key) {
		checkOpen(true);
		long start = System.nanoTime();
		try {
			TupleBinding<Object> keyBind = new PrimaryKeyTupleBinding();
			DatabaseEntry theKey = new DatabaseEntry();
			keyBind.objectToEntry(key, theKey);
			DatabaseEntry theData = new DatabaseEntry();
			OperationStatus os = db.get(null, theKey, theData, LockMode.DEFAULT);
			if (os != OperationStatus.SUCCESS) {
				totalTimeNano += (System.nanoTime() - start);
				return null;
			}
			List<Map<String, Object>> returnDataList = parseData(theKey, theData);
			totalTimeNano += (System.nanoTime() - start);
			return returnDataList.iterator();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Iterator<Map<String, Object>> iterator() {
		return new BerkleyBackedCacheIterator();
	}

	private int getColumnIndex(String name) {
		for (int i = 0; i < columns.length; i++) {
			if (name.equalsIgnoreCase(columns[i])) {
				return i;
			}
		}
		return -1;
	}

	private void writeObj(DIHCacheTypes type, Object o, TupleOutput to, boolean allowNulls) {
		boolean isNull = (o == null);
		if (isNull && !allowNulls) {
			throw new RuntimeException(
					"A NULL was attempted for write but not allowed.");
		}
		if (allowNulls && type != DIHCacheTypes.NULL) {
			to.writeBoolean(isNull);
		}
		if (!isNull) {
			switch (type) {
				case BYTE:
					to.writeByte(((Byte) o).byteValue());
					break;
				case SHORT:
					to.writeShort(((Short) o).shortValue());
					break;
				case INTEGER:
					to.writeInt(((Integer) o).intValue());
					break;
				case LONG:
					to.writeLong(((Long) o).longValue());
					break;
				case FLOAT:
					to.writeFloat(((Float) o).floatValue());
					break;
				case DOUBLE:
					to.writeDouble(((Double) o).doubleValue());
					break;
				case BOOLEAN:
					to.writeBoolean(((Boolean) o).booleanValue());
					break;
				case CHARACTER:
					to.writeChar(((Character) o).charValue());
					break;
				case STRING:
					to.writeString(o.toString());
					break;
				case DATE:
					Date d = (Date) o;
					long l = d.getTime();
					to.writeLong(l);
					break;
				case BIGINTEGER:
					to.writeBigInteger((BigInteger) o);
					break;
				case BIGDECIMAL:
					BigDecimal bd = (BigDecimal) o;
					String bdstr = bd.toString();
					to.writeString(bdstr);
					break;
				case BIGDECIMAL_INTEGER:
					BigDecimal bdi = (BigDecimal) o;
					int bdint = bdi.intValue();
					to.writeInt(bdint);
					break;
				case CLOB:
					try {
						Clob cl = (Clob) o;
						StringBuilder sb = new StringBuilder();
						Reader in = cl.getCharacterStream();
						char[] cbuf = new char[1024];
						int numGot = -1;
						while ((numGot = in.read(cbuf)) != -1) {
							sb.append(String.valueOf(cbuf, 0, numGot));
						}
						to.writeString(sb.toString());
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
					break;
				case NULL:
					break;
				default:
					String a = o.toString();
					to.writeString(a);
			}
		}
	}

	private Object readObj(DIHCacheTypes type, TupleInput ti, boolean allowNulls) {
		Object returnObj = null;
		if (allowNulls && type != DIHCacheTypes.NULL) {
			boolean isNull = ti.readBoolean();
			if (isNull) {
				return null;
			}
		}
		switch (type) {
			case BYTE:
				returnObj = ti.readByte();
				break;
			case SHORT:
				returnObj = ti.readShort();
				break;
			case INTEGER:
				returnObj = ti.readInt();
				break;
			case LONG:
				returnObj = ti.readLong();
				break;
			case FLOAT:
				returnObj = ti.readFloat();
				break;
			case DOUBLE:
				returnObj = ti.readDouble();
				break;
			case BOOLEAN:
				returnObj = ti.readBoolean();
				break;
			case CHARACTER:
				returnObj = ti.readChar();
				break;
			case STRING:
				returnObj = ti.readString();
				break;
			case DATE:
				long l = ti.readLong();
				Date d = new Date();
				d.setTime(l);
				returnObj = d;
				break;
			case BIGINTEGER:
				returnObj = ti.readBigInteger();
				break;
			case BIGDECIMAL:
				returnObj = new BigDecimal(ti.readString());
				break;
			case BIGDECIMAL_INTEGER:
				returnObj = new BigDecimal(ti.readInt());
				break;
			case CLOB:
				try {
					returnObj = new SerialClob(ti.readString().toCharArray());
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
				break;
			case NULL:
				returnObj = null;
				break;
			default:
				returnObj = ti.readString();
		}
		return returnObj;
	}

	class BerkleyBackedCacheIterator implements Iterator<Map<String, Object>> {
		private Cursor orderedCursor = null;
		private Transaction cursorTransaction = null;
		private List<Map<String, Object>> currentKeysData = null;
		private Iterator<Map<String, Object>> currentKeysIterator = null;
		private Map<String, Object> next = null;
		private boolean closed = false;

		public BerkleyBackedCacheIterator() {

		}

		private void close() {
			if (closed) {
				return;
			}
			if (orderedCursor != null) {
				try {
					orderedCursor.close();
				} catch (Exception e) {
					LOG.warn("couldn't close cursor for cache iterator: " + cacheName);
				}
				if (transactional) {
					try {
						cursorTransaction.commit();
					} catch (Exception e) {
						LOG.warn("couldn't close transaction for cache iterator: "
								+ cacheName);
					}
				}
				orderedCursor = null;
				cursorTransaction = null;
			}
			closed = true;
		}

		@Override
		public boolean hasNext() {
			checkOpen();

			if (closed) {
				return false;
			}

			if (next != null) {
				return true;
			}

			if (orderedCursor == null) {
				if (transactional) {
					cursorTransaction = env.beginTransaction(null, null);
				} else {
					cursorTransaction = null;
				}
				orderedCursor = db.openCursor(cursorTransaction, null);
			}

			long start = System.nanoTime();

			try {
				if (currentKeysData != null && currentKeysIterator.hasNext()) {
					next = currentKeysIterator.next();
					return true;
				} else {
					OperationStatus retVal = null;
					if (retVal == null || retVal == OperationStatus.SUCCESS) {
						DatabaseEntry theKey = new DatabaseEntry();
						DatabaseEntry theData = new DatabaseEntry();
						retVal = orderedCursor.getNext(theKey, theData, LockMode.DEFAULT);
						if (retVal == OperationStatus.SUCCESS) {
							currentKeysData = parseData(theKey, theData);
							currentKeysIterator = currentKeysData.iterator();
							totalTimeNano += (System.nanoTime() - start);
							next = currentKeysIterator.next();
							return true;
						}
					}
				}
				close();
				return false;
			} catch (IllegalStateException e) {
				if (e.toString().toLowerCase().contains("thread")) {
					throw new RuntimeException("If using multiple threads, you must set "
							+ BERKLEY_TRANSACTIONAL + " to true.", e);
				} else {
					throw new RuntimeException(e);
				}
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		private void checkOpen() {
			if (env == null) {
				throw new IllegalStateException("The underlying cache is not open.");
			}
		}

		@Override
		public Map<String, Object> next() {
			if (hasNext()) {
				Map<String, Object> nextObj = next;
				next = null;
				return nextObj;
			}
			return null;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	class HeaderTupleBinding extends TupleBinding<String[]> {

		@Override
		public String[] entryToObject(TupleInput ti) {
			int size = ti.readInt();
			if (size < 1) {
				return null;
			}
			String[] retStr = new String[size];
			for (int i = 0; i < size; i++) {
				retStr[i] = ti.readString();
			}
			return retStr;
		}

		@Override
		public void objectToEntry(String[] s, TupleOutput to) {
			to.writeInt(s.length);
			for (String s1 : s) {
				to.writeString(s1);
			}
		}

	}

	class CacheTupleBinding extends TupleBinding<List<Object[]>> {
		@SuppressWarnings("unchecked")
		@Override
		public List<Object[]> entryToObject(TupleInput ti) {
			List<Object[]> objsList = new ArrayList<Object[]>(2);
			while (true) {
				Object o[] = new Object[types.length - 1];
				int k = 0;
				for (int i = 0; i < types.length; i++) {
					if (!columns[i].equals(pkColumn)) {
						int size = ti.readShort();
						if (size == 1) {
							o[k] = readObj(types[i], ti, true);
						} else {
							List theList = new ArrayList(size);
							o[k] = theList;
							for (int j = 0; j < size; j++) {
								theList.add(readObj(types[i], ti, true));
							}
						}
						k++;
					}
				}
				objsList.add(o);
				if (!ti.readBoolean()) {
					break;
				}
			}
			return objsList;
		}

		@SuppressWarnings("unchecked")
		@Override
		public void objectToEntry(List<Object[]> objsList, TupleOutput to) {
			for (int h = 0; h < objsList.size(); h++) {
				Object[] objs = objsList.get(h);
				for (int i = 0; i < types.length; i++) {
					if (!columns[i].equals(pkColumn)) {
						int j = i;
						if (i > pkColumnIndex) {
							j--;
						}
						if (objs[j] instanceof List) {
							int size = ((List) objs[j]).size();
							if (size > Short.MAX_VALUE) {
								LOG.warn("Cannot store all the values.  Max is: "
										+ Short.MAX_VALUE);
								List temp = new ArrayList(Short.MAX_VALUE);
								for (int jj = 0; jj < Short.MAX_VALUE; jj++) {
									temp.add(((List) objs[j]).get(jj));
								}
								objs[j] = temp;
								size = Short.MAX_VALUE;
							}
							to.writeShort(size);
							for (Object o : (List) objs[j]) {
								writeObj(types[i], o, to, true);
							}
						} else {
							int size = 1;
							to.writeShort(size);
							writeObj(types[i], objs[j], to, true);
						}
					}
				}
				if (h == objsList.size() - 1) {
					to.writeBoolean(false);
				} else {
					to.writeBoolean(true);
				}
			}
		}
	}

	class PrimaryKeyTupleBinding extends TupleBinding<Object> {
		@Override
		public Object entryToObject(TupleInput ti) {
			DIHCacheTypes type = types[pkColumnIndex];
			return readObj(type, ti, false);

		}

		@SuppressWarnings("unchecked")
		@Override
		public void objectToEntry(Object obj, TupleOutput to) {
			if (types == null) {
				return;
			}

			if (obj instanceof List) {
				if (((List) obj).size() > 1) {
					LOG.warn("Discarding duplicated primary keys: " + obj);
				}
				obj = ((List) obj).iterator().next();

			}
			DIHCacheTypes type = types[pkColumnIndex];
			writeObj(type, obj, to, false);
		}
	}
}
