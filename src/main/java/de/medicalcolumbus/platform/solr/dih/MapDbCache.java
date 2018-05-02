package de.medicalcolumbus.platform.solr.dih;

import org.apache.solr.handler.dataimport.CachePropertyUtil;
import org.apache.solr.handler.dataimport.Context;
import org.apache.solr.handler.dataimport.DIHCache;
import org.mapdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.concurrent.Executors;

import static java.util.Objects.isNull;

public class MapDbCache implements DIHCache {

	private static final Logger LOG = LoggerFactory.getLogger(MapDbCache.class);

	private HTreeMap<Object, Object> inMemoryCache;
	private HTreeMap<Object, Object> onDiskCache;

	private String primaryKeyName = null;
	private boolean isOpen = false;
	private boolean isReadOnly = false;

	// disk and in-memory factories
	private DB dbDisk;
	private DB dbMemory;
	private String baseLocation;
	private String cacheName;


	@Override
	public void open(Context context) {
		checkOpen(false);
		isOpen = true;

		baseLocation = CachePropertyUtil.getAttributeValueAsString(context, DIHCachePersistProperties.CACHE_BASE_DIRECTORY);
		if (isNull(baseLocation)) {
			baseLocation = System.getProperty("java.io.tmpdir");
		}
		cacheName = CachePropertyUtil.getAttributeValueAsString(context, DIHCachePersistProperties.CACHE_NAME);
		if (isNull(cacheName)) {
			cacheName = "MapDbCache-" + System.currentTimeMillis();
		}

		init();
	}

	@Override
	public void close() {
		flush();

		isOpen = false;

		if (!dbMemory.isClosed()) {
			dbMemory.close();
		}

		if (!dbDisk.isClosed()) {
			dbDisk.close();
		}
	}

	@Override
	public void flush() {
		checkOpen(true);
		checkReadOnly();
	}

	@Override
	public void destroy() {
		deleteAll(true);
		inMemoryCache = null;
		onDiskCache = null;
		isOpen = false;
	}

	@Override
	public void add(Map<String, Object> rec) {
		checkOpen(true);
		checkReadOnly();

		if (isNull(rec) || rec.size() == 0) {
			return;
		}

		if (isNull(primaryKeyName)) {
			primaryKeyName = rec.keySet().iterator().next();
		}

		Object pk = rec.get(primaryKeyName);
		if (pk instanceof Collection<?>) {
			Collection<Object> c = (Collection<Object>) pk;
			if (c.size() != 1) {
				LOG.error("The primary key must have exactly 1 element.");
				throw new RuntimeException(
						"The primary key must have exactly 1 element.");
			}
			pk = c.iterator().next();
		}
		//Rows with null keys are not added.
		if(isNull(pk)) {
			return;
		}

		List<Map<String,Object>> thisKeysRecs = (List<Map<String, Object>>) inMemoryCache.get(pk);
		if (thisKeysRecs == null) {
			thisKeysRecs = new ArrayList<>();
			inMemoryCache.put(pk, thisKeysRecs);
		}
		thisKeysRecs.add(rec);
	}

	@Override
	public Iterator<Map<String, Object>> iterator() {
		return new Iterator<Map<String, Object>>() {
			private Iterator<Map.Entry<Object,List<Map<String,Object>>>> theMapIter;
			private List<Map<String,Object>> currentKeyResult = null;
			private Iterator<Map<String,Object>> currentKeyResultIter = null;

			{
				theMapIter = inMemoryCache.entrySet().iterator();
			}

			@Override
			public boolean hasNext() {
				if (currentKeyResultIter != null) {
					if (currentKeyResultIter.hasNext()) {
						return true;
					} else {
						currentKeyResult = null;
						currentKeyResultIter = null;
					}
				}

				Map.Entry<Object,List<Map<String,Object>>> next = null;
				if (theMapIter.hasNext()) {
					next = theMapIter.next();
					currentKeyResult = next.getValue();
					currentKeyResultIter = currentKeyResult.iterator();
					if (currentKeyResultIter.hasNext()) {
						return true;
					}
				}
				return false;
			}

			@Override
			public Map<String,Object> next() {
				if (currentKeyResultIter != null) {
					if (currentKeyResultIter.hasNext()) {
						return currentKeyResultIter.next();
					} else {
						currentKeyResult = null;
						currentKeyResultIter = null;
					}
				}

				Map.Entry<Object,List<Map<String,Object>>> next = null;
				if (theMapIter.hasNext()) {
					next = theMapIter.next();
					currentKeyResult = next.getValue();
					currentKeyResultIter = currentKeyResult.iterator();
					if (currentKeyResultIter.hasNext()) {
						return currentKeyResultIter.next();
					}
				}
				return null;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}

	@Override
	public Iterator<Map<String, Object>> iterator(Object key) {
		checkOpen(true);
		if(key==null) {
			return null;
		}
		if(key instanceof Iterable<?>) {
			List<Map<String,Object>> vals = new ArrayList<>();
			Iterator<?> iter = ((Iterable<?>) key).iterator();
			while(iter.hasNext()) {
				List<Map<String,Object>> val = (List<Map<String, Object>>) inMemoryCache.get(iter.next());
				if(val!=null) {
					vals.addAll(val);
				}
			}
			if(vals.size()==0) {
				return null;
			}
			return vals.iterator();
		}
		List<Map<String,Object>> val = (List<Map<String, Object>>) inMemoryCache.get(key);
		if (val == null) {
			return null;
		}
		return val.iterator();
	}

	@Override
	public void delete(Object key) {
		checkOpen(true);
		checkReadOnly();
		if(key==null) {
			return;
		}
		inMemoryCache.remove(key);
	}

	@Override
	public void deleteAll() {
		deleteAll(false);
	}


	private void checkOpen(boolean shouldItBe) {
		if (!isOpen && shouldItBe) {
			throw new IllegalStateException(
					"Must call open() before using this cache.");
		}
		if (isOpen && !shouldItBe) {
			throw new IllegalStateException("The cache is already open.");
		}
	}

	private void checkReadOnly() {
		if (isReadOnly) {
			throw new IllegalStateException("Cache is read-only.");
		}
	}

	private void init() {

		String path = baseLocation + File.separator + cacheName;
		File onDiskLocation = new File(path);
		if (onDiskLocation.mkdirs()) {
			LOG.error("Cache " + path + " not created on disk!");
			throw new RuntimeException("Cache " + path + " not created on disk!");
		}

		dbDisk = DBMaker
				.fileDB(onDiskLocation)
				.closeOnJvmShutdown()
				.make();

		dbMemory = DBMaker
				.memoryDB()
				.closeOnJvmShutdown()
				.make();

		onDiskCache = dbDisk
				.hashMap("onDisk")
				.keySerializer(dbDisk.getDefaultSerializer())
				.valueSerializer(dbDisk.getDefaultSerializer())
				.create();

		inMemoryCache = dbMemory
				.hashMap("inMemory")
				.keySerializer(dbMemory.getDefaultSerializer())
				.valueSerializer(dbMemory.getDefaultSerializer())
				.expireMaxSize(128)
				.expireAfterCreate()
				.expireOverflow(onDiskCache)
				.expireExecutor(Executors.newScheduledThreadPool(2))
				.create();
	}


	private void deleteAll(boolean readOnlyOk) {
		if (!readOnlyOk) {
			checkReadOnly();
		}

		if (!isNull(inMemoryCache)) {
			inMemoryCache.clear();
		}

		if (!isNull(onDiskCache)) {
			onDiskCache.clear();
		}
	}
}

//	checkOpen(false);
//	isOpen = true;
//    if (theMap == null) {
//		theMap = new TreeMap<>();
//	}
//
//	String pkName = CachePropertyUtil.getAttributeValueAsString(context,
//			DIHCacheSupport.CACHE_PRIMARY_KEY);
//    if (pkName != null) {
//		primaryKeyName = pkName;
//	}
//	isReadOnly = false;
//	String readOnlyStr = CachePropertyUtil.getAttributeValueAsString(context,
//			DIHCacheSupport.CACHE_READ_ONLY);
//    if ("true".equalsIgnoreCase(readOnlyStr)) {
//		isReadOnly = true;
//	}

