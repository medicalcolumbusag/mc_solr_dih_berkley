package de.medicalcolumbus.platform.solr.dih;

import org.apache.solr.handler.dataimport.CachePropertyUtil;
import org.apache.solr.handler.dataimport.Context;
import org.apache.solr.handler.dataimport.DIHCache;
import org.apache.solr.handler.dataimport.DIHCacheSupport;
import org.mapdb.*;

import org.mapdb.serializer.SerializerArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.concurrent.Executors;

import static de.medicalcolumbus.platform.solr.dih.DIHCachePersistProperties.EXPIRE_ELEMENT_MAX_SIZE;
import static de.medicalcolumbus.platform.solr.dih.DIHCachePersistProperties.EXPIRE_FROM_RAM;
import static de.medicalcolumbus.platform.solr.dih.DIHCachePersistProperties.RAM_TO_DISK_THREAD_NUMBER;
import static java.util.Objects.isNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class MapDbCache implements DIHCache {

	private static final Logger LOG = LoggerFactory.getLogger(MapDbCache.class);

	private HTreeMap<Object, HashMap<String, Object>[]> inMemoryCache;
	private HTreeMap<Object, HashMap<String, Object>[]> onDiskCache;

	private String primaryKeyName = null;
	private boolean isOpen = false;
	private boolean isReadOnly = false;

	private DB dbMemory;
	private DB dbDisk;

	private String baseLocation;
	private String cacheName;


	@Override
	public void open(Context context) {
		checkOpen(false);

		isOpen = true;

		setupCacheLocation(context);

		init(context);
	}

	@Override
	public void close() {
		flush();

		closeCaches();

		isOpen = false;
	}

	@Override
	public void flush() {
		checkOpen(true);
		checkReadOnly();
		if (!isNull(dbMemory) && !dbMemory.isClosed()) {
			dbMemory.commit();
		}
	}

	@Override
	public void destroy() {
		close();
		deleteCacheFile();
	}

	private void closeCaches() {

		if (!isNull(inMemoryCache) && !inMemoryCache.isClosed()) {
			inMemoryCache.close();
			if (!isNull(dbMemory) && !dbMemory.isClosed()) {
				dbMemory.close();
			}
		}

		if (!isNull(onDiskCache) && !onDiskCache.isClosed()) {
			onDiskCache.close();
			if (!isNull(dbDisk) && !dbDisk.isClosed()) {
				dbDisk.close();
			}
		}
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

		Map<String,Object>[] thisKeysRecs =  inMemoryCache.get(pk);

		List<Map<String, Object>> thisKeyRecsList;

		if (!isNull(thisKeysRecs)) {
			thisKeyRecsList = new ArrayList<>(Arrays.asList(thisKeysRecs));
		} else {
			thisKeyRecsList = new ArrayList<>();
		}

		thisKeyRecsList.add(rec);

		HashMap<String, Object>[] finalArray = new HashMap[thisKeyRecsList.size()];
		finalArray = thisKeyRecsList.toArray(finalArray);
		inMemoryCache.put(pk, finalArray);

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

				Map.Entry<Object,List<Map<String,Object>>> next;
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

				Map.Entry<Object,List<Map<String,Object>>> next;
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

				Map<String, Object>[] arrVal =  inMemoryCache.get(iter.next());

				if (arrVal != null) {
					vals.addAll(new ArrayList<>(Arrays.asList(arrVal)));
				}

			}
			if(vals.size()==0) {
				return null;
			}
			return vals.iterator();
		}
		Map<String,Object>[] val =  inMemoryCache.get(key);
		if (val == null) {
			return null;
		}

		for(Map<String, Object> mapValue : val) {
			for (Map.Entry<String, Object> entry : mapValue.entrySet()) {
				LOG.info("***KEY: ", entry.getKey());
				LOG.info("***VALUE: ", entry.getValue());
			}
		}


		return new ArrayList<>(Arrays.asList(val)).iterator();
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

	private void init(Context context) {

		isOpen = true;

		int expireElementMaxSize;
		int ramToDiskThreadNumber;
		int expireFromRamTime;

		String expireElementMaxSizeProp = CachePropertyUtil.getAttributeValueAsString(context, EXPIRE_ELEMENT_MAX_SIZE);
		expireElementMaxSize = !isNull(expireElementMaxSizeProp) ? Integer.parseInt(expireElementMaxSizeProp) : 1_000;

		String ramToDiskThreadNumberProp = CachePropertyUtil.getAttributeValueAsString(context, RAM_TO_DISK_THREAD_NUMBER);
		ramToDiskThreadNumber = !isNull(ramToDiskThreadNumberProp) ? Integer.parseInt(ramToDiskThreadNumberProp) : 1; // 1 thread default

		String expireFromRamProp = CachePropertyUtil.getAttributeValueAsString(context, EXPIRE_FROM_RAM);
		expireFromRamTime = !isNull(expireFromRamProp) ? Integer.parseInt(expireFromRamProp) : 5; // 5 seconds default

		String path = baseLocation + File.separator + cacheName;
		File onDiskLocation = new File(path);

		dbDisk = DBMaker
				.fileDB(onDiskLocation)
				.fileMmapEnable()
				.fileMmapEnableIfSupported()
				.fileMmapPreclearDisable()
				.cleanerHackEnable()
				.closeOnJvmShutdown()
				.make();

		dbMemory = DBMaker
				.memoryDB()
				.closeOnJvmShutdown()
				.make();

		onDiskCache = dbDisk
				.hashMap("onDisk_" + cacheName)
				.keySerializer(dbDisk.getDefaultSerializer())
				.valueSerializer(new SerializerArray<>(new MapDbSerializer(), (Class<HashMap<String, Object>>) new HashMap<String, Object>().getClass()))
				.create();

		inMemoryCache = dbMemory
				.hashMap("inMemory_" + cacheName)
				.keySerializer(dbMemory.getDefaultSerializer())
				.valueSerializer(new SerializerArray<>(new MapDbSerializer(), (Class<HashMap<String, Object>>) new HashMap<String, Object>().getClass()))
				.expireMaxSize(expireElementMaxSize)
				.expireAfterGet(expireFromRamTime, SECONDS)
				.expireOverflow(onDiskCache)
				.expireExecutor(Executors.newScheduledThreadPool(ramToDiskThreadNumber))
				.create();
	}


	private void setupCacheLocation(Context context) {
		baseLocation = CachePropertyUtil.getAttributeValueAsString(context, DIHCachePersistProperties.CACHE_BASE_DIRECTORY);
		if (isNull(baseLocation)) {
			baseLocation = System.getProperty("java.io.tmpdir");
		}

		cacheName = CachePropertyUtil.getAttributeValueAsString(context, DIHCachePersistProperties.CACHE_NAME);
		if (isNull(cacheName)) {
			cacheName = "MapDbCache_" + System.currentTimeMillis() + "_cache.db";
		} else {
			cacheName = "MapDbCache_" + cacheName;
		}

		String readOnlyStr = CachePropertyUtil.getAttributeValueAsString(context,
				DIHCacheSupport.CACHE_READ_ONLY);
		if ("true".equalsIgnoreCase(readOnlyStr)) {
			isReadOnly = true;
		}
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

	private void deleteCacheFile() {
		File cacheFile = new File(baseLocation + File.separator + cacheName);
		LOG.debug(cacheFile.getName());
		if (cacheFile.exists() && cacheFile.isFile()) {
			if (!cacheFile.delete()) {
				LOG.error("Could not delete file: " + cacheFile);
				throw new RuntimeException("Could not delete file: " + cacheFile);
			}
		}
	}
}

