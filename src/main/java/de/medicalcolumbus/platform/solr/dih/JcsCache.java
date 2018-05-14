package de.medicalcolumbus.platform.solr.dih;

import org.apache.commons.jcs.auxiliary.AuxiliaryCache;
import org.apache.commons.jcs.auxiliary.disk.indexed.IndexedDiskCache;
import org.apache.commons.jcs.auxiliary.disk.indexed.IndexedDiskCacheAttributes;
import org.apache.commons.jcs.engine.CacheElement;
import org.apache.commons.jcs.engine.behavior.ICacheElement;
import org.apache.commons.jcs.engine.control.CompositeCache;
import org.apache.commons.jcs.engine.control.CompositeCacheManager;
import org.apache.solr.handler.dataimport.CachePropertyUtil;
import org.apache.solr.handler.dataimport.Context;
import org.apache.solr.handler.dataimport.DIHCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static java.util.Objects.isNull;

public class JcsCache implements DIHCache {

	private static final Logger LOG = LoggerFactory.getLogger(JcsCache.class);

	private CompositeCache<Object, List<Map<String, Object>>> cache;

	private String cacheName;

	private String primaryKeyName = null;


	@Override
	public void open(Context context) {
		cacheName = CachePropertyUtil.getAttributeValueAsString(context, DIHCachePersistProperties.CACHE_NAME);
		if (isNull(cacheName)) {
			cacheName = "MapDbCache_" + System.currentTimeMillis() + "_cache_db";
		} else {
			cacheName = "MapDbCache_" + cacheName;
		}

		cacheName = cacheName.replaceAll("\\.", "_");

		Properties properties = createCacheProperties(cacheName);

		CompositeCacheManager compositeCacheManager = CompositeCacheManager.getUnconfiguredInstance();
		compositeCacheManager.configure(properties);

		cache = compositeCacheManager.getCache(cacheName);

		List<AuxiliaryCache<Object, List<Map<String, Object>>>> auxiliaryCaches = new ArrayList<>();

		auxiliaryCaches.add(createDiskCache(cacheName));

		cache.setAuxCaches(auxiliaryCaches.toArray(new AuxiliaryCache[0]));
	}

	@Override
	public void close() {
		cache.save();
		cache.dispose();
	}

	@Override
	public void flush() {
		cache.save();
	}

	@Override
	public void destroy() {
		cache.save();
		cache.dispose();
	}

	@Override
	public void add(Map<String, Object> rec) {
		if (rec == null || rec.size() == 0) {
			return;
		}

		if (primaryKeyName == null) {
			primaryKeyName = rec.keySet().iterator().next();
		}

		Object pk = rec.get(primaryKeyName);
		if (pk instanceof Collection<?>) {
			Collection<Object> c = (Collection<Object>) pk;
			if (c.size() != 1) {
				throw new RuntimeException(
						"The primary key must have exactly 1 element.");
			}
			pk = c.iterator().next();
		}
		//Rows with null keys are not added.
		if(pk==null) {
			return;
		}
		ICacheElement<Object, List<Map<String, Object>>> iCacheElement = cache.get(pk);
		List<Map<String,Object>> thisKeysRecs;

		if (!isNull(iCacheElement)) {
			thisKeysRecs = iCacheElement.getVal();

			if (isNull(thisKeysRecs)) {
				thisKeysRecs = new ArrayList<>();
			}
		} else {
			thisKeysRecs = new ArrayList<>();
		}

		thisKeysRecs.add(rec);

		try {
			cache.update(new CacheElement<>(cacheName, pk, thisKeysRecs));
		} catch (IOException e) {
			LOG.error(e.getMessage());
			throw new RuntimeException(e);
		}
	}

	@Override
	public Iterator<Map<String, Object>> iterator() {
		return new Iterator<Map<String, Object>>() {
			private Iterator<Object> keyIterator;
			private List<Map<String,Object>> currentKeyResult = null;
			private Iterator<Map<String,Object>> currentKeyResultIter = null;

			{
				keyIterator = cache.getKeySet().iterator();
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

				if (keyIterator.hasNext()) {
					ICacheElement<Object, List<Map<String, Object>>> iCacheElement = cache.get(keyIterator.next());
					currentKeyResult = iCacheElement.getVal();
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

				if (keyIterator.hasNext()) {
					ICacheElement<Object, List<Map<String, Object>>> iCacheElement = cache.get(keyIterator.next());
					currentKeyResult = iCacheElement.getVal();
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
		if(key==null) {
			return null;
		}
		if(key instanceof Iterable<?>) {
			List<Map<String,Object>> vals = new ArrayList<>();
			Iterator<?> iter = ((Iterable<?>) key).iterator();
			while(iter.hasNext()) {
				ICacheElement<Object, List<Map<String, Object>>> iCacheElement = cache.get(iter.next());
				if (!isNull(iCacheElement)) {
					List<Map<String,Object>> val = iCacheElement.getVal();
					if(val!=null) {
						vals.addAll(val);
					}
				}
			}
			if(vals.size()==0) {
				return null;
			}
			return vals.iterator();
		}

		ICacheElement<Object, List<Map<String, Object>>> iCacheElement = cache.get(key);

		if (isNull(iCacheElement)) {
			return null;
		}

		List<Map<String,Object>> val = iCacheElement.getVal();

		if (val == null) {
			return null;
		}
		return val.iterator();
	}

	@Override
	public void delete(Object key) {
		cache.remove(key);
	}

	@Override
	public void deleteAll() {
		try {
			cache.removeAll();
		} catch (IOException e) {
			LOG.error(e.getMessage());
			throw new RuntimeException(e);
		}
	}

	private Properties createCacheProperties(String cacheName) {
		Properties properties = new Properties();

		// region properties
		String regionKey = "jcs.region." + cacheName;
		properties.put(regionKey, "DC");
		properties.put(regionKey + ".cacheattributes", "org.apache.commons.jcs.engine.CompositeCacheAttributes");
		properties.put(regionKey + ".cacheattributes.MaxObjects", "1000");
		properties.put(regionKey + ".cacheattributes.MemoryCacheName", "org.apache.commons.jcs.engine.memory.lru.LRUMemoryCache");
		properties.put(regionKey + ".cacheattributes.UseMemoryShrinker", "true");
		properties.put(regionKey + ".cacheattributes.MaxMemoryIdleTimeSeconds", "3600");
		properties.put(regionKey + ".cacheattributes.ShrinkerIntervalSeconds", "60");
		properties.put(regionKey + ".cacheattributes.MaxSpoolPerRun", "500");
		properties.put(regionKey + ".elementattributes", "org.apache.commons.jcs.engine.ElementAttributes");
		properties. put(regionKey + ".elementattributes.IsEternal",  "false");

		return properties;
	}



	private IndexedDiskCache<Object, List<Map<String, Object>>> createDiskCache(String cacheName) {

		IndexedDiskCacheAttributes indexedDiskCacheAttributes = new IndexedDiskCacheAttributes();
		indexedDiskCacheAttributes.setCacheName(cacheName);
		indexedDiskCacheAttributes.setDiskPath("jcs_swap");
		indexedDiskCacheAttributes.setMaxPurgatorySize(10000000);
		indexedDiskCacheAttributes.setMaxKeySize(1000000);
		indexedDiskCacheAttributes.setOptimizeAtRemoveCount(300000);
		indexedDiskCacheAttributes.setShutdownSpoolTimeLimit(60);

		return new IndexedDiskCache<>(indexedDiskCacheAttributes);
	}
}
