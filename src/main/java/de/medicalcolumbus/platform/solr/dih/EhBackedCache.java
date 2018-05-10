package de.medicalcolumbus.platform.solr.dih;

import org.apache.solr.handler.dataimport.CachePropertyUtil;
import org.apache.solr.handler.dataimport.Context;
import org.apache.solr.handler.dataimport.DIHCache;
import org.apache.solr.handler.dataimport.DIHCacheSupport;
import org.ehcache.Cache;
import org.ehcache.CachePersistenceException;
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;

import java.io.File;
import java.util.*;

public class EhBackedCache implements DIHCache {

	private Cache<String, EhCacheEntry> theCache = null;
	private PersistentCacheManager cacheManager = null;
	private boolean isOpen = false;
	private boolean isReadOnly = false;
	private String cacheName = null;
	private String baseLocation = null;
	private Long maxCacheMemSize = null;
	String primaryKeyName = null;

	@SuppressWarnings("unchecked")
	@Override
	public void add(Map<String, Object> rec) {
		checkOpen(true);
		checkReadOnly();

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
				throw new IllegalArgumentException("The primary key must have exactly 1 element.");
			}
			pk = c.iterator().next();
		}
		//Rows with null keys are not added.
		if (pk == null) {
			return;
		}
		EhCacheEntry cacheEntry = theCache.get(pk.toString());
		List<Map<String, Object>> thisKeysRecs = null;
		if (cacheEntry == null) {
			thisKeysRecs = new ArrayList<>();
			theCache.put(pk.toString(), new EhCacheEntry(thisKeysRecs));
		} else {
			thisKeysRecs = cacheEntry.getValues();
		}
		thisKeysRecs.add(rec);
	}

	private void checkOpen(boolean shouldItBe) {
		if (!isOpen && shouldItBe) {
			throw new IllegalStateException("Must call open() before using this cache.");
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

	@Override
	public void close() {
		isOpen = false;
	}

	@Override
	public void delete(Object key) {
		checkOpen(true);
		checkReadOnly();
		if (key == null) {
			return;
		}
		theCache.remove(key.toString());
	}

	@Override
	public void deleteAll() {
		deleteAll(false);
	}

	private void deleteAll(boolean readOnlyOk) {
		if (!readOnlyOk) {
			checkReadOnly();
		}
		if (theCache != null) {
			theCache.clear();
		}
	}

	@Override
	public void destroy() {
		deleteAll(true);
		try {
			cacheManager.destroyCache(cacheName);
			cacheManager.close();
		} catch (CachePersistenceException cpe) {
			throw new RuntimeException("Failed to destroy cache", cpe);
		}
		isOpen = false;
	}

	@Override
	public void flush() {
		checkOpen(true);
		checkReadOnly();
	}

	@Override
	public Iterator<Map<String, Object>> iterator(Object key) {
		checkOpen(true);
		if (key == null) {
			return null;
		}
		if (key instanceof Iterable<?>) {
			List<Map<String, Object>> vals = new ArrayList<>();
			Iterator<?> iter = ((Iterable<?>) key).iterator();
			while (iter.hasNext()) {
				List<Map<String, Object>> val = theCache.get(iter.next().toString()).getValues();
				if (val != null) {
					vals.addAll(val);
				}
			}
			if (vals.size() == 0) {
				return null;
			}
			return vals.iterator();
		}
		EhCacheEntry cacheEntry = theCache.get(key.toString());
		if (cacheEntry == null) {
			return null;
		}
		return cacheEntry.getValues().iterator();
	}

	@Override
	public Iterator<Map<String, Object>> iterator() {
		return new Iterator<Map<String, Object>>() {
			private Iterator<Cache.Entry<String, EhCacheEntry>> theCacheIter;
			private List<Map<String, Object>> currentKeyResult = null;
			private Iterator<Map<String, Object>> currentKeyResultIter = null;

			{
				theCacheIter = theCache.iterator();
			}

			private void initCurrentKeyResult() {
				currentKeyResult = theCacheIter.next().getValue().getValues();
				currentKeyResultIter = currentKeyResult.iterator();
			}

			private void resetCurrentKeyResult() {
				currentKeyResult = null;
				currentKeyResultIter = null;
			}

			@Override
			public boolean hasNext() {
				if (currentKeyResultIter != null) {
					if (currentKeyResultIter.hasNext()) {
						return true;
					} else {
						resetCurrentKeyResult();
					}
				}

				if (theCacheIter.hasNext()) {
					initCurrentKeyResult();
					if (currentKeyResultIter.hasNext()) {
						return true;
					}
				}
				return false;
			}

			@Override
			public Map<String, Object> next() {
				if (currentKeyResultIter != null) {
					if (currentKeyResultIter.hasNext()) {
						return currentKeyResultIter.next();
					} else {
						resetCurrentKeyResult();
					}
				}

				if (theCacheIter.hasNext()) {
					initCurrentKeyResult();
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
	public void open(Context context) {
		checkOpen(false);
		isOpen = true;
		baseLocation = CachePropertyUtil.getAttributeValueAsString(context, DIHCachePersistProperties.CACHE_BASE_DIRECTORY);
		if (baseLocation == null) {
			baseLocation = System.getProperty("java.io.tmpdir");
		}
		cacheName = CachePropertyUtil.getAttributeValueAsString(context, DIHCachePersistProperties.CACHE_NAME);
		if (cacheName == null) {
			cacheName = "EhBackedCache-" + System.currentTimeMillis();
		}

		maxCacheMemSize = 1000L;
		String maxSize = CachePropertyUtil.getAttributeValueAsString(context, DIHCachePersistProperties.EXPIRE_STORE_SIZE);
		if (maxSize != null) {
			maxCacheMemSize = Long.parseLong(maxSize);
		}

		cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
				.with(CacheManagerBuilder.persistence(new File(baseLocation, cacheName)))
				.withCache(cacheName,
						CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, EhCacheEntry.class,
								ResourcePoolsBuilder.newResourcePoolsBuilder()
										.heap(maxCacheMemSize, EntryUnit.ENTRIES)
										//.offheap()
										.disk(3, MemoryUnit.GB, true)
						)
				).build(true);


		theCache = cacheManager.getCache(cacheName, String.class, EhCacheEntry.class);

		String pkName = CachePropertyUtil.getAttributeValueAsString(context, DIHCacheSupport.CACHE_PRIMARY_KEY);
		if (pkName != null) {
			primaryKeyName = pkName;
		}
		isReadOnly = false;
		String readOnlyStr = CachePropertyUtil.getAttributeValueAsString(context, DIHCacheSupport.CACHE_READ_ONLY);
		if ("true".equalsIgnoreCase(readOnlyStr)) {
			isReadOnly = true;
		}
	}
}
