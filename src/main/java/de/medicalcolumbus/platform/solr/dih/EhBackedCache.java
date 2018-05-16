package de.medicalcolumbus.platform.solr.dih;

import org.apache.solr.handler.dataimport.CachePropertyUtil;
import org.apache.solr.handler.dataimport.Context;
import org.apache.solr.handler.dataimport.DIHCache;
import org.apache.solr.handler.dataimport.DIHCacheSupport;
import org.ehcache.*;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.builders.UserManagedCacheBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.spi.service.LocalPersistenceService;
import org.ehcache.impl.config.persistence.DefaultPersistenceConfiguration;
import org.ehcache.impl.config.persistence.UserManagedPersistenceContext;
import org.ehcache.impl.persistence.DefaultLocalPersistenceService;

import java.io.File;
import java.util.*;

import static de.medicalcolumbus.platform.solr.dih.EhCacheUtil.deleteCacheAsync;
import static java.util.Objects.isNull;

public class EhBackedCache implements DIHCache {

	private PersistentUserManagedCache<String, EhCacheEntry> theCache = null;
	private boolean isOpen = false;
	private boolean isReadOnly = false;
	private String cacheName = null;
	private String primaryKeyName = null;
	private String baseLocation;
	private LocalPersistenceService persistenceService;
	private int destroyDelayInSeconds;

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
		List<Map<String, Object>> thisKeysRecs;
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
		theCache.close();
		persistenceService.stop();
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
		close();
		deleteCacheAsync(theCache, baseLocation, cacheName, destroyDelayInSeconds);
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
			for (Object o : ((Iterable<?>) key)) {
				List<Map<String, Object>> val = theCache.get(o.toString()).getValues();
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
			cacheName = "EhBackedCache_" + System.currentTimeMillis();
		}

		String destroyDelayInSecondsProp = CachePropertyUtil.getAttributeValueAsString(context, DIHCachePersistProperties.DESTROY_DELAY_SECONDS);
		destroyDelayInSeconds = !isNull(destroyDelayInSecondsProp) ? Integer.parseInt(destroyDelayInSecondsProp) : 10;

		long maxCacheMemSize = 1_000L; //entries
		String maxSize = CachePropertyUtil.getAttributeValueAsString(context, DIHCachePersistProperties.EXPIRE_ELEMENT_MAX_SIZE);
		if (maxSize != null) {
			maxCacheMemSize = Long.parseLong(maxSize);
		}

		int diskMaxSize = 1_000; //MB
		String diskMaxSizeProp = CachePropertyUtil.getAttributeValueAsString(context, DIHCachePersistProperties.DISK_MAX_SIZE);
		if (diskMaxSizeProp != null) {
			diskMaxSize = Integer.parseInt(diskMaxSizeProp);
		}

		/*
		   @ramMaxSize must always be smaller than @diskMaxSize
		 *  so it can write to disk
		 */
		int ramMaxSize = 500; // MB
		String ramMaxSizeProp = CachePropertyUtil.getAttributeValueAsString(context, DIHCachePersistProperties.RAM_MAX_SIZE);
		if (ramMaxSizeProp != null) {
			ramMaxSize = Integer.parseInt(ramMaxSizeProp);
		}


		persistenceService = new DefaultLocalPersistenceService(new DefaultPersistenceConfiguration(new File(baseLocation, cacheName)));

		theCache = UserManagedCacheBuilder.newUserManagedCacheBuilder(String.class, EhCacheEntry.class)
				.with(new UserManagedPersistenceContext<>(cacheName, persistenceService))
				.withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder()
						.heap(maxCacheMemSize, EntryUnit.ENTRIES)
						.offheap(ramMaxSize, MemoryUnit.MB)
						.disk(diskMaxSize, MemoryUnit.MB, false))
				.build(true);

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
