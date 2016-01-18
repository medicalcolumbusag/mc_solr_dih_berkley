package de.medicalcolumbus.platform.solr.dih;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.dataimport.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DIHCacheWriter implements DIHWriter {
	private static final Logger LOG = LoggerFactory
			.getLogger(DIHCacheWriter.class);

	private DIHCache[] caches;
	private String keyFieldName;
	private Set<Object> deltaKeys = null;

	public void init(Context context) {
		String cacheImplStr = CachePropertyUtil.getAttributeValueAsString(context,
				DIHCachePersistProperties.CACHE_IMPL);
		if (cacheImplStr == null) {
			throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
					"No Persistent Cache Implementation Specified:" + cacheImplStr);
		}
		keyFieldName = CachePropertyUtil.getAttributeValueAsString(context,
				DIHCachePersistProperties.CACHE_PRIMARY_KEY);
		String numPartStr = CachePropertyUtil.getAttributeValueAsString(context,
				DIHCachePersistProperties.CACHE_PARTITIONS);
		int numParts = 1;
		if (numPartStr != null) {
			try {
				numParts = Integer.parseInt(numPartStr);
			} catch (Exception e) {
				// do nothing
			}
		}
		if (numParts < 1) {
			numParts = 1;
		}
		caches = new DIHCache[numParts];
		for (int i = 0; i < numParts; i++) {
			if (numParts > 1) {
				String name = CachePropertyUtil.getAttributeValue(context,
						DIHCachePersistProperties.CACHE_NAME)
						+ "-part" + i;
				context.setSessionAttribute(DIHCachePersistProperties.CACHE_NAME, name,
						Context.SCOPE_ENTITY);
				caches[i] = instantiateCache(cacheImplStr, context);
				context.setSessionAttribute(DIHCachePersistProperties.CACHE_NAME, null,
						Context.SCOPE_ENTITY);
			} else {
				caches[i] = instantiateCache(cacheImplStr, context);
			}
		}
	}

	@SuppressWarnings("unchecked")
	private DIHCache instantiateCache(String cacheImplStr, Context context) {
		DIHCache cache = null;
		try {

			// Invoke following method through reflection, as this class is not in the same jar / package
			// Class<DIHCache> cacheClass = DocBuilder.loadClass(cacheImplStr, context.getSolrCore());

			Method docBuilderLoadClassMethod = DocBuilder.class.getDeclaredMethod("loadClass", String.class, SolrCore.class);
			docBuilderLoadClassMethod.setAccessible(true); //if security settings allow this
			Class<DIHCache> cacheClass = (Class<DIHCache>) docBuilderLoadClassMethod.invoke(null, cacheImplStr, context.getSolrCore()); //use null if the method is static


			Constructor<DIHCache> constr = cacheClass.getConstructor();
			cache = constr.newInstance();
			cache.open(context);
		} catch (Exception e) {
			throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
					"Unable to load Cache implementation:" + cacheImplStr, e);
		}
		return cache;
	}

	@Override
	public void commit(boolean optimize) {
		for (DIHCache cache : caches) {
			cache.flush();
		}
	}

	@Override
	public void deleteByQuery(String q) {
		throw new UnsupportedOperationException(
				"Delete-by-Query is not supported by the Cache Writer");

	}

	private int getPartitionForKey(Object key) {
		return Math.abs(key.hashCode() % caches.length);

	}

	@Override
	public void deleteDoc(Object key) {
		caches[getPartitionForKey(key)].delete(key);
	}

	@Override
	public void doDeleteAll() {
		for (DIHCache cache : caches) {
			cache.deleteAll();
		}
	}

	@Override
	public void rollback() {
		LOG.warn("The CacheWriter does not support rollback.");
	}

	@Override
	public void close() {
		for (DIHCache cache : caches) {
			cache.close();
		}
	}

	@Override
	public boolean upload(SolrInputDocument doc) {
		Map<String, Object> rec = new HashMap<String, Object>();
		Object keyObj = null;
		for (SolrInputField field : doc) {
			rec.put(field.getName(), field.getValues());
			if (keyFieldName == null) {
				keyFieldName = field.getName();
			}
			if (keyFieldName.equals(field.getName())) {
				keyObj = field.getFirstValue();
			}
		}
		if (keyObj == null) {
			LOG.warn("Tried to add a document with a null key.");
			return false;
		}
		try {
			if (deltaKeys != null && deltaKeys.remove(keyObj)) {
				deleteDoc(keyObj);
			}
			caches[getPartitionForKey(keyObj)].add(rec);
		} catch (Exception e) {
			throw new RuntimeException("Key=" + keyObj + " mod="
					+ getPartitionForKey(keyObj));
		}
		return true;
	}

	@Override
	public void setDeltaKeys(Set<Map<String, Object>> passedInDeltaKeys) {
		deltaKeys = new HashSet<Object>();
		for (Map<String, Object> aMap : passedInDeltaKeys) {
			if (aMap.size() > 0) {
				Object key = null;
				if (keyFieldName != null) {
					key = aMap.get(keyFieldName);
				} else {
					key = aMap.entrySet().iterator().next();
				}
				if (key != null) {
					deltaKeys.add(key);
				}
			}
		}
	}
}
