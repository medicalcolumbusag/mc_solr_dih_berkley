package de.medicalcolumbus.platform.solr.dih;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.Map;

import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.dataimport.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DIHCacheProcessor extends EntityProcessorBase {
	private static final Logger LOG = LoggerFactory
			.getLogger(DIHCacheProcessor.class);

	private DIHCache cache = null;
	private String cacheFk = null;
	private Object lastFkValue = null;
	private Iterator<Map<String, Object>> fullCacheIterator = null;
	private Iterator<Map<String, Object>> lastFkIterator = null;

	public void destroy() {
		cache.close();
		cache = null;
		lastFkValue = null;
		lastFkIterator = null;
	}

	@SuppressWarnings("unchecked")
	public void init(Context context) {
		if (cache != null) {
			return;
		}
		super.init(context);

		cacheFk = context
				.getResolvedEntityAttribute(DIHCachePersistProperties.CACHE_FOREIGN_KEY);
		String cacheName = context
				.getResolvedEntityAttribute(DIHCachePersistProperties.CACHE_NAME);
		try {
			String partitionNumberStr = context
					.getResolvedEntityAttribute(DIHCachePersistProperties.PARTITION_NUMBER);
			int partitionNumber = Integer.parseInt(partitionNumberStr);
			if (partitionNumber >= 0) {
				cacheName = cacheName + "-part" + partitionNumber;
			}
		} catch (Exception e) {
			// do nothing.
		}
		context.setSessionAttribute(DIHCachePersistProperties.CACHE_NAME, cacheName,
				Context.SCOPE_ENTITY);
		context.setSessionAttribute(DIHCachePersistProperties.CACHE_READ_ONLY, "true",
				Context.SCOPE_ENTITY);
		context.setSessionAttribute(DIHCachePersistProperties.CACHE_DELETE_PRIOR_DATA,
				"false", Context.SCOPE_ENTITY);
		String cacheImplStr = context
				.getResolvedEntityAttribute(DIHCachePersistProperties.CACHE_IMPL);
		if (cacheImplStr == null) {
			throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
					"No Persistent Cache Implementation Specified:" + cacheImplStr);
		}

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

		LOG.info("Accessing cache of type " + cacheImplStr + " for Entity "
				+ context.getEntityAttribute("name"));
	}

	public Map<String, Object> nextRow() {
		if (cacheFk == null) {
			if (fullCacheIterator == null) {
				fullCacheIterator = cache.iterator();
			}
			if (fullCacheIterator.hasNext()) {
				return fullCacheIterator.next();
			}
			return null;
		}

		Object key = context.resolve(cacheFk);
		if (key == null) {
			lastFkValue = null;
			lastFkIterator = null;
			return null;
		}
		if (!key.equals(lastFkValue)) {
			lastFkValue = key;
			Iterator<Map<String, Object>> valueIter = cache.iterator(key);
			if (valueIter != null) {
				lastFkIterator = valueIter;
			}
		}
		if (lastFkIterator != null && lastFkIterator.hasNext()) {
			return lastFkIterator.next();
		}
		lastFkValue = null;
		lastFkIterator = null;
		return null;
	}
}