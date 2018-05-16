package de.medicalcolumbus.platform.solr.dih;

import org.ehcache.CachePersistenceException;
import org.ehcache.PersistentUserManagedCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class EhCacheUtil {

	private static final Logger LOG = LoggerFactory.getLogger(EhBackedCache.class);

	public static void deleteCacheAsync(PersistentUserManagedCache<String, EhCacheEntry> cache, String baseLocation, String cacheName, int delay) {
		ScheduledExecutorService scheduler
				= Executors.newSingleThreadScheduledExecutor();

		Runnable task = () -> {
			try {
				cache.destroy();
			} catch (CachePersistenceException e) {
				LOG.error(e.getMessage());
				throw new RuntimeException(e);
			}
			deleteCacheDirectories(baseLocation, cacheName);
		};

		scheduler.schedule(task, delay, TimeUnit.SECONDS);
		scheduler.shutdown();
	}

	private static void deleteCacheDirectories(String baseLocation, String cacheName) {
		File cacheDirectory = new File(baseLocation + File.separator + cacheName);
		LOG.debug("**********" + cacheDirectory.getName() + "***********");
		if (cacheDirectory.exists() && cacheDirectory.isDirectory()) {
			if (!deleteDirectory(cacheDirectory)) {
				LOG.error("Could not delete directory: " + cacheDirectory);
				throw new RuntimeException("Could not delete directory: " + cacheDirectory);
			}
		}
	}

	private static boolean deleteDirectory(File directoryToBeDeleted) {
		File[] allContents = directoryToBeDeleted.listFiles();
		if (allContents != null) {
			for (File file : allContents) {
				deleteDirectory(file);
			}
		}
		return directoryToBeDeleted.delete();
	}
}
