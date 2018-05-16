package de.medicalcolumbus.platform.solr.dih;

public final class DIHCachePersistProperties {

	private DIHCachePersistProperties() {
	}

	/**
	 * <p>
	 * Specify the Cache Implementation to use
	 * with {@link DIHCacheWriter} or {@link DIHCacheProcessor}.
	 */
	public static final String CACHE_IMPL = "persistCacheImpl";
	/**
	 * <p>
	 * Specify the path to where the cache is to be saved on disk (if applicable)
	 */
	public static final String CACHE_BASE_DIRECTORY = "persistCacheBaseDir";
	/**
	 * <p>
	 * Specify the Foreign Key from the parent entity to join on. Use if the cache
	 * is on a child entity.
	 */
	public static final String CACHE_FOREIGN_KEY = "persistCacheLookup";
	/**
	 * <p>
	 * Specify the Primary Key field from this Entity to map the input records
	 * with
	 */
	public static final String CACHE_PRIMARY_KEY = "persistCachePk";
	/**
	 * <p>
	 * The cache name. This should be unique across all cached entities, or
	 * undefined behavior may result.
	 */
	public static final String CACHE_NAME = "persistCacheName";
	/**
	 * <p>
	 * If the cache supports persistent data, set to "true" to delete any prior
	 * persisted data before running the entity.
	 */
	public static final String CACHE_DELETE_PRIOR_DATA = "persistCacheDeletePriorData";
	/**
	 * <p>
	 * If true, a pre-existing cache is re-opened for read-only access.
	 */
	public static final String CACHE_READ_ONLY = "persistCacheReadOnly";
	/**
	 * <p>
	 * To be used with the DIHCacheWriter. If set &gt;1, this specifies the number of
	 * cache partitions to create.
	 * <p>
	 * Caches are numbered beginning at 0. Each Document is placed in a Partition
	 * based on: (Primary_Key.hashCode() % #Partitions)
	 * <p>
	 * There are two possible use cases:
	 * <ul>
	 * <li>
	 * Cache the data into partitions, then index each partition to a separate
	 * Solr Shard.</li>
	 * <li>
	 * Cache the data into partitions, then index each partition in parallel to
	 * the same Shard so as to take advantage of a multi-processor indexing
	 * configuration</li>
	 * </ul>
	 * <p>
	 * Note: This may not be supported by all cache implementations
	 */
	public static final String CACHE_PARTITIONS = "persistCachePartitions";
	/**
	 * <p>
	 * To be used with the DIHCacheProcessor. If set, specifies the cache number
	 * to access. See CACHE_PARTITIONS for information on creating partitions.
	 */
	public static final String PARTITION_NUMBER = "persistCachePartitionNumber";
	/**
	 * <p>
	 * For use with the DIHCacheWriter.
	 * <p>
	 * If the cache supports duplicate keys, setting this to "true" disables this
	 * ability for new additions. Specifically, this causes calls to
	 * "DIHWriter.add(key)" to first delete any existing records with the
	 * passed-in key, then add.
	 * <p>
	 * This is helpful:
	 * <ul>
	 * <li>
	 * If you are adding from a document that contains duplicate key rows, but
	 * desire only the last added document for each key to remain in the cache.</li>
	 * <li>
	 * If you are doing a delta update to the DIHCacheWriter using DIH parameters:
	 * command=full-import&amp;clean=false, this prevents the DIH Cache from having
	 * to remember all re-written keys in memory. See
	 * CACHE_DELTA_WITH_FULL_UPDATE_NO_CLEAN.</li>
	 * </ul>
	 * <p>
	 * Default is "false".
	 */
	public static final String CACHE_NO_DUPLICATE_KEYS = "persistCacheNoDuplicateKeys";
	/**
	 * <p>
	 * Setting this to "true" allows the cache to perform certain optimizations if
	 * calls to "add(key)" will be done in key-order. This is helpful when doing a
	 * delta update to the DIHCacheWriter using DIH parameters:
	 * command=full-Import&amp;clean=false, and if CACHE_NO_DUPLICATE_KEYS cannot be
	 * specified (because duplicates are required by the data). If the keys can be
	 * guaranteed to arrive in key-order, the cache will not need to remember all
	 * re-written keys in memory.
	 * <p>
	 * Default is "false".
	 */
	public static final String CACHE_ADDS_ARRIVE_IN_KEY_ORDER = "persistCacheAddsArriveInKeyOrder";
	/**
	 * <p>
	 * For use with DIHCacheWriter and a Cache Implementation that writes
	 * duplicate keys by default and doing a delta update with DIH parameters:
	 * command=full-import&amp;clean=false.
	 * <p>
	 * Setting this to "true" instructs the cache to remember each re-written key
	 * in memory such that the first cache call to "add(key)" for a particular key
	 * will result in an "delete-then-add" operation. Then, each subsequent call
	 * to "add(key)" for a particular key will result in an "add only" operation.
	 * Failure to set this will result in the cache containing both the old and
	 * new data.
	 */
	public static final String CACHE_DELTA_WITH_FULL_UPDATE_NO_CLEAN = "persistCacheDeltaWithFullUpdateNoClean";
	/**
	 * <p>
	 * A comma-delimited list of field names. <b>For some implementations, It is
	 * necessary to specify this parameter if this is to be a newly-created cache
	 * and the first row (or document) does not contain a non-null entry for every
	 * field</b>
	 */
	public static final String FIELD_NAMES = "persistCacheFieldNames";
	/**
	 * <p>
	 * A comma-delimited list of field types. This should be specified with
	 * fieldNames. See org.apache.solr.handler.dataimport.DIHCacheTypes for a list
	 * of valid types
	 */
	public static final String FIELD_TYPES = "persistCacheFieldTypes";

	/**
	 * EhCache and MapDbCache specific value. Defines maximum elements number stored in memory (heap)
	 * before it overflows to disk.
	 */
	public static final String EXPIRE_ELEMENT_MAX_SIZE = "expireElementMaxSize";

	/**
	 * MapDb specific value. Defines number of threads used to write data from RAM to disk.
	 * Defaults to 1.
	 */
	public static final String RAM_TO_DISK_THREAD_NUMBER = "ramToDiskThreadNumber";


	/**
	 * EHCache specific value. Defines how much disk space in MB can be used by the cache.
	 * Defaults to 1000 MB.
	 */
	public static final String DISK_MAX_SIZE = "diskMaxSize";

	/**
	 * EHCache specific value. Defines how much memory in MB should be used as off Heap cache space.
	 * When not specified, off heap space is not used. Providing a value might boost performance.
	 * Note that it must have a value smaller then 'diskMaxSize' to permit overflow to disk.
	 */
	public static final String RAM_MAX_SIZE = "ramMaxSize";

	/**
	 * MapDBCache specific value. Defines how many seconds an element should persist in RAM after get.
	 * Defaults to 5 seconds
	 */
	public static final String EXPIRE_FROM_RAM = "expireFromRam";

	/**
	 * EHCache specific value - defines a delay in seconds for cleaning up cache files. Defaults to 10 sec.
	 * If you spot cache files are not removed you might need to increase this value
	 */
	public static final String DESTROY_DELAY_SECONDS = "destroyDelayInSeconds";

}
