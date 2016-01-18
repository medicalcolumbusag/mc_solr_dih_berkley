# mc_solr_dih_berkley

Solr DIH that uses Berkeley DB as disk backed cache `http://www.oracle.com/technetwork/database/database-technologies/berkeleydb/overview/index.html`
This is useful for memory improvements, where the entire cached information is not kept in memory.

Implementation is based on the examples from:
`https://issues.apache.org/jira/browse/SOLR-2613` -> `https://issues.apache.org/jira/secure/attachment/12506136/SOLR-2613.patch`
`https://issues.apache.org/jira/browse/SOLR-2382` -> `https://issues.apache.org/jira/secure/attachment/12505376/SOLR-2382-dihwriter_standalone.patch`

## Configuration example:

```
<dataConfig>
	<document>
		<entity 
			name="PARENT"
			...>

			<entity
				name="CHILD"
				dataSource="my_data_source"
				query="SELECT ID, NAME, TYPE FROM MY_TABLE"
				processor="SqlEntityProcessor"
				cacheImpl="de.medicalcolumbus.platform.solr.dih.BerkleyBackedCache"
				persistCacheName="CHILD_CACHE_NAME"
				persistCachePartitionNumber="0"
				cachePk="id"
				cacheLookup="PARENT.id"
				berkleyInternalCacheSize="1000000"
				berkleyInternalShared="true"
				persistCacheBaseDir="\some\disk\location"
			/>
		</entity>
	</document>
</dataConfig>
```