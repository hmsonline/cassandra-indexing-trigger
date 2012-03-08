package com.hmsonline.cassandra.triggers.index;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CassandraServer;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.aspectj.lang.annotation.Aspect;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hmsonline.cassandra.triggers.ColumnOperation;
import com.hmsonline.cassandra.triggers.LogEntry;
import com.hmsonline.cassandra.triggers.Trigger;

@Aspect
public class IndexTrigger implements Trigger {

    private static Logger logger = LoggerFactory.getLogger(IndexTrigger.class);

    private Indexer indexer = null;

    public synchronized Indexer getIndexer(String solrUrl) {
        if (indexer == null) {
            indexer = new SolrIndexer(solrUrl);
        }
        return indexer;
    }

    public Cassandra.Iface getConnection(String keyspace) throws Exception {
        CassandraServer server = new CassandraServer();
        if (keyspace != null) {
            server.set_keyspace(keyspace);
        }
        return server;
    }

    public void process(LogEntry logEntry) {
        String columnFamily = logEntry.getColumnFamily();
        try {
            String rowKey = ByteBufferUtil.string(logEntry.getRowKey());// logEntry.getUuid();

            indexer = getIndexer(System.getProperty("solrHost"));
            logger.debug("solr enabled: " + indexer.toString());
            if (isMarkedForDelete(logEntry)) {
                indexer.delete(columnFamily, rowKey);
            } else {
                JSONObject json = this.getSlice(logEntry.getKeyspace(), logEntry.getColumnFamily(),
                        logEntry.getRowKey(), logEntry.getConsistencyLevel());
                indexer.index(logEntry.getColumnFamily(), ByteBufferUtil.string(logEntry.getRowKey()), json);
            }
        } catch (Exception ex) {
            throw new RuntimeException("Unable to update index", ex);
        }
    }

    @SuppressWarnings("unchecked")
    public JSONObject getSlice(String keyspace, String columnFamily, ByteBuffer key, ConsistencyLevel consistencyLevel)
            throws Exception {
        JSONObject json = null;

        SlicePredicate predicate = new SlicePredicate();
        SliceRange range = new SliceRange(ByteBufferUtil.bytes(""), ByteBufferUtil.bytes(""), false, 1000);
        predicate.setSlice_range(range);
        ColumnParent parent = new ColumnParent(columnFamily);
        List<ColumnOrSuperColumn> slice = this.getConnection(keyspace).get_slice(key, parent, predicate,
                consistencyLevel);

        if (slice.size() > 0) {
            json = new JSONObject();
            for (ColumnOrSuperColumn column : slice) {
                Column col = column.getColumn();
                json.put(new String(col.getName(), "UTF8"), new String(col.getValue(), "UTF8"));
            }
        }
        return json;
    }

    //
    // PRIVATE METHODS
    //
    private boolean isMarkedForDelete(LogEntry logEntry) {
        boolean isDeletedEntry = false;
        for (ColumnOperation operation : logEntry.getOperations()) {
            if (operation.isDelete()) {
                isDeletedEntry = true;
                break;
            }
        }
        return isDeletedEntry;
    }

}
