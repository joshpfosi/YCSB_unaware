// Jacob Leverich <leverich@stanford.edu>, 2011
// Memcached client for YCSB framework.
//

package com.yahoo.ycsb.db;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ExecutionException;

import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.StringByteIterator;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;

public class Memcached extends com.yahoo.ycsb.DB
{
  MemcachedClient client;

  public static final String PORT_PROPERTY    = "memcached.port";
  public static final String SERVERS_PROPERTY = "memcached.servers";
  public static final String CONNS_PROPERTY   = "memcached.connsPerServer";
  public static final String THREADS_PROPERTY = "memcached.numThreads";

  /**
   * Initialize any state for this DB.  Called once per DB instance;
   * there is one DB instance per client thread.
   */
  public void init() throws DBException {
    Properties props = getProperties();

    int port = 8888, connsPerServer = 1, numThreads = 4;

    String portString           = props.getProperty(PORT_PROPERTY);
    String serverString         = props.getProperty(SERVERS_PROPERTY);
    String connsPerServerString = props.getProperty(CONNS_PROPERTY);
    String numThreadsString     = props.getProperty(THREADS_PROPERTY);

    String[] servers = serverString.split(" ");

    if (portString != null) {
      port = Integer.parseInt(portString);
    }

    if (connsPerServerString != null) {
      connsPerServer = Integer.parseInt(connsPerServerString);
    }

    if (numThreadsString != null) {
      numThreads = Integer.parseInt(numThreadsString);
    }

    try {
      List<InetSocketAddress> addrs = new ArrayList<InetSocketAddress>();

      // NOTE: Order of list is important here -- It must be
      // [ mem0, mem1, mem2, mem3, mem0, mem1, mem2, mem3, ... ]
      // not [ mem0, mem0, mem1, mem1, ... ]
      for (int i = 0; i < connsPerServer; i++) {
        for (int j = 0; j < numThreads; j++) {
          for (String server : servers) {
            addrs.add(new InetSocketAddress(server, port));
          }
        }
      }

      client = new MemcachedClient(addrs);
    } catch (IOException e) { throw new DBException(e); }
  }

  /**
   * Cleanup any state for this DB.  Called once per DB instance;
   * there is one DB instance per client thread.
   */
  public void cleanup() throws DBException
  {
    if (/* client.isAlive() */ true) client.shutdown();
  }


  
  /**
   * Read a record from the database. Each field/value pair from the
   * result will be stored in a HashMap.
   *
   * @param table The name of the table
   * @param key The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error or "not found".
   */
  @Override
  public Status read(String table, String key, Set<String> fields,
                  HashMap<String,ByteIterator> result) {
    HashMap<String, byte[]> values = 
      (HashMap<String, byte[]>) client.get(table + ":" + key);

    if (values == null) return Status.NOT_FOUND;
    if (values.keySet().isEmpty()) return Status.NOT_FOUND;
    if (fields == null) fields = values.keySet();

    for (String k: fields) {
      byte[] v = values.get(k);
      if (v == null) return Status.NOT_FOUND;
      result.put(k, new ByteArrayByteIterator(v));
    }

    return Status.OK;
  }

  /**
   * Perform a range scan for a set of records in the database. Each
   * field/value pair from the result will be stored in a HashMap.
   *
   * @param table The name of the table
   * @param startkey The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields The list of fields to read, or null for all of them
   * @param result A Vector of HashMaps, where each HashMap is a set
   * field/value pairs for one record
   * @return Zero on success, a non-zero error code on error.  See
   * this class's description for a discussion of error codes.
   */
  @Override
  public Status scan(String table, String startkey, int recordcount,
                  Set<String> fields,
                  Vector<HashMap<String,ByteIterator>> result) {
    return Status.ERROR;
  }
  
  /**
   * Update a record in the database. Any field/value pairs in the
   * specified values HashMap will be written into the record with the
   * specified record key, overwriting any existing values with the
   * same field name.
   *
   * @param table The name of the table
   * @param key The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error.  See
   * this class's description for a discussion of error codes.
   */
  @Override
  public Status update(String table, String key,
                    HashMap<String,ByteIterator> values) {
    HashMap<String, byte[]> new_values = new HashMap<String, byte[]>();

    for (String k: values.keySet()) {
      new_values.put(k, values.get(k).toArray());
    }

    OperationFuture<Boolean> f =
      client.set(table + ":" + key, 3600, new_values);

    try { return f.get() ? Status.OK : Status.ERROR; }
    catch (InterruptedException e) { return Status.ERROR; }
    catch (ExecutionException e) { return Status.ERROR; }
  }

  /**
   * Insert a record in the database. Any field/value pairs in the
   * specified values HashMap will be written into the record with the
   * specified record key.
   *
   * @param table The name of the table
   * @param key The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the
   * record
   * @return Zero on success, a non-zero error code on error.  See
   * this class's description for a discussion of error codes.
   */
  @Override
  public Status insert(String table, String key,
                    HashMap<String,ByteIterator> values) {
    return update(table, key, values);
  }

  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error.  See
   * this class's description for a discussion of error codes.
   */
  @Override
  public Status delete(String table, String key) {
    client.delete(table + ":" + key);
    return Status.OK; // FIXME check future
  }
}
