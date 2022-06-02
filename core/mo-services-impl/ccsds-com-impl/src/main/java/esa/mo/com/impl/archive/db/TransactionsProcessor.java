/* ----------------------------------------------------------------------------
 * Copyright (C) 2021      European Space Agency
 *                         European Space Operations Centre
 *                         Darmstadt
 *                         Germany
 * ----------------------------------------------------------------------------
 * System                : ESA NanoSat MO Framework
 * ----------------------------------------------------------------------------
 * Licensed under European Space Agency Public License (ESA-PL) Weak Copyleft – v2.4
 * You may not use this file except in compliance with the License.
 *
 * Except as expressly set forth in this License, the Software is provided to
 * You on an "as is" basis and without warranties of any kind, including without
 * limitation merchantability, fitness for a particular purpose, absence of
 * defects or errors, accuracy or non-infringement of intellectual property rights.
 * 
 * See the License for the specific language governing permissions and
 * limitations under the License. 
 * ----------------------------------------------------------------------------
 */
package esa.mo.com.impl.archive.db;

import esa.mo.com.impl.archive.entities.COMObjectEntity;
import esa.mo.com.impl.provider.ArchiveManager;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.ccsds.moims.mo.com.archive.structures.ArchiveQuery;
import org.ccsds.moims.mo.com.archive.structures.QueryFilter;
import org.ccsds.moims.mo.mal.structures.IntegerList;
import org.ccsds.moims.mo.mal.structures.LongList;

/**
 * The Transactions Processor is responsible for executing the transactions with
 * the database. It has one general executor with 2 threads for flushing the
 * generated COM Events and a single thread executor for orderly executing the
 * interactions with the database. It includes an optimization for batch stores,
 * basically, if many stores are received sequentially, they will be
 * consolidated and executed in one single transaction.
 */
public class TransactionsProcessor {
    public static Logger LOGGER = Logger.getLogger(TransactionsProcessor.class.getName());
    final DatabaseBackend dbBackend;

    // This executor is responsible for the interactions with the db
    // Guarantees sequential order
    private final ExecutorService dbTransactionsExecutor = Executors.newSingleThreadExecutor(
            new DBThreadFactory("Archive_DBTransactionsProcessor"));

    // This executor is expecting "short-lived" runnables that generate Events.
    // 2 Threads minimum because we need to acquire the lock from 2 different tasks during startup
    private final ExecutorService generalExecutor = Executors.newFixedThreadPool(2,
            new DBThreadFactory("Archive_GeneralProcessor"));
    private final AtomicBoolean sequencialStoring;

    private final LinkedBlockingQueue<StoreCOMObjectsContainer> storeQueue;

    public TransactionsProcessor(DatabaseBackend dbBackend) {
        this.dbBackend = dbBackend;
        this.storeQueue = new LinkedBlockingQueue<>();
        this.sequencialStoring = new AtomicBoolean(false);
    }

    public void submitExternalTask(final Runnable task) {
        this.sequencialStoring.set(false); // Sequential stores can no longer happen otherwise we break order

        generalExecutor.execute(task);
    }

    public void submitExternalTask2(final Runnable task) {
        this.sequencialStoring.set(false); // Sequential stores can no longer happen otherwise we break order

        dbTransactionsExecutor.execute(task);
    }

  /**
   * Resizes the database file to match its contents.
   */
  public void vacuum() {
    try {
      Connection c = dbBackend.getConnection();
      Statement stmt;
      LOGGER.log(Level.FINE, "Vacuuming database");
      stmt = c.createStatement();
      stmt.executeUpdate("VACUUM");
    } catch (SQLException ex) {
      LOGGER.log(Level.SEVERE,
          "Failed to vacuum database", ex);
    }
  }

    public COMObjectEntity getCOMObject(final Integer objTypeId, final Integer domainId,
            final Long objId) {
        this.sequencialStoring.set(false); // Sequential stores can no longer happen otherwise we break order

        Future<COMObjectEntity> future = dbTransactionsExecutor.submit(new CallableGetCOMObject(this, domainId, objId, objTypeId));

        try {
            return future.get();
        } catch (InterruptedException ex) {
            LOGGER.log(Level.SEVERE, null, ex);
        } catch (ExecutionException ex) {
            LOGGER.log(Level.SEVERE, null, ex);
        }

        return null;
    }

    public boolean existsCOMObject(final Integer objTypeId, final Integer domain, final Long objId) {
        return (getCOMObject(objTypeId, domain, objId) != null);
    }

    public List<COMObjectEntity> getCOMObjects(final Integer objTypeId, final Integer domainId, final LongList ids) {
        this.sequencialStoring.set(false); // Sequential stores can no longer happen otherwise we break order

        Future<List<COMObjectEntity>> future = dbTransactionsExecutor.submit(new CallableGetCOMObjects(this, ids, domainId, objTypeId));

        try {
            return future.get();
        } catch (InterruptedException | ExecutionException ex) {
            LOGGER.log(Level.SEVERE, null, ex);
        }

        return null;
    }

    public List<COMObjectEntity> getAllCOMObjects(final Integer objTypeId, final Integer domainId) {
        this.sequencialStoring.set(false); // Sequential stores can no longer happen otherwise we break order

        IntegerList types = new IntegerList();
        types.add(objTypeId);
        IntegerList domains = new IntegerList();
        domains.add(domainId);
        ArchiveQuery archiveQuery = new ArchiveQuery(null, null, null, 0L, null, null, null, null, null);
        CallableQuery query = new CallableQuery(this, types, archiveQuery, domains, null, null, null, null);
        Future<Object> future = dbTransactionsExecutor.submit(query);

        try {
            return (List<COMObjectEntity>)future.get();
        } catch (InterruptedException | ExecutionException ex) {
            LOGGER.log(Level.SEVERE, null, ex);
        }

        return null;
    }

    public LongList getAllCOMObjectsIds(final Integer objTypeId, final Integer domainId) {
        this.sequencialStoring.set(false); // Sequential stores can no longer happen otherwise we break order

        Future<LongList> future = dbTransactionsExecutor.submit(new CallableGetAllCOMObjects(this, domainId, objTypeId));

        try {
            return future.get();
        } catch (InterruptedException | ExecutionException ex) {
            LOGGER.log(Level.SEVERE, null, ex);
        }

        return null;
    }

    private void persistObjects(final ArrayList<COMObjectEntity> perObjs) {
        Connection c = dbBackend.getConnection();
        String stm = "INSERT INTO COMObjectEntity (objectTypeId, objId, domainId, network, objBody, providerURI, relatedLink, sourceLinkDomainId, sourceLinkObjId, sourceLinkObjectTypeId, timestampArchiveDetails) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try {
            c.setAutoCommit(false);
            PreparedStatement getCOMObject = c.prepareStatement(stm);

            for (int i = 0; i < perObjs.size(); i++) { // 6.510 ms per cycle
                COMObjectEntity obj = perObjs.get(i);
                getCOMObject.setObject(1, obj.getObjectTypeId());
                getCOMObject.setObject(2, obj.getObjectId());
                getCOMObject.setObject(3, obj.getDomainId());
                getCOMObject.setObject(4, obj.getNetwork());
                getCOMObject.setObject(5, obj.getObjectEncoded());
                getCOMObject.setObject(6, obj.getProviderURI());
                getCOMObject.setObject(7, obj.getRelatedLink());
                getCOMObject.setObject(8, obj.getSourceLink().getDomainId());
                getCOMObject.setObject(9, obj.getSourceLink().getObjId());
                getCOMObject.setObject(10, obj.getSourceLink().getObjectTypeId());
                getCOMObject.setObject(11, obj.getTimestamp().getValue());
                getCOMObject.addBatch();

                // Flush every 1k objects...
                if (i != 0) {
                    if ((i % 1000) == 0) {
                        LOGGER.log(Level.FINE,
                                "Flushing the data after 1000 serial stores...");

                        getCOMObject.executeBatch();
                        getCOMObject.clearBatch();
                    }
                }
            }

            getCOMObject.executeBatch();
            c.setAutoCommit(true);
        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, null, ex);
        }
    }

    public void insert(final ArrayList<COMObjectEntity> perObjs, final Runnable publishEvents) {
        final boolean isSequential = this.sequencialStoring.get();
        final StoreCOMObjectsContainer container = new StoreCOMObjectsContainer(perObjs, isSequential);

        this.sequencialStoring.set(true);

        try { // Insert into queue
            storeQueue.put(container);
        } catch (InterruptedException ex) {
            Logger.getLogger(ArchiveManager.class.getName()).log(Level.SEVERE,
                    "Something went wrong...", ex);
        }

        dbTransactionsExecutor.execute(new Runnable() {
            @Override
            public void run() {
                StoreCOMObjectsContainer container = storeQueue.poll();
                if (container != null) {
                    try {
                        dbBackend.getAvailability().acquire();
                    } catch (InterruptedException ex) {
                        LOGGER.log(Level.SEVERE, null, ex);
                    }

                    ArrayList<COMObjectEntity> objs = new ArrayList<>();
                    objs.addAll(container.getPerObjs());

                    while (true) {
                        container = storeQueue.peek(); // get next if there is one available

                        if (container == null || !container.isContinuous()) {
                            break;
                        }

                        objs.addAll(storeQueue.poll().getPerObjs());
                    }

                    persistObjects(objs); // store
                    dbBackend.getAvailability().release();
                }

                if(publishEvents != null) {
                  generalExecutor.submit(publishEvents);
                }
            }
        });
    }

    public void remove(final Integer objTypeId, final Integer domainId,
            final LongList objIds, final Runnable publishEvents) {
        this.sequencialStoring.set(false); // Sequential stores can no longer happen otherwise we break order

        dbTransactionsExecutor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    dbBackend.getAvailability().acquire();
                } catch (InterruptedException ex) {
                    LOGGER.log(Level.SEVERE, null, ex);
                }

                try {
                    Connection c = dbBackend.getConnection();
                    c.setAutoCommit(false);

                    String stm = "DELETE FROM COMObjectEntity WHERE (((objectTypeId = ?) AND (domainId = ?) AND (objId = ?)))";
                    PreparedStatement delete = c.prepareStatement(stm);

                    // Generate the object Ids if needed and the persistence objects to be removed
                    for (int i = 0; i < objIds.size(); i++) {
                        delete.setInt(1, objTypeId);
                        delete.setInt(2, domainId);
                        delete.setLong(3, objIds.get(i));
                        delete.addBatch();

                        // Flush every 1k objects...
                        if (i != 0) {
                            if ((i % 1000) == 0) {
                                LOGGER.log(Level.FINE,
                                        "Flushing the data after 1000 serial stores...");

                                delete.executeBatch();
                                delete.clearBatch();
                            }
                        }
                    }

                    delete.executeBatch();
                    c.setAutoCommit(true);
                } catch (SQLException ex) {
                    LOGGER.log(Level.SEVERE, null, ex);
                }

                dbBackend.getAvailability().release();
              if(publishEvents != null) {
                generalExecutor.submit(publishEvents);
              }
                vacuum();
            }
        });
    }

    public void update(final ArrayList<COMObjectEntity> newObjs, final Runnable publishEvents) {
        this.sequencialStoring.set(false); // Sequential stores can no longer happen otherwise we break order

        dbTransactionsExecutor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    dbBackend.getAvailability().acquire();
                } catch (InterruptedException ex) {
                    LOGGER.log(Level.SEVERE, null, ex);
                }

                try {
                    Connection c = dbBackend.getConnection();
                    c.setAutoCommit(false);

                    StringBuilder stm = new StringBuilder();
                    stm.append("UPDATE COMObjectEntity ");
                    stm.append("SET objectTypeId = ?, objId = ?, domainId = ?, network = ?, objBody = ?, ");
                    stm.append("providerURI = ?, relatedLink = ?, sourceLinkDomainId = ?, ");
                    stm.append("sourceLinkObjId = ?, sourceLinkObjectTypeId = ?, timestampArchiveDetails = ? ");
                    stm.append("WHERE (((objectTypeId = ?) AND (domainId = ?) AND (objId = ?)));");

                    PreparedStatement update = c.prepareStatement(stm.toString());

                    // Generate the object Ids if needed and the persistence objects to be removed
                    for (int i = 0; i < newObjs.size(); i++) {
                        COMObjectEntity obj = newObjs.get(i);
                        update.setObject(1, obj.getObjectTypeId());
                        update.setObject(2, obj.getObjectId());
                        update.setObject(3, obj.getDomainId());
                        update.setObject(4, obj.getNetwork());
                        update.setObject(5, obj.getObjectEncoded());
                        update.setObject(6, obj.getProviderURI());
                        update.setObject(7, obj.getRelatedLink());
                        update.setObject(8, obj.getSourceLink().getDomainId());
                        update.setObject(9, obj.getSourceLink().getObjId());
                        update.setObject(10, obj.getSourceLink().getObjectTypeId());
                        update.setObject(11, obj.getTimestamp().getValue());

                        update.setObject(12, newObjs.get(i).getObjectTypeId());
                        update.setObject(13, newObjs.get(i).getDomainId());
                        update.setObject(14, newObjs.get(i).getObjectId());
                        update.addBatch();

                        // Flush every 1k objects...
                        if (i != 0) {
                            if ((i % 1000) == 0) {
                                LOGGER.log(Level.FINE,
                                        "Flushing the data after 1000 serial stores...");

                                update.executeBatch();
                                update.clearBatch();
                            }
                        }
                    }

                    update.executeBatch();
                    c.setAutoCommit(true);
                } catch (SQLException ex) {
                    LOGGER.log(Level.SEVERE, null, ex);
                }

                dbBackend.getAvailability().release();

              if(publishEvents != null) {
                generalExecutor.submit(publishEvents);
              }
            }
        });
    }

  enum QueryType {
    SELECT("SELECT"), DELETE("DELETE");
    private final String queryPrefix;
    private QueryType(String queryPrefix) {
      this.queryPrefix = queryPrefix;
    }
    public String getQueryPrefix() {
      return queryPrefix;
    }
  }
  public static String generateQueryStringFromLists(final String field, final IntegerList list) {
    if (list.isEmpty()) {
      return "";
    }

    if (list.size() == 1) {
      return field + "=" + list.get(0) + " AND ";
    }

    StringBuilder stringForWildcards = new StringBuilder("(");

    for (Integer id : list) {
      stringForWildcards.append(field).append("=").append(id).append(" OR ");
    }

    // Remove the " OR " par of it!
    stringForWildcards = new StringBuilder(stringForWildcards.substring(0, stringForWildcards.length() - 4));

    return stringForWildcards + ") AND ";
  }

  public ArrayList<COMObjectEntity> query(final IntegerList objTypeIds,
      final ArchiveQuery archiveQuery, final IntegerList domainIds,
      final Integer providerURIId, final Integer networkId,
      final SourceLinkContainer sourceLink, final QueryFilter filter) {
    this.sequencialStoring.set(false); // Sequential stores can no longer happen otherwise we break order
    final CallableQuery task = new CallableQuery(this, objTypeIds, archiveQuery,
        domainIds, providerURIId, networkId, sourceLink, filter);

    Future<Object> future = dbTransactionsExecutor.submit(task);

    try {
      return (ArrayList<COMObjectEntity>)future.get();
    } catch (InterruptedException | ExecutionException ex) {
      LOGGER.log(Level.SEVERE, null, ex);
    }

    return null;
  }

  public int delete(final IntegerList objTypeIds,
      final ArchiveQuery archiveQuery, final IntegerList domainIds,
      final Integer providerURIId, final Integer networkId,
      final SourceLinkContainer sourceLink, final QueryFilter filter) {
    this.sequencialStoring.set(false); // Sequential stores can no longer happen otherwise we break order
    final CallableQuery task = new CallableQuery(this, objTypeIds, archiveQuery,
        domainIds, providerURIId, networkId, sourceLink, filter, QueryType.DELETE);

    Future<Object> future = dbTransactionsExecutor.submit(task);

    try {
      return (Integer)future.get();
    } catch (InterruptedException | ExecutionException ex) {
      LOGGER.log(Level.SEVERE, null, ex);
    }

    return 0;
  }

  public void resetMainTable(final Callable task) {
    this.sequencialStoring.set(false); // Sequential stores can no longer happen otherwise we break order
    Future<Integer> nullValue = dbTransactionsExecutor.submit(task);
    LOGGER.info("Reset table submitted!");

    try {
      Integer dummyInt = nullValue.get(); // Dummy code to Force a wait until the actual restart is done!
      LOGGER.info("Reset table callback!");
    } catch (InterruptedException | ExecutionException ex) {
      LOGGER.log(Level.SEVERE, null, ex);
    }
  }

  public void stopInteractions(final Callable task) {
    this.sequencialStoring.set(false); // Sequential stores can no longer happen otherwise we break order
    Future<Integer> nullValue = dbTransactionsExecutor.submit(task);

    try {
      nullValue.get(); // Dummy code to Force a wait until the actual restart is done!
    } catch (InterruptedException | ExecutionException ex) {
      LOGGER.log(Level.SEVERE, null, ex);
    }
  }

  /**
   * The database backend thread factory
   */
  static class DBThreadFactory implements ThreadFactory {

    private final ThreadGroup group;
    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final String namePrefix;

    DBThreadFactory(String prefix) {
      SecurityManager s = System.getSecurityManager();
      group = (s != null) ? s.getThreadGroup()
          : Thread.currentThread().getThreadGroup();
      namePrefix = prefix + "-thread-";
    }

    @Override
    public Thread newThread(Runnable r) {
      Thread t = new Thread(group, r,
          namePrefix + threadNumber.getAndIncrement(),
          0);
      if (t.isDaemon()) {
        t.setDaemon(false);
      }
      if (t.getPriority() != Thread.NORM_PRIORITY) {
        t.setPriority(Thread.NORM_PRIORITY);
      }
      return t;
    }
  }

    public static Long convert2Long(final Object obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof Long) {
            return (Long) obj;
        }

        if (obj instanceof Integer) {
            return ((Integer) obj).longValue();
        }

        return null;
    }

}
