package esa.mo.com.impl.archive.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import esa.mo.com.impl.archive.entities.COMObjectEntity;

final class RunnableInsert implements Runnable {
    
    public static Logger LOGGER = Logger.getLogger(RunnableInsert.class.getName());

    private final TransactionsProcessor transactionsProcessor;
    private final Runnable publishEvents;

    RunnableInsert(TransactionsProcessor transactionsProcessor, Runnable publishEvents) {
        this.transactionsProcessor = transactionsProcessor;
        this.publishEvents = publishEvents;
    }

    @Override
    public void run() {
        StoreCOMObjectsContainer container = this.transactionsProcessor.storeQueue.poll();
        if (container != null) {
            try {
                this.transactionsProcessor.dbBackend.getAvailability().acquire();
            } catch (InterruptedException ex) {
                TransactionsProcessor.LOGGER.log(Level.SEVERE, null, ex);
            }

            ArrayList<COMObjectEntity> objs = new ArrayList<>();
            objs.addAll(container.getPerObjs());

            while (true) {
                container = this.transactionsProcessor.storeQueue.peek(); // get next if there is one available

                if (container == null || !container.isContinuous()) {
                    break;
                }

                objs.addAll(this.transactionsProcessor.storeQueue.poll().getPerObjs());
            }

            this.transactionsProcessor.persistObjects(objs); // store
            this.transactionsProcessor.dbBackend.getAvailability().release();
        }

        if(publishEvents != null) {
          this.transactionsProcessor.generalExecutor.submit(publishEvents);
        }
    }
    
    void persistObjects(final ArrayList<COMObjectEntity> perObjs) {
        Connection c = this.transactionsProcessor.dbBackend.getConnection();
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
}