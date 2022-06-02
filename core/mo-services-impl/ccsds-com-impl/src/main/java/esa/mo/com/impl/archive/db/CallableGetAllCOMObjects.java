package esa.mo.com.impl.archive.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.logging.Level;

import org.ccsds.moims.mo.mal.structures.LongList;

final class CallableGetAllCOMObjects implements Callable<LongList> {
    /**
 *
 */
private final TransactionsProcessor transactionsProcessor;
    private final Integer domainId;
    private final Integer objTypeId;

    CallableGetAllCOMObjects(TransactionsProcessor transactionsProcessor, Integer domainId, Integer objTypeId) {
        this.transactionsProcessor = transactionsProcessor;
        this.domainId = domainId;
        this.objTypeId = objTypeId;
    }

    @Override
    public LongList call() {
        try {
            this.transactionsProcessor.dbBackend.getAvailability().acquire();
        } catch (InterruptedException ex) {
            TransactionsProcessor.LOGGER.log(Level.SEVERE, null, ex);
        }

        LongList objIds = new LongList();
        Connection c = this.transactionsProcessor.dbBackend.getConnection();

        try {
            String stm = "SELECT objId FROM COMObjectEntity WHERE ((objectTypeId = ?) AND (domainId = ?))";
            PreparedStatement getCOMObject = c.prepareStatement(stm);
            getCOMObject.setInt(1, objTypeId);
            getCOMObject.setInt(2, domainId);
            ResultSet rs = getCOMObject.executeQuery();

            while (rs.next()) {
                objIds.add(TransactionsProcessor.convert2Long(rs.getObject(1)));
            }
        } catch (SQLException ex) {
            TransactionsProcessor.LOGGER.log(Level.SEVERE, null, ex);
        }

        this.transactionsProcessor.dbBackend.getAvailability().release();
        return objIds;
    }
}