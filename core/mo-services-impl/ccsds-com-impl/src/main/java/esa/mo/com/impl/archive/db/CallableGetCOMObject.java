package esa.mo.com.impl.archive.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.logging.Level;

import esa.mo.com.impl.archive.entities.COMObjectEntity;

final class CallableGetCOMObject implements Callable<COMObjectEntity> {
    /**
 *
 */
private final TransactionsProcessor transactionsProcessor;
    private final Integer domainId;
    private final Long objId;
    private final Integer objTypeId;

    CallableGetCOMObject(TransactionsProcessor transactionsProcessor, Integer domainId, Long objId, Integer objTypeId) {
        this.transactionsProcessor = transactionsProcessor;
        this.domainId = domainId;
        this.objId = objId;
        this.objTypeId = objTypeId;
    }

    @Override
    public COMObjectEntity call() {
        try {
            this.transactionsProcessor.dbBackend.getAvailability().acquire();
        } catch (InterruptedException ex) {
            TransactionsProcessor.LOGGER.log(Level.SEVERE, null, ex);
        }

        COMObjectEntity comObject = null;
        Connection c = this.transactionsProcessor.dbBackend.getConnection();

        try {
            String stm = "SELECT objectTypeId, domainId, objId, timestampArchiveDetails, providerURI, network, sourceLinkObjectTypeId, sourceLinkDomainId, sourceLinkObjId, relatedLink, objBody FROM COMObjectEntity WHERE (((objectTypeId = ?) AND (objId = ?)) AND (domainId = ?))";
            PreparedStatement getCOMObject = c.prepareStatement(stm);
            getCOMObject.setInt(1, objTypeId);
            getCOMObject.setLong(2, objId);
            getCOMObject.setInt(3, domainId);
            ResultSet rs = getCOMObject.executeQuery();

            while (rs.next()) {
                comObject = new COMObjectEntity(
                        (Integer) rs.getObject(1),
                        (Integer) rs.getObject(2),
                        TransactionsProcessor.convert2Long(rs.getObject(3)),
                        TransactionsProcessor.convert2Long(rs.getObject(4)),
                        (Integer) rs.getObject(5),
                        (Integer) rs.getObject(6),
                        new SourceLinkContainer((Integer) rs.getObject(7), (Integer) rs.getObject(8), TransactionsProcessor.convert2Long(rs.getObject(9))),
                        TransactionsProcessor.convert2Long(rs.getObject(10)),
                        (byte[]) rs.getObject(11));
            }
        } catch (SQLException ex) {
            TransactionsProcessor.LOGGER.log(Level.SEVERE, null, ex);
        }

        this.transactionsProcessor.dbBackend.getAvailability().release();
        return comObject;
    }
}