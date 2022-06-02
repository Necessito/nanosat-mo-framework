package esa.mo.com.impl.archive.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.logging.Level;

import org.ccsds.moims.mo.com.archive.structures.ArchiveQuery;
import org.ccsds.moims.mo.com.archive.structures.PaginationFilter;
import org.ccsds.moims.mo.com.archive.structures.QueryFilter;
import org.ccsds.moims.mo.mal.structures.IntegerList;

import esa.mo.com.impl.archive.db.TransactionsProcessor.QueryType;
import esa.mo.com.impl.archive.entities.COMObjectEntity;

class CallableQuery implements Callable<Object> {

/**
 *
 */
private final TransactionsProcessor transactionsProcessor;
private final IntegerList objTypeIds;
private final ArchiveQuery archiveQuery;
private final IntegerList domainIds;
private final Integer providerURIId;
private final Integer networkId;
private final SourceLinkContainer sourceLink;
private final QueryFilter filter;
private final QueryType queryType;


public CallableQuery(TransactionsProcessor transactionsProcessor, final IntegerList objTypeIds,
    final ArchiveQuery archiveQuery, final IntegerList domainIds,
    final Integer providerURIId, final Integer networkId,
    final SourceLinkContainer sourceLink, final QueryFilter filter) {
  this(transactionsProcessor, objTypeIds, archiveQuery, domainIds, providerURIId, networkId, sourceLink, filter, QueryType.SELECT);
}

public CallableQuery(TransactionsProcessor transactionsProcessor, final IntegerList objTypeIds,
    final ArchiveQuery archiveQuery, final IntegerList domainIds,
    final Integer providerURIId, final Integer networkId,
    final SourceLinkContainer sourceLink, final QueryFilter filter,
    final QueryType queryType) {
  this.transactionsProcessor = transactionsProcessor;
this.objTypeIds = objTypeIds;
  this.archiveQuery = archiveQuery;
  this.domainIds = domainIds;
  this.providerURIId = providerURIId;
  this.networkId = networkId;
  this.sourceLink = sourceLink;
  this.filter = filter;
  this.queryType = queryType;
}

@Override
public Object call() {
  final boolean relatedContainsWildcard = (archiveQuery.getRelated().equals((long) 0));
  final boolean startTimeContainsWildcard = (archiveQuery.getStartTime() == null);
  final boolean endTimeContainsWildcard = (archiveQuery.getEndTime() == null);
  final boolean providerURIContainsWildcard = (archiveQuery.getProvider() == null);
  final boolean networkContainsWildcard = (archiveQuery.getNetwork() == null);

  final boolean sourceContainsWildcard = (archiveQuery.getSource() == null);
  boolean sourceObjIdContainsWildcard = true;

  this.transactionsProcessor.dbBackend.createIndexesIfFirstTime();

  if (!sourceContainsWildcard) {
      sourceObjIdContainsWildcard
              = (archiveQuery.getSource().getKey().getInstId() == null || archiveQuery.getSource().getKey().getInstId() == 0);
  }

  // Generate the query string
  String fieldsList = "objectTypeId, domainId, objId, timestampArchiveDetails, providerURI, " +
                      "network, sourceLinkObjectTypeId, sourceLinkDomainId, sourceLinkObjId, relatedLink, objBody";
  String queryString = queryType.getQueryPrefix() + " " + (queryType == QueryType.SELECT ? fieldsList : "") + " FROM COMObjectEntity ";

  queryString += "WHERE ";

  queryString += TransactionsProcessor.generateQueryStringFromLists("domainId", domainIds);
  queryString += TransactionsProcessor.generateQueryStringFromLists("objectTypeId", objTypeIds);

  queryString += (relatedContainsWildcard) ? ""
      : "relatedLink=" + archiveQuery.getRelated() + " AND ";
  queryString += (startTimeContainsWildcard) ? ""
      : "timestampArchiveDetails>=" + archiveQuery.getStartTime().getValue() + " AND ";
  queryString += (endTimeContainsWildcard) ? ""
      : "timestampArchiveDetails<=" + archiveQuery.getEndTime().getValue() + " AND ";
  queryString += (providerURIContainsWildcard) ? ""
      : "providerURI=" + providerURIId + " AND ";
  queryString += (networkContainsWildcard) ? "" : "network=" + networkId + " AND ";

  if (!sourceContainsWildcard) {
    queryString += TransactionsProcessor.generateQueryStringFromLists("sourceLinkObjectTypeId",
        sourceLink.getObjectTypeIds());
    queryString += TransactionsProcessor.generateQueryStringFromLists("sourceLinkDomainId",
        sourceLink.getDomainIds());
    queryString += (sourceObjIdContainsWildcard) ? ""
        : "sourceLinkObjId=" + sourceLink.getObjId() + " AND ";
  }

  queryString = queryString.substring(0, queryString.length() - 4);

  // A dedicated PaginationFilter for this particular COM Archive implementation was created and implemented
  if (filter != null) {
    if (filter instanceof PaginationFilter) {
      PaginationFilter pfilter = (PaginationFilter) filter;

      // Double check if the filter fields are really not null
      if (pfilter.getLimit() != null && pfilter.getOffset() != null) {
        String sortOrder = "ASC ";
        if (archiveQuery.getSortOrder() != null) {
          sortOrder = (archiveQuery.getSortOrder()) ? "ASC " : "DESC ";
        }

                    queryString += "ORDER BY timestampArchiveDetails "
                            + sortOrder
                            + "LIMIT "
                            + pfilter.getLimit().getValue()
                            + " OFFSET "
                            + pfilter.getOffset().getValue();
                }
            }
        }

        try {
            this.transactionsProcessor.dbBackend.getAvailability().acquire();
        } catch (InterruptedException ex) {
            TransactionsProcessor.LOGGER.log(Level.SEVERE, null, ex);
        }

        ArrayList<COMObjectEntity> perObjs = new ArrayList<>();
        try {
            Connection c = this.transactionsProcessor.dbBackend.getConnection();
            PreparedStatement query = c.prepareStatement(queryString);
            if (queryType.equals(QueryType.DELETE)) {
                int result =  query.executeUpdate();
                this.transactionsProcessor.dbBackend.getAvailability().release();
                return result;
            }
            ResultSet rs = query.executeQuery();

            while (rs.next()) {
                perObjs.add(new COMObjectEntity(
                        (Integer) rs.getObject(1),
                        (Integer) rs.getObject(2),
                        TransactionsProcessor.convert2Long(rs.getObject(3)),
                        TransactionsProcessor.convert2Long(rs.getObject(4)),
                        (Integer) rs.getObject(5),
                        (Integer) rs.getObject(6),
                        new SourceLinkContainer((Integer) rs.getObject(7), (Integer) rs.getObject(8), TransactionsProcessor.convert2Long(rs.getObject(9))),
                        TransactionsProcessor.convert2Long(rs.getObject(10)),
                        (byte[]) rs.getObject(11))
                );
            }
        } catch (SQLException ex) {
            TransactionsProcessor.LOGGER.log(Level.SEVERE, null, ex);
        }

        this.transactionsProcessor.dbBackend.getAvailability().release();
        return perObjs;
    }

}