package com.evolveum.polygon.connector.csv.util;

import org.identityconnectors.framework.common.objects.ConnectorObjectBuilder;
import org.identityconnectors.framework.common.objects.SyncDeltaType;
import org.identityconnectors.framework.common.objects.SyncToken;

import java.util.Set;

public class SyncDeltaObjectCandidate extends ConnectorObjectCandidate {
    private SyncToken syncToken;
    private SyncDeltaType syncDeltaType;

    public SyncDeltaObjectCandidate(ConnectorObjectId id, ConnectorObjectBuilder candidateBuilder,
                                    Set<ConnectorObjectId> associatedObjectIds,
                                    Set<ConnectorObjectId> subjectIdsToBeProcessed, SyncToken syncToken,
                                    SyncDeltaType syncDeltaType, String referenceName) {

        super(id, candidateBuilder, associatedObjectIds, subjectIdsToBeProcessed, referenceName);
        this.syncToken = syncToken;
        this.syncDeltaType = syncDeltaType;
    }

    public SyncToken getSyncToken() {
        return syncToken;
    }

    public SyncDeltaType getSyncDeltaType() {
        return syncDeltaType;
    }

}
