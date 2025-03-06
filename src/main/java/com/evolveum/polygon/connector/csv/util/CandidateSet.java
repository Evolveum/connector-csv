package com.evolveum.polygon.connector.csv.util;

import java.util.HashSet;
import java.util.Set;


public class CandidateSet<E extends ConnectorObjectCandidate> extends HashSet<ConnectorObjectCandidate> {

    Set<ConnectorObjectId> candidateId = new HashSet<>();


    public boolean addWithId(E e) {

        candidateId.add(e.getId());
        return super.add(e);
    }

    public boolean removeWithId(E e) {

        candidateId.remove(e.getId());
        return super.remove(e);
    }

    public Set<ConnectorObjectId> getCandidateId() {
        return candidateId;
    }
}
