package com.evolveum.polygon.connector.csv.util;

import org.identityconnectors.framework.common.objects.AttributeBuilder;
import org.identityconnectors.framework.common.objects.ConnectorObjectBuilder;
import org.identityconnectors.framework.common.objects.ConnectorObjectReference;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static com.evolveum.polygon.connector.csv.util.Util.ASSOC_ATTR_GROUP;

public class ConnectorObjectCandidate {

    private ConnectorObjectId id;
    private ConnectorObjectBuilder candidateBuilder;
    private Set<ConnectorObjectReference> referencedObjects = new HashSet<>();
    private Set<ConnectorObjectCandidate> candidatesUponWhichThisObjectDepends = new HashSet<>();
    private Set<ConnectorObjectId> alreadyProcessedObjectIds = new HashSet<>();
    private Set<ConnectorObjectId> objectIdsToBeProcessed;
    private Set<ConnectorObjectId> subjectIdsToBeProcessed;
    private boolean isComplete = false;


    public ConnectorObjectCandidate(ConnectorObjectId id, ConnectorObjectBuilder candidateBuilder, Set<ConnectorObjectId> associatedObjectIds,
                                    Set<ConnectorObjectId> subjectIdsToBeProcessed) {
        this.id = id;
        this.candidateBuilder = candidateBuilder;
        this.objectIdsToBeProcessed = associatedObjectIds;
        this.subjectIdsToBeProcessed = subjectIdsToBeProcessed;
    }


    public void evaluateDependencies() {

        Iterator<ConnectorObjectCandidate> iterator = candidatesUponWhichThisObjectDepends.iterator();
        while (iterator.hasNext()) {
            ConnectorObjectCandidate candidate = iterator.next();
            ConnectorObjectId candidateId = candidate.getId();
            if (candidate.complete()) {

                ConnectorObjectReference connectorObjectReference =
                        new ConnectorObjectReference(candidate.getCandidateBuilder().build());
                referencedObjects.add(connectorObjectReference);
                if (objectIdsToBeProcessed.remove(candidateId)) {

                    alreadyProcessedObjectIds.add(candidateId);
                }

                iterator.remove();
            }
        }

    }

    public boolean complete() {

        if (isComplete) {
            return true;
        }

        if (!objectIdsToBeProcessed.isEmpty()) {
            evaluateDependencies();
        }

        if (objectIdsToBeProcessed.isEmpty()) {

            if (!referencedObjects.isEmpty()) {
                candidateBuilder.addAttribute(AttributeBuilder.build(ASSOC_ATTR_GROUP, referencedObjects));
            }
            isComplete = true;
            return true;
        }
        return false;
    }

    public void addCandidateUponWhichThisDepends(ConnectorObjectCandidate candidate) {

        if (this.getId().equals(candidate.getId())) {
            return;
        }

        for (ConnectorObjectCandidate currentCandidate : candidatesUponWhichThisObjectDepends) {
            if (currentCandidate.getId().equals(candidate.getId())) {
                return;
            }
        }
        candidatesUponWhichThisObjectDepends.add(candidate);
    }

    public ConnectorObjectId getId() {
        return id;
    }

    public ConnectorObjectBuilder getCandidateBuilder() {
        return candidateBuilder;
    }

    public Set<ConnectorObjectId> getObjectIdsToBeProcessed() {
        return objectIdsToBeProcessed;
    }

    public Set<ConnectorObjectId> getSubjectIdsToBeProcessed() {
        return subjectIdsToBeProcessed;
    }

    public Set<ConnectorObjectId> getAlreadyProcessedObjectIds() {
        return alreadyProcessedObjectIds;
    }
}
