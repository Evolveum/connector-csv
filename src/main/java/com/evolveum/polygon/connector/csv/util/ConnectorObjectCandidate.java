package com.evolveum.polygon.connector.csv.util;

import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.objects.*;

import java.util.*;

public class ConnectorObjectCandidate {

    private ConnectorObjectId id;
    private ConnectorObjectBuilder candidateBuilder;
    private Set<ConnectorObjectReference> referencedObjects = new HashSet<>();
    private Set<ConnectorObjectCandidate> candidatesUponWhichThisObjectDepends = new HashSet<>();
    private Set<ConnectorObjectId> alreadyProcessedObjectIds = new HashSet<>();
    private Set<ConnectorObjectId> objectIdsToBeProcessed;
    private Set<ConnectorObjectId> subjectIdsToBeProcessed;
    private String nameAssocAttrDirect;
    private Integer depth = 0;

    private Set<String> referenceNames;

    private boolean isComplete = false;
    private static final Log LOG = Log.getLog(ConnectorObjectCandidate.class);

    public ConnectorObjectCandidate(ConnectorObjectId id, ConnectorObjectBuilder candidateBuilder, Set<ConnectorObjectId> associatedObjectIds,
                                    Set<ConnectorObjectId> subjectIdsToBeProcessed, Set<String> referenceNames, String nameAssocAttrDirect) {
        this.id = id;
        this.candidateBuilder = candidateBuilder;
        this.objectIdsToBeProcessed = associatedObjectIds;
        this.subjectIdsToBeProcessed = subjectIdsToBeProcessed;
        this.referenceNames = referenceNames;
        this.nameAssocAttrDirect = nameAssocAttrDirect;
    }

    public void evaluateDependencies() {

        if (depth < 2) {
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
        } else {
            Iterator<ConnectorObjectId> connectorObjectIdIterator = objectIdsToBeProcessed.iterator();
            while(connectorObjectIdIterator.hasNext()) {

                ConnectorObjectId objectId = connectorObjectIdIterator.next();
                AttributeBuilder attributeBuilder = new AttributeBuilder();

                attributeBuilder.setName(Name.NAME).addValue(Collections.singleton(objectId.getId()));
                ConnectorObjectIdentification connectorObjectIdentification =
                        new ConnectorObjectIdentification(objectId.getObjectClass(), Set.of(attributeBuilder.build()));

                ConnectorObjectReference connectorObjectReference =
                        new ConnectorObjectReference(connectorObjectIdentification);
                referencedObjects.add(connectorObjectReference);
                connectorObjectIdIterator.remove();
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
                if (referenceNames.size() == 1) {
                    candidateBuilder.addAttribute(AttributeBuilder.build(referenceNames.iterator().next(),
                            referencedObjects));
                } else {

                    Map<String, Set<ConnectorObjectReference>> referencesPerAttribute = new HashMap<>();
                    for (ConnectorObjectReference connectorObjectReference : referencedObjects) {

                        BaseConnectorObject baseConnectorObject = connectorObjectReference.getValue();

                        for (String name : referenceNames) {

                            String refAttrName = name;
                            if (refAttrName.contains(nameAssocAttrDirect + "-")) {
                                refAttrName = refAttrName.replaceAll(nameAssocAttrDirect + "-", "");
                            }
                            Attribute attribute = baseConnectorObject.getAttributeByName(refAttrName);

                            if (attribute != null && attribute.getValue()
                                    .contains(id.getId())) {
                                Set<ConnectorObjectReference> referenceSet = referencesPerAttribute.get(name);

                                if (referenceSet != null) {

                                    referenceSet.add(connectorObjectReference);
                                } else {
                                    referenceSet = new HashSet<>();
                                    referenceSet.add(connectorObjectReference);
                                    referencesPerAttribute.put(name, referenceSet);
                                }
                            }
                        }
                    }
                    for (String attrName : referencesPerAttribute.keySet()) {

                        candidateBuilder.addAttribute(AttributeBuilder.build(attrName,
                                referencesPerAttribute.get(attrName)));
                    }
                }
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
        candidate.setDepth(getDepth() + 1);
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

    public void dumpNonCompleteReferences() {

        if (LOG.isOk()) {
            if (objectIdsToBeProcessed != null) {
                LOG.ok("The reference lookup for connector object candidate {0} not complete.", id);
                LOG.ok("The following references still remain unresolved:");

                for (ConnectorObjectId coi : objectIdsToBeProcessed) {
                    LOG.ok("Referenced connector object: {0}", coi);
                    for (ConnectorObjectCandidate candidate : candidatesUponWhichThisObjectDepends) {
                        if (candidate.getId().equals(coi)) {
                            candidate.dumpNonCompleteReferences();
                        }
                    }
                }
            }
        }
    }

    public Integer getDepth() {
        return depth;
    }

    public void setDepth(Integer depth) {

        if (this.depth != 0) {

            if (depth < this.depth) {
                this.depth = depth;
            }
        } else {

            this.depth = depth;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        ConnectorObjectCandidate candidate = (ConnectorObjectCandidate) o;
        return isComplete == candidate.isComplete && Objects.equals(getId(), candidate.getId()) && Objects.equals(getCandidateBuilder(), candidate.getCandidateBuilder()) && Objects.equals(referencedObjects, candidate.referencedObjects) && Objects.equals(candidatesUponWhichThisObjectDepends, candidate.candidatesUponWhichThisObjectDepends) && Objects.equals(getAlreadyProcessedObjectIds(), candidate.getAlreadyProcessedObjectIds()) && Objects.equals(getObjectIdsToBeProcessed(), candidate.getObjectIdsToBeProcessed()) && Objects.equals(getSubjectIdsToBeProcessed(), candidate.getSubjectIdsToBeProcessed()) && Objects.equals(nameAssocAttrDirect, candidate.nameAssocAttrDirect) && Objects.equals(referenceNames, candidate.referenceNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId(), getCandidateBuilder(), referencedObjects, candidatesUponWhichThisObjectDepends, getAlreadyProcessedObjectIds(), getObjectIdsToBeProcessed(), getSubjectIdsToBeProcessed(), nameAssocAttrDirect, referenceNames, isComplete);
    }
}
