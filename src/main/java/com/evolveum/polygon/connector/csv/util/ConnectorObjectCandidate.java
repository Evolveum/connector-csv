package com.evolveum.polygon.connector.csv.util;

import org.identityconnectors.framework.common.objects.*;

import java.util.*;

import static com.evolveum.polygon.connector.csv.util.Util.ASSOC_ATTR_GROUP;

public class ConnectorObjectCandidate {

    private ConnectorObjectId id;
    private ConnectorObjectBuilder candidateBuilder;
    private Set<ConnectorObjectReference> referencedObjects = new HashSet<>();
    private Set<ConnectorObjectCandidate> candidatesUponWhichThisObjectDepends = new HashSet<>();
    private Set<ConnectorObjectId> alreadyProcessedObjectIds = new HashSet<>();
    private Set<ConnectorObjectId> objectIdsToBeProcessed;
    private Set<ConnectorObjectId> subjectIdsToBeProcessed;
//    private String referenceName;

    private Set<String> referenceNames;

    private boolean isComplete = false;


    public ConnectorObjectCandidate(ConnectorObjectId id, ConnectorObjectBuilder candidateBuilder, Set<ConnectorObjectId> associatedObjectIds,
                                    Set<ConnectorObjectId> subjectIdsToBeProcessed, String referenceName) {
        this(id, candidateBuilder, associatedObjectIds, subjectIdsToBeProcessed, Set.of(referenceName));
    }

    public ConnectorObjectCandidate(ConnectorObjectId id, ConnectorObjectBuilder candidateBuilder, Set<ConnectorObjectId> associatedObjectIds,
                                    Set<ConnectorObjectId> subjectIdsToBeProcessed, Set<String> referenceNames) {
        this.id = id;
        this.candidateBuilder = candidateBuilder;
        this.objectIdsToBeProcessed = associatedObjectIds;
        this.subjectIdsToBeProcessed = subjectIdsToBeProcessed;
        this.referenceNames = referenceNames;
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

                if(referenceNames.size() == 1){

                    candidateBuilder.addAttribute(AttributeBuilder.build(referenceNames.iterator().next(),
                            referencedObjects));
                }else {

                    Map<String, Set<ConnectorObjectReference>> referencesPerAttribute = new HashMap<>();
                    for (ConnectorObjectReference connectorObjectReference : referencedObjects) {

                        BaseConnectorObject baseConnectorObject = connectorObjectReference.getValue();

                        for (String name : referenceNames) {

                           String refAttrName = name;
                            if(refAttrName.contains(ASSOC_ATTR_GROUP+"-")){
                                refAttrName = refAttrName.replaceAll(ASSOC_ATTR_GROUP+"-", "");
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
                    for(String attrName : referencesPerAttribute.keySet()){

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
