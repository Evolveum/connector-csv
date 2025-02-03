package com.evolveum.polygon.connector.csv.util;

import com.evolveum.polygon.connector.csv.ObjectClassHandler;
import com.evolveum.polygon.connector.csv.ObjectClassHandlerConfiguration;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.*;
import org.identityconnectors.framework.common.objects.filter.*;

import java.util.*;

import static com.evolveum.polygon.connector.csv.util.Util.constructReferenceDataVector;

public class ReferentialInterityHandler {

    private ObjectClassHandler objectClassHandler;
    private static final Log LOG = Log.getLog(ReferentialInterityHandler.class);

    public ReferentialInterityHandler(ObjectClassHandler objectClassHandler) {

        this.objectClassHandler = objectClassHandler;
    }

    public void handle(String uidValue, Operation operation) {

        handle(uidValue, null, operation);
    }

    public void handle(String uidValue, String uidUpdated, Operation operation) {

        if(LOG.isOk()){

            String tmp = operation == Operation.UPDATE ? "The updated uid: "+ uidUpdated : "";

            LOG.ok("Handling referential integrity for the object {0}, the operation which triggered this handling: {1}."
                    + tmp, uidValue, operation);
        }

        Map<ObjectClass, Set<ReferenceDataDeliveryVector>> referenceDataDeliveryVectors =
                determineReferenceDataDeliveryVectors();

            handleReferentialIntegrity(uidValue, uidUpdated, referenceDataDeliveryVectors, operation);
    }

    private void handleReferentialIntegrity(String subjectOfReferentialIntegrityUidValue,
                                            String uidUpdated, Map<ObjectClass,
            Set<ReferenceDataDeliveryVector>> referenceDataDeliveryVectors, Operation operation) {

        for (ObjectClass vectorObjectClass : referenceDataDeliveryVectors.keySet()) {

            Set<ReferenceDataDeliveryVector> referenceDataDeliveryVectorsPerOc = referenceDataDeliveryVectors.get
                    (vectorObjectClass);
            Set<Filter> filters = new HashSet<>();
            Set<AttributeDelta> attributeDeltasSet = new HashSet<>();
            boolean isSubjectForCleanup = false;
            ObjectClassHandler vectorObjectClassHandler;

            if (objectClassHandler.getObjectClass().equals(vectorObjectClass)) {

                vectorObjectClassHandler = objectClassHandler;
            } else {

                vectorObjectClassHandler = objectClassHandler.getHandlers().get(vectorObjectClass);
            }

            for (ReferenceDataDeliveryVector referenceDataDeliveryVector : referenceDataDeliveryVectorsPerOc) {

                if(LOG.isOk()){
                    LOG.ok("The Vector used for Referential Integrity operation {0}", referenceDataDeliveryVector);
                }

                Attribute attribute = AttributeBuilder.build(referenceDataDeliveryVector.getAttributeName(),
                        subjectOfReferentialIntegrityUidValue);

                saturateFilterSet(filters, attribute,
                        vectorObjectClassHandler.getConfiguration().isIgnoreIdentifierCase());

                AttributeDelta attributeDelta;
                if (Operation.DELETE.equals(operation)){

                    attributeDelta = AttributeDeltaBuilder.build(referenceDataDeliveryVector
                            .getAttributeName(), null, Arrays.asList(subjectOfReferentialIntegrityUidValue));
                } else {

                    attributeDelta = AttributeDeltaBuilder.build(referenceDataDeliveryVector
                            .getAttributeName(), Arrays.asList(uidUpdated),
                            Arrays.asList(subjectOfReferentialIntegrityUidValue));
                }

                attributeDeltasSet.add(attributeDelta);

                    if (!isSubjectForCleanup) {

                        isSubjectForCleanup = referenceDataDeliveryVector.isAccess;
                    }
            }

            Filter filter = constructFinalUpdateFilter(filters,
                    subjectOfReferentialIntegrityUidValue);

            if(LOG.isOk()){

                for (AttributeDelta attributeDelta : attributeDeltasSet){

                    LOG.ok("Attribute DELTA used for referential integrity update: {0}. {1}", attributeDelta,
                            System.lineSeparator());
                }
                LOG.ok("Attribute FILTER used for referential integrity update: {0}. {1}", filter,
                        System.lineSeparator());
            }

            vectorObjectClassHandler.updateAllReferencesOfValue(filter, attributeDeltasSet,
                    subjectOfReferentialIntegrityUidValue, isSubjectForCleanup);
        }
    }

    private void saturateFilterSet(Set<Filter> filters, Attribute attribute, boolean isIgnoreCase) {
        Filter filter;

        if (isIgnoreCase) {

            filter = FilterBuilder.equalsIgnoreCase(attribute);
        } else {

            filter = FilterBuilder.equalTo(attribute);
        }

        filters.add(filter);
    }

    private Filter constructFinalUpdateFilter(Set<Filter> filters, String subjectOfReferentialIntegrityUidValue) {
        Filter filter;

        if (filters.size() > 1) {

            filter = FilterBuilder.or(filters);
        } else {

            Iterator<Filter> iterator = filters.iterator();
            if (!iterator.hasNext()) {

                throw new ConnectorException("Referential integrity could not construct resource query for the" +
                        "analysis of object with the UID " + subjectOfReferentialIntegrityUidValue);
            } else {
                filter = iterator.next();
            }
        }

        return filter;
    }

    private Map<ObjectClass, Set<ReferenceDataDeliveryVector>> determineReferenceDataDeliveryVectors() {

        Map<ObjectClass, Set<ReferenceDataDeliveryVector>> vectorMap = new HashMap<>();

        Map<String, HashSet<AssociationHolder>> associationHolders = objectClassHandler.getAssociationHolders();
        String currentObjectClassName = objectClassHandler.getObjectClassName();
        ObjectClassHandlerConfiguration configuration = objectClassHandler.getConfiguration();

        if (associationHolders.containsKey(currentObjectClassName)) {
            HashSet<AssociationHolder> holders = associationHolders.get(currentObjectClassName);

            List<String> cleanupOcList = new ArrayList<>();

            for (AssociationHolder holder : holders) {
                ReferenceDataDeliveryVector referenceDataDeliveryVector;

                if (holder.isAccess()) {

                    cleanupOcList.add(holder.getObjectObjectClassName());
                }

                String vectorObjectClassName;

                if (!currentObjectClassName.equals(holder.getSubjectObjectClassName())) {

                    vectorObjectClassName = holder.getSubjectObjectClassName();
                } else {
                    vectorObjectClassName = holder.getObjectObjectClassName();
                }


                boolean isPartOfAccess = isPartOfAccess(holder, associationHolders.get(vectorObjectClassName));

//                    referenceDataDeliveryVector = Util.constructReferenceDataVector(
//                            Util.getObjectClass(vectorObjectClassName), holder,
//                            configuration.getUniqueAttribute(),currentObjectClassName, objectClassHandler.getHandlers(),
//                            null, isPartOfAccess);

                if (!currentObjectClassName.equals(holder.getObjectObjectClassName())) {
                    ObjectClassHandler handler = objectClassHandler.getHandlers()
                            .get(Util.getObjectClass(holder.getObjectObjectClassName()));

                    referenceDataDeliveryVector = constructReferenceDataVector(
                            Util.getObjectClass(vectorObjectClassName), holder,
                            handler.getConfiguration().getUniqueAttribute(),
                            handler.getConfiguration().getNameAttribute(), null,
                            isPartOfAccess);
                } else {

                    referenceDataDeliveryVector = constructReferenceDataVector(
                            Util.getObjectClass(vectorObjectClassName),
                            holder, configuration.getUniqueAttribute()
                            , configuration.getNameAttribute(),
                            null, isPartOfAccess);
                }

                if (referenceDataDeliveryVector != null) {
                    ObjectClass objectClassOfVector = referenceDataDeliveryVector.getObjectClass();
                    if (vectorMap.containsKey(objectClassOfVector)) {

                        Set<ReferenceDataDeliveryVector> setOfVectors = vectorMap.get(objectClassOfVector);
                        setOfVectors.add(referenceDataDeliveryVector);

                    } else {

                        Set<ReferenceDataDeliveryVector> setOfVectors = new HashSet<>();
                        setOfVectors.add(referenceDataDeliveryVector);
                        vectorMap.put(objectClassOfVector, setOfVectors);
                    }
                }
            }
        }
        return vectorMap;
    }

    private boolean isPartOfAccess(AssociationHolder holder, HashSet<AssociationHolder> associationHolders) {

        if(holder.isAccess()){
            return true;
        }

        for (AssociationHolder holderFromSet : associationHolders) {
            if (!holderFromSet.equals(holder)) {
                if (holderFromSet.isAccess()) {
                    if (holderFromSet.getObjectObjectClassName().
                            equals(holder.getSubjectObjectClassName())) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

}