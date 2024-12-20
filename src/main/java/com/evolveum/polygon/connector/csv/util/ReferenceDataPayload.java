package com.evolveum.polygon.connector.csv.util;

import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.ObjectClass;

public class ReferenceDataPayload {

    private Attribute attribute;
    private String objectId;
    private ReferenceDataDeliveryVector referenceDataDeliveryVector;

    public ReferenceDataPayload(String objectId, Attribute attribute,
                                ReferenceDataDeliveryVector referenceDataDeliveryVector) {
        this.objectId = objectId;
        this.attribute = attribute;
        this.referenceDataDeliveryVector = referenceDataDeliveryVector;
    }

    public Attribute getAttribute() {
        return attribute;
    }

    public ReferenceDataDeliveryVector getReferenceDataDeliveryVector() {
        return referenceDataDeliveryVector;
    }

    public String getObjectId() {
        return objectId;
    }
}
