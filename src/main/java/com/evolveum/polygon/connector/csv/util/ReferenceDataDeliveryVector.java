package com.evolveum.polygon.connector.csv.util;

import org.identityconnectors.framework.common.objects.ObjectClass;

public class ReferenceDataDeliveryVector {
    private ObjectClass objectClass;
    private Boolean originIsRecipient;
    private String attributeName;
    private String identificatorAttributeName;
    boolean isAccess;

    public ReferenceDataDeliveryVector(ObjectClass objectClass, Boolean isRecipient, String attributeName,
                                       String identificatorAttributeName) {
        this(objectClass, isRecipient, attributeName, identificatorAttributeName, false);
    }

    public ReferenceDataDeliveryVector(ObjectClass objectClass, Boolean isRecipient, String attributeName,
                                       String identificatorAttributeName, boolean isAccess) {
        this.objectClass = objectClass;
        this.originIsRecipient = isRecipient;
        this.attributeName = attributeName;
        this.identificatorAttributeName = identificatorAttributeName;
        this.isAccess = isAccess;
    }

    public ObjectClass getObjectClass() {
        return objectClass;
    }

    public Boolean originIsRecipient() {
        return originIsRecipient;
    }

    public String getAttributeName() {
        return attributeName;
    }

    public String getIdAttributeName() {
        return identificatorAttributeName;
    }

    public boolean isAccess() {
        return isAccess;
    }
}
