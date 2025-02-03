package com.evolveum.polygon.connector.csv.util;

import org.identityconnectors.framework.common.objects.ObjectClass;

import java.util.Objects;

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

    @Override
    public String toString() {
        return "ReferenceDataDeliveryVector{" +
                "objectClass=" + objectClass +
                ", originIsRecipient=" + originIsRecipient +
                ", attributeName='" + attributeName + '\'' +
                ", identificatorAttributeName='" + identificatorAttributeName + '\'' +
                ", isAccess=" + isAccess +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReferenceDataDeliveryVector that = (ReferenceDataDeliveryVector) o;
        return isAccess() == that.isAccess() && Objects.equals(getObjectClass(), that.getObjectClass()) && Objects.equals(originIsRecipient, that.originIsRecipient) && Objects.equals(getAttributeName(), that.getAttributeName()) && Objects.equals(identificatorAttributeName, that.identificatorAttributeName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getObjectClass(), originIsRecipient, getAttributeName(), identificatorAttributeName, isAccess());
    }
}
