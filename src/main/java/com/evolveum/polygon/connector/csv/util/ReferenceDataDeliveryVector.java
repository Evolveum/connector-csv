package com.evolveum.polygon.connector.csv.util;

import org.identityconnectors.framework.common.objects.ObjectClass;

import java.util.Objects;

public class ReferenceDataDeliveryVector {
    private ObjectClass objectClass;
    private Boolean originIsRecipient;
    private String attributeName;
    private String identificatorAttributeNameSubject;
    private String identificatorAttributeNameObject;
    boolean isAccess;

    public ReferenceDataDeliveryVector(ObjectClass objectClass, Boolean isRecipient, String attributeName,
                                       String identificatorAttributeNameSubject, String identificatorAttributeNameObject, boolean isAccess) {
        this.objectClass = objectClass;
        this.originIsRecipient = isRecipient;
        this.attributeName = attributeName;
        this.identificatorAttributeNameSubject = identificatorAttributeNameSubject;
        this.identificatorAttributeNameObject = identificatorAttributeNameObject;
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

    public String getIdentificatorAttributeNameSubject() {
        return identificatorAttributeNameSubject;
    }

    public String getIdentificatorAttributeNameObject() {
        return identificatorAttributeNameObject;
    }

    public boolean isAccess() {
        return isAccess;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        ReferenceDataDeliveryVector that = (ReferenceDataDeliveryVector) o;
        return isAccess() == that.isAccess() && Objects.equals(getObjectClass(), that.getObjectClass()) && Objects.equals(originIsRecipient, that.originIsRecipient) && Objects.equals(getAttributeName(), that.getAttributeName()) && Objects.equals(identificatorAttributeNameSubject, that.identificatorAttributeNameSubject) && Objects.equals(identificatorAttributeNameObject, that.identificatorAttributeNameObject);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getObjectClass(), originIsRecipient, getAttributeName(), identificatorAttributeNameSubject, identificatorAttributeNameObject, isAccess());
    }

    @Override
    public String toString() {
        return "ReferenceDataDeliveryVector{" +
                "objectClass=" + objectClass +
                ", originIsRecipient=" + originIsRecipient +
                ", attributeName='" + attributeName + '\'' +
                ", identificatorAttributeNameSubject='" + identificatorAttributeNameSubject + '\'' +
                ", identificatorAttributeNameObject='" + identificatorAttributeNameObject + '\'' +
                ", isAccess=" + isAccess +
                '}';
    }
}
