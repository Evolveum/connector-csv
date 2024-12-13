package com.evolveum.polygon.connector.csv.util;

import org.identityconnectors.framework.common.objects.ObjectClass;

import java.util.Objects;
import java.util.Set;

public class ConnectorObjectId {

    private String id;
    private ObjectClass objectClass;
    private Set<ObjectClass> relatedObjectClasses;

    public ConnectorObjectId(String id, ObjectClass objectClass) {

        this(id ,objectClass, null);
    }
    public ConnectorObjectId(String id, ObjectClass objectClass, Set<ObjectClass> relatedObjectClasses) {
        this.id = id;
        this.objectClass = objectClass;
        this.relatedObjectClasses = relatedObjectClasses;
    }

    public ObjectClass getObjectClass() {
        return objectClass;
    }

    public String getId() {
        return id;
    }

    public Set<ObjectClass> getRelatedObjectClasses() {
        return relatedObjectClasses;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConnectorObjectId that = (ConnectorObjectId) o;
        return Objects.equals(getId(), that.getId()) && Objects.equals(getObjectClass(), that.getObjectClass());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId(), getObjectClass());
    }
}
