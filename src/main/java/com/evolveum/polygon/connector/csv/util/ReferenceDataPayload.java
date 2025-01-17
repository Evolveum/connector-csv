package com.evolveum.polygon.connector.csv.util;

import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeBuilder;
import org.identityconnectors.framework.common.objects.ObjectClass;

import java.util.Set;

public class ReferenceDataPayload {

    private Set<Attribute> attributes;
    private ConnectorObjectId connectorObjectId;

    public ReferenceDataPayload(ConnectorObjectId connectorObjectId, Set<Attribute> attributes) {
        this.connectorObjectId = connectorObjectId;
        this.attributes = attributes;

    }

    public Set<Attribute> getAttributes() {
        return attributes;
    }

    public ConnectorObjectId getConnectorObjectId() {
        return connectorObjectId;
    }
}
