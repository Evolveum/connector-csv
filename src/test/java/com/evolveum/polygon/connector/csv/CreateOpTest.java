package com.evolveum.polygon.connector.csv;

import org.identityconnectors.common.Base64;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.common.objects.*;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.testng.AssertJUnit.*;

/**
 * Created by Viliam Repan (lazyman).
 */
public class CreateOpTest extends BaseTest {

    @Override
    protected CsvConfiguration createConfiguration() {
        CsvConfiguration config = new CsvConfiguration();

        config.setUniqueAttribute("uid");
        config.setPasswordAttribute("password");
        config.setNameAttribute("lastName");

        return config;
    }

    @Test
    public void createWithoutUid() throws Exception {
        ConnectorFacade connector = setupConnector("/create-empty.csv");

        final String uidValue = "uid=vilo,dc=example,dc=com";
        Set<Attribute> attributes = new HashSet<>();
        attributes.add(new Name("vilo repan"));
        attributes.add(createAttribute("firstName", "vilo"));
        attributes.add(createAttribute("uid", uidValue));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(Base64.encode("asdf".getBytes()).toCharArray())));
        Uid uid = connector.create(ObjectClass.ACCOUNT, attributes, null);
        assertNotNull(uid);
        assertEquals(uidValue, uid.getUidValue());

        ConnectorObject newObject = connector.getObject(ObjectClass.ACCOUNT, uid, null);
        assertConnectorObject(attributes, newObject);
    }

    private void assertConnectorObject(Set<Attribute> expected, ConnectorObject object) {
        Set<Attribute> real = object.getAttributes();
        assertEquals(expected.size(), real.size());

        for (Attribute attr : expected) {
            List<Object> expValues = attr.getValue();

            String name = attr.getName();
            if ("uid".equals(name)) {
                name = Uid.NAME;
            }
            Attribute realAttr = object.getAttributeByName(name);
            assertNotNull(realAttr);

            assertEquals(expValues, realAttr.getValue());
        }
    }

    private void assertAttribute(ConnectorObject object, String attrName, String value) {
        Attribute attribute = object.getAttributeByName(attrName);
        assertNotNull(attribute);

        List<Object> values = attribute.getValue();
        assertNotNull(values);
        assertEquals(1, values.size());

        assertEquals(value, values.get(0));
    }

    private Attribute createAttribute(String name, Object... values) {
        return AttributeBuilder.build(name, values);
    }
}
