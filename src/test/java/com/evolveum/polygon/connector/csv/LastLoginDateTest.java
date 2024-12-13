package com.evolveum.polygon.connector.csv;

import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.identityconnectors.framework.common.objects.*;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Set;

import static org.testng.AssertJUnit.*;

public class LastLoginDateTest extends BaseTest {

    private static final String CSV_FILE = "/last-login-date.csv";

    private static final String LAST_LOGIN_DATE_ATTRIBUTE = "myLastLoginDate";

    private static final String LAST_LOGIN_DATE_CUSTOM_FORMAT_ATTRIBUTE = "myLastLoginDateCustomFormat";

    private static final String LAST_LOGIN_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";

    private static final String JOHN_UID = "123";

    /*
     * Same for formatted value: 2024-11-08T17:39:05.311+01:00
     */
    private static final long LAST_LOGIN_DATE_VALUE = 1731083945311L;

    private static final String LAST_LOGIN_DATE_CUSTOM_FORMAT_VALUE = "2024-11-08T17:39:05.311+01:00";

    @Test(expectedExceptions = ConfigurationException.class, expectedExceptionsMessageRegExp = ".*Invalid last login date format.*")
    public void wrongLastLoginDateFormat() throws Exception {
        setupConnectorFacade(LAST_LOGIN_DATE_CUSTOM_FORMAT_ATTRIBUTE, "wrong\nformatasdfjklo");
    }

    @Test
    public void noLastLoginDate() throws Exception {
        ConnectorFacade connector = setupConnectorFacade(null, null);

        Schema schema = connector.schema();
        assertSchema(schema, null);

        ConnectorObject object = connector.getObject(ObjectClass.ACCOUNT, new Uid(JOHN_UID), null);
        assertAttributes(object, null, Long.toString(LAST_LOGIN_DATE_VALUE), LAST_LOGIN_DATE_CUSTOM_FORMAT_VALUE);
    }

    @Test
    public void withLastLoginDate() throws Exception {
        ConnectorFacade connector = setupConnectorFacade(LAST_LOGIN_DATE_ATTRIBUTE, null);

        Schema schema = connector.schema();
        assertSchema(schema, LAST_LOGIN_DATE_ATTRIBUTE);

        ConnectorObject object = connector.getObject(ObjectClass.ACCOUNT, new Uid(JOHN_UID), null);
        assertAttributes(object, LAST_LOGIN_DATE_VALUE, null, LAST_LOGIN_DATE_CUSTOM_FORMAT_VALUE);

        updateAndAssertLastLoginDate(connector, object);
    }

    private void updateAndAssertLastLoginDate(ConnectorFacade connector, ConnectorObject object) {
        long current = System.currentTimeMillis();
        Attribute updated = AttributeBuilder.build(PredefinedAttributes.LAST_LOGIN_DATE_NAME, current);
        connector.update(ObjectClass.ACCOUNT, new Uid(JOHN_UID), Set.of(updated), null);

        String myLastLoginDate = null;
        if (object.getAttributeByName(LAST_LOGIN_DATE_ATTRIBUTE) != null) {
            myLastLoginDate = object.getAttributeByName(LAST_LOGIN_DATE_ATTRIBUTE).getValue().get(0).toString();
        }

        String myLastLoginDateCustomFormat = null;
        if (object.getAttributeByName(LAST_LOGIN_DATE_CUSTOM_FORMAT_ATTRIBUTE) != null) {
            myLastLoginDateCustomFormat = object.getAttributeByName(LAST_LOGIN_DATE_CUSTOM_FORMAT_ATTRIBUTE).getValue().get(0).toString();
        }

        object = connector.getObject(ObjectClass.ACCOUNT, new Uid(JOHN_UID), null);
        assertAttributes(object, current, myLastLoginDate, myLastLoginDateCustomFormat);
    }

    private void assertAttributes(ConnectorObject object, Long lastLoginDate, String myLastLoginDate, String myLastLoginDateCustomFormat) {
        assertNotNull(object);

        Attribute lastLoginDateAttribute = object.getAttributeByName(PredefinedAttributes.LAST_LOGIN_DATE_NAME);
        if (lastLoginDate != null) {
            assertNotNull(lastLoginDateAttribute);
            assertEquals(lastLoginDate, lastLoginDateAttribute.getValue().get(0));
        } else {
            assertNull(lastLoginDateAttribute);
        }

        Attribute myLastLoginDateAttribute = object.getAttributeByName(LAST_LOGIN_DATE_ATTRIBUTE);
        if (myLastLoginDate != null) {
            assertNotNull(myLastLoginDateAttribute);
            assertEquals(myLastLoginDate, myLastLoginDateAttribute.getValue().get(0));
        } else {
            assertNull(myLastLoginDateAttribute);
        }

        Attribute myLastLoginDateCustomFormatAttribute = object.getAttributeByName(LAST_LOGIN_DATE_CUSTOM_FORMAT_ATTRIBUTE);
        if (myLastLoginDateCustomFormat != null) {
            assertNotNull(myLastLoginDateCustomFormatAttribute);
            assertEquals(myLastLoginDateCustomFormat, myLastLoginDateCustomFormatAttribute.getValue().get(0));
        } else {
            assertNull(myLastLoginDateCustomFormatAttribute);
        }
    }

    @Test
    public void withLastLoginDateCustomFormat() throws Exception {
        ConnectorFacade connector = setupConnectorFacade(LAST_LOGIN_DATE_CUSTOM_FORMAT_ATTRIBUTE, LAST_LOGIN_DATE_FORMAT);

        Schema schema = connector.schema();
        assertSchema(schema, LAST_LOGIN_DATE_CUSTOM_FORMAT_ATTRIBUTE);

        ConnectorObject object = connector.getObject(ObjectClass.ACCOUNT, new Uid(JOHN_UID), null);
        assertAttributes(object, LAST_LOGIN_DATE_VALUE, Long.toString(LAST_LOGIN_DATE_VALUE), null);

        updateAndAssertLastLoginDate(connector, object);
    }

    private void assertSchema(Schema schema, String lastLoginDateAttribute) {
        ObjectClassInfo account = schema.findObjectClassInfo(ObjectClass.ACCOUNT_NAME);
        assertNotNull(account);

        Set<AttributeInfo> attributes = account.getAttributeInfo();
        assertNotNull(attributes);

        assertEquals(7, attributes.size());

        if (lastLoginDateAttribute == null) {
            assertTrue(attributes.stream().noneMatch(a -> PredefinedAttributes.LAST_LOGIN_DATE_NAME.equals(a.getName())));
            assertEquals(1L, attributes.stream().filter(a -> LAST_LOGIN_DATE_ATTRIBUTE.equals(a.getName())).count());
            assertEquals(1L, attributes.stream().filter(a -> LAST_LOGIN_DATE_CUSTOM_FORMAT_ATTRIBUTE.equals(a.getName())).count());
        } else {
            assertEquals(1L, attributes.stream().filter(a -> PredefinedAttributes.LAST_LOGIN_DATE_NAME.equals(a.getName())).count());
            assertTrue(attributes.stream().noneMatch(a -> lastLoginDateAttribute.equals(a.getName())));
        }
    }

    private ConnectorFacade setupConnectorFacade(String lastLoginDateAttribute, String lastLoginDateFormat)
            throws IOException {

        CsvConfiguration config = createConfiguration();
        config.setUniqueAttribute("uid");
        config.setLastLoginDateAttribute(lastLoginDateAttribute);
        config.setLastLoginDateFormat(lastLoginDateFormat);
        return setupConnector("/last-login-date.csv", config);
    }
}
