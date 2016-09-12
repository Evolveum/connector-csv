package com.evolveum.polygon.connector.csv;

import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.identityconnectors.framework.common.exceptions.ConnectorIOException;
import org.identityconnectors.framework.common.objects.AttributeInfo;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.ObjectClassInfo;
import org.identityconnectors.framework.common.objects.Schema;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Iterator;
import java.util.Set;

import static org.testng.AssertJUnit.*;

/**
 * Created by Viliam Repan (lazyman).
 */
public class SchemaOpTest extends BaseTest {

    @Test(expectedExceptions = ConnectorIOException.class)
    public void deleted() throws Exception {
        ConnectorFacade connector = setupConnector("/schema-good.csv");

        connector.schema();

        new File(CSV_FILE_PATH).delete();

        connector.schema();
    }

    @Test(expectedExceptions = ConfigurationException.class)
    public void emptySchema() throws Exception {
        ConnectorFacade connector = setupConnector("/schema-empty.csv");

        connector.schema();
    }

    @Test(expectedExceptions = ConfigurationException.class)
    public void badPwdFileSchema() throws Exception {
        ConnectorFacade connector = setupConnector("/schema-bad-pwd.csv");

        connector.schema();
    }

    @Test(expectedExceptions = ConfigurationException.class)
    public void badUniqueFileSchema() throws Exception {
        CsvConfiguration config = new CsvConfiguration();
        config.setUniqueAttribute("uid");

        ConnectorFacade connector = setupConnector("/schema-bad-unique.csv", config);

        connector.schema();
    }

    @Test
    public void goodFileSchema() throws Exception {
        ConnectorFacade connector = setupConnector("/schema-good.csv");

        Schema schema = connector.schema();
        assertNotNull(schema);
        Set<ObjectClassInfo> objClassInfos = schema.getObjectClassInfo();
        assertNotNull(objClassInfos);
        assertEquals(1, objClassInfos.size());

        ObjectClassInfo info = objClassInfos.iterator().next();
        assertNotNull(info);
        assertEquals(ObjectClass.ACCOUNT.getObjectClassValue(), info.getType());
        assertFalse(info.isContainer());
        Set<AttributeInfo> attrInfos = info.getAttributeInfo();
        assertNotNull(attrInfos);
        assertEquals(4, attrInfos.size());

        testAttribute("firstName", attrInfos, false, false);
        testAttribute("lastName", attrInfos, false, false);
        testAttribute("__NAME__", attrInfos, true, false);
        testAttribute("__PASSWORD__", attrInfos, false, true);
    }

    @Test
    public void uniqueDifferentThanNameSchema() throws Exception {
        CsvConfiguration config = new CsvConfiguration();
        config.setUniqueAttribute("uid");
        config.setNameAttribute("lastName");
        config.setPasswordAttribute("password");

        ConnectorFacade connector = setupConnector("/schema-good.csv", config);

        Schema schema = connector.schema();
        assertNotNull(schema);
        Set<ObjectClassInfo> objClassInfos = schema.getObjectClassInfo();
        assertNotNull(objClassInfos);
        assertEquals(1, objClassInfos.size());

        ObjectClassInfo info = objClassInfos.iterator().next();
        assertNotNull(info);
        assertEquals(ObjectClass.ACCOUNT.getObjectClassValue(), info.getType());
        assertFalse(info.isContainer());
        Set<AttributeInfo> attrInfos = info.getAttributeInfo();
        assertNotNull(attrInfos);
        assertEquals(5, attrInfos.size());

        testAttribute("firstName", attrInfos, false, false);
        testAttribute("lastName", attrInfos, false, false);
        testAttribute("uid", attrInfos, true, false);
        testAttribute("__NAME__", attrInfos, true, false);
        testAttribute("__PASSWORD__", attrInfos, false, true);
    }

    private void testAttribute(String name, Set<AttributeInfo> attrInfos, boolean unique, boolean password) {
        Iterator<AttributeInfo> iterator = attrInfos.iterator();

        boolean found = false;
        while (iterator.hasNext()) {
            AttributeInfo info = iterator.next();
            assertNotNull(info);

            if (!name.equals(info.getName())) {
                continue;
            }
            found = true;

            if (password) {
                assertEquals(GuardedString.class, info.getType());
            } else {
                assertEquals(String.class, info.getType());
            }

            if (unique) {
                assertTrue(info.isRequired());
            }
        }

        assertTrue(found);
    }
}
