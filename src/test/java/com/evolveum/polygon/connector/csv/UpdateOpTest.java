package com.evolveum.polygon.connector.csv;

import org.identityconnectors.common.Base64;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.exceptions.UnknownUidException;
import org.identityconnectors.framework.common.objects.*;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Set;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

/**
 * Created by Viliam Repan (lazyman).
 */
public class UpdateOpTest extends BaseTest {

    private ConnectorFacade connector;

    @BeforeMethod
    public void before() throws Exception {
        connector = setupConnector("/update.csv");
    }

    @AfterMethod
    public void after() {
        connector = null;
    }

    @Test(expectedExceptions = ConnectorException.class)
    public void badObjectClass() {
        connector.update(ObjectClass.GROUP, new Uid("vilo"), new HashSet<Attribute>(), null);
    }

    @Test(expectedExceptions = UnknownUidException.class)
    public void notExistingUid() {
        connector.update(ObjectClass.ACCOUNT, new Uid("unknown"), new HashSet<Attribute>(), null);
    }

    @Test(expectedExceptions = ConnectorException.class)
    public void updateNonExistingAttribute() throws Exception {
        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build("nonExisting", "repantest"));
        connector.update(ObjectClass.ACCOUNT, new Uid("vilo"), attributes, null);
    }

    @Test
    public void updateAttributeDelete() throws Exception {
        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build("lastName"));
        Uid uid = connector.update(ObjectClass.ACCOUNT, new Uid("vilo"), attributes, null);
        assertNotNull(uid);
        assertEquals("vilo", uid.getUidValue());

        ConnectorObject newObject = connector.getObject(ObjectClass.ACCOUNT, uid, null);
        assertNotNull(newObject);

        attributes.clear();
        attributes.add(createAttribute("firstName", "vilo"));
        attributes.add(createAttribute(Uid.NAME, AttributeUtil.getStringValue(uid)));
        attributes.add(createAttribute(Name.NAME, AttributeUtil.getStringValue(uid)));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(Base64.encode("asdf".getBytes()).toCharArray())));

        assertConnectorObject(attributes, newObject);
    }

    @Test
    public void updateAttributeAdd() throws Exception {
        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build("lastName", "repantest"));
        Uid uid = connector.update(ObjectClass.ACCOUNT, new Uid("vilo"), attributes, null);
        assertNotNull(uid);
        assertEquals("vilo", uid.getUidValue());

//        String result = TestUtils.compareFiles(TestUtils.getTestFile("update.csv"),
//                TestUtils.getTestFile("update-result-add.csv"));
//        assertNull(result, "File updated incorrectly: " + result);
    }

    @Test
    public void renameWhenUniqueEqualsNamingAttribute() throws Exception {
        Set<Attribute> attributes = new HashSet<>();

        attributes.add(new Name("troll"));
        Uid uid = connector.update(ObjectClass.ACCOUNT, new Uid("vilo"), attributes, null);
        assertNotNull(uid);
        assertEquals(uid.getUidValue(), "troll");

//        String result = TestUtils.compareFiles(TestUtils.getTestFile("update.csv"),
//                TestUtils.getTestFile("update-result-rename.csv"));
//        assertNull(result, "File updated incorrectly: " + result);
    }
}
