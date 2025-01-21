package com.evolveum.polygon.connector.csv;

import com.evolveum.polygon.connector.csv.util.CsvTestUtil;

import org.apache.commons.io.FileUtils;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.exceptions.UnknownUidException;
import org.identityconnectors.framework.common.objects.*;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.evolveum.polygon.connector.csv.util.Util.ASSOC_ATTR_ACCESS;
import static com.evolveum.polygon.connector.csv.util.Util.ASSOC_ATTR_GROUP;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

/**
 * Created by Viliam Repan (lazyman).
 */
public class UpdateOpTest extends BaseTest {

    public static final String TEMPLATE_UPDATE = "/update.csv";

    public static final String VILO_UID = "vilo";
    public static final String VILO_LAST_NAME = "repan";
    public static final String VILO_FIRST_NAME = "viliam";
    public static final String VILO_PASSWORD = "Z29vZA==";


    public static final String USER_MEMBER_UID = "123";
    public static final String USER_MEMBER_LAST_NAME = "doe";
    public static final String USER_MEMBER_FIRST_NAME = "john";
    public static final String USER_MEMBER_PASSWORD = "qwe123";

    public static final String GROUP_MEMBER_UID_NEW = "2";
    public static final String GROUP_MEMBER_UPDATED_UID = "4";
    public static final String GROUP_MEMBER_UPDATED_DESCRIPTION = "accounting";
    public static final String GROUP_MEMBER_UPDATED_MEMBER_OF = "1";

    public static final String CHANGED_VALUE = "repantest";

    public static final String ACCESS_MEMBER_NEW_UID = "4";

    @Test(expectedExceptions = ConnectorException.class)
    public void badObjectClass() throws Exception {
        ConnectorFacade connector = setupConnector(TEMPLATE_UPDATE);

        connector.update(ObjectClass.GROUP, new Uid(VILO_UID), new HashSet<Attribute>(), null);
    }

    @Test(expectedExceptions = UnknownUidException.class)
    public void notExistingUid() throws Exception {
        ConnectorFacade connector = setupConnector(TEMPLATE_UPDATE);

        connector.update(ObjectClass.ACCOUNT, new Uid("unknown"), new HashSet<Attribute>(), null);
    }

    @Test(expectedExceptions = ConnectorException.class)
    public void updateNonExistingAttribute() throws Exception {
        ConnectorFacade connector = setupConnector(TEMPLATE_UPDATE);

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build("nonExisting", CHANGED_VALUE));
        connector.update(ObjectClass.ACCOUNT, new Uid(VILO_UID), attributes, null);
    }

    /**
     * uid column is not editable
     *
     * @throws Exception
     */
    @Test
    public void updateAttributeUid() throws Exception {
        ConnectorFacade connector = setupConnector(TEMPLATE_UPDATE, createConfigurationNameEqualsUid());

        Uid expected = new Uid(VILO_UID);

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build(ATTR_UID, CHANGED_VALUE));
        Uid real = connector.update(ObjectClass.ACCOUNT, expected, attributes, null);

        AssertJUnit.assertEquals(new Uid(CHANGED_VALUE), real);

        ConnectorObject object = connector.getObject(ObjectClass.ACCOUNT, real, null);
        assertNotNull(object);

        attributes = new HashSet<>();
        attributes.add(new Name(CHANGED_VALUE));
        attributes.add(createAttribute(Uid.NAME, CHANGED_VALUE));
        attributes.add(createAttribute(ATTR_FIRST_NAME, VILO_FIRST_NAME));
        attributes.add(createAttribute(ATTR_LAST_NAME, VILO_LAST_NAME));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(VILO_PASSWORD.toCharArray())));
        assertConnectorObject(attributes, object);

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_UID, CHANGED_VALUE);
        expectedRecord.put(ATTR_FIRST_NAME, VILO_FIRST_NAME);
        expectedRecord.put(ATTR_LAST_NAME, VILO_LAST_NAME);
        expectedRecord.put(ATTR_PASSWORD, VILO_PASSWORD);

        Map<String, String> realRecord = CsvTestUtil.findRecord(createConfigurationNameEqualsUid(), CHANGED_VALUE);
        assertEquals(expectedRecord, realRecord);
    }

    @Test
    public void updateNameAttribute() throws Exception {
        ConnectorFacade connector = setupConnector(TEMPLATE_UPDATE, createConfigurationNameEqualsUid());

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build(Name.NAME, CHANGED_VALUE));
        Uid real = connector.update(ObjectClass.ACCOUNT, new Uid(VILO_UID), attributes, null);

        AssertJUnit.assertEquals(new Uid(CHANGED_VALUE), real);

        ConnectorObject object = connector.getObject(ObjectClass.ACCOUNT, real, null);
        assertNotNull(object);

        attributes = new HashSet<>();
        attributes.add(new Name(CHANGED_VALUE));
        attributes.add(createAttribute(Uid.NAME, CHANGED_VALUE));
        attributes.add(createAttribute(ATTR_FIRST_NAME, VILO_FIRST_NAME));
        attributes.add(createAttribute(ATTR_LAST_NAME, VILO_LAST_NAME));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(VILO_PASSWORD.toCharArray())));
        assertConnectorObject(attributes, object);

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_UID, CHANGED_VALUE);
        expectedRecord.put(ATTR_FIRST_NAME, VILO_FIRST_NAME);
        expectedRecord.put(ATTR_LAST_NAME, VILO_LAST_NAME);
        expectedRecord.put(ATTR_PASSWORD, VILO_PASSWORD);

        Map<String, String> realRecord = CsvTestUtil.findRecord(createConfigurationNameEqualsUid(), CHANGED_VALUE);
        assertEquals(expectedRecord, realRecord);
    }

    @Test
    public void updateDifferentNameAttribute() throws Exception {
        ConnectorFacade connector = setupConnector(TEMPLATE_UPDATE, createConfigurationDifferent());

        Uid expected = new Uid(VILO_UID);

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build(Name.NAME, CHANGED_VALUE));
        Uid real = connector.update(ObjectClass.ACCOUNT, expected, attributes, null);

        AssertJUnit.assertEquals(expected, real);

        ConnectorObject object = connector.getObject(ObjectClass.ACCOUNT, real, null);
        assertNotNull(object);

        attributes = new HashSet<>();
        attributes.add(new Name(CHANGED_VALUE));
        attributes.add(createAttribute(Uid.NAME, VILO_UID));
        attributes.add(createAttribute(ATTR_FIRST_NAME, VILO_FIRST_NAME));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(VILO_PASSWORD.toCharArray())));
        assertConnectorObject(attributes, object);

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_UID, VILO_UID);
        expectedRecord.put(ATTR_FIRST_NAME, VILO_FIRST_NAME);
        expectedRecord.put(ATTR_LAST_NAME, CHANGED_VALUE);
        expectedRecord.put(ATTR_PASSWORD, VILO_PASSWORD);

        Map<String, String> realRecord = CsvTestUtil.findRecord(createConfigurationNameEqualsUid(), VILO_UID);
        assertEquals(expectedRecord, realRecord);
    }

    @Test
    public void updateOtherAttribute() throws Exception {
        ConnectorFacade connector = setupConnector(TEMPLATE_UPDATE, createConfigurationNameEqualsUid());

        Uid expected = new Uid(VILO_UID);

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build(ATTR_LAST_NAME, CHANGED_VALUE));
        Uid real = connector.update(ObjectClass.ACCOUNT, expected, attributes, null);

        AssertJUnit.assertEquals(expected, real);

        ConnectorObject object = connector.getObject(ObjectClass.ACCOUNT, real, null);
        assertNotNull(object);

        attributes = new HashSet<>();
        attributes.add(new Name(VILO_UID));
        attributes.add(createAttribute(Uid.NAME, VILO_UID));
        attributes.add(createAttribute(ATTR_FIRST_NAME, VILO_FIRST_NAME));
        attributes.add(createAttribute(ATTR_LAST_NAME, CHANGED_VALUE));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(VILO_PASSWORD.toCharArray())));
        assertConnectorObject(attributes, object);

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_UID, VILO_UID);
        expectedRecord.put(ATTR_FIRST_NAME, VILO_FIRST_NAME);
        expectedRecord.put(ATTR_LAST_NAME, CHANGED_VALUE);
        expectedRecord.put(ATTR_PASSWORD, VILO_PASSWORD);

        Map<String, String> realRecord = CsvTestUtil.findRecord(createConfigurationNameEqualsUid(), VILO_UID);
        assertEquals(expectedRecord, realRecord);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void updateUidAttributeWithTwoValues() throws Exception {
        ConnectorFacade connector = setupConnector(TEMPLATE_UPDATE, createConfigurationNameEqualsUid());

        Uid expected = new Uid(VILO_UID);

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build(ATTR_UID, "first", "second"));
        connector.update(ObjectClass.ACCOUNT, expected, attributes, null);
    }

    @Test(expectedExceptions = ConnectorException.class)
    public void updateDifferentOtherAttributeMultivalueNotDefined() throws Exception {
        ConnectorFacade connector = setupConnector(TEMPLATE_UPDATE, createConfigurationDifferent());

        Uid expected = new Uid(VILO_UID);

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build(ATTR_FIRST_NAME, CHANGED_VALUE, "second value"));
        connector.update(ObjectClass.ACCOUNT, expected, attributes, null);
    }

    @Test
    public void updateDifferentOtherAttributeMultivalue() throws Exception {
        CsvConfiguration config = createConfigurationDifferent();
        config.setMultivalueDelimiter(",");

        ConnectorFacade connector = setupConnector(TEMPLATE_UPDATE, config);

        Uid expected = new Uid(VILO_UID);

        final String SECOND_VALUE = "second value";

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build(ATTR_FIRST_NAME, CHANGED_VALUE, SECOND_VALUE));
        Uid real = connector.update(ObjectClass.ACCOUNT, expected, attributes, null);

        assertEquals(expected, real);

        ConnectorObject object = connector.getObject(ObjectClass.ACCOUNT, real, null);
        assertNotNull(object);

        attributes = new HashSet<>();
        attributes.add(new Name(VILO_LAST_NAME));
        attributes.add(createAttribute(Uid.NAME, VILO_UID));
        attributes.add(createAttribute(ATTR_FIRST_NAME, CHANGED_VALUE, SECOND_VALUE));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(VILO_PASSWORD.toCharArray())));
        assertConnectorObject(attributes, object);

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_UID, VILO_UID);
        expectedRecord.put(ATTR_FIRST_NAME, CHANGED_VALUE + "," + SECOND_VALUE);
        expectedRecord.put(ATTR_LAST_NAME, VILO_LAST_NAME );
        expectedRecord.put(ATTR_PASSWORD, VILO_PASSWORD);

        Map<String, String> realRecord = CsvTestUtil.findRecord(createConfigurationNameEqualsUid(), VILO_UID);
        assertEquals(expectedRecord, realRecord);
    }

    @Test
    public void updateReferenceAttributeOnSubjectDifferentOc() throws Exception {
        CsvConfiguration config = createConfigurationNameEqualsUid();
        config.setMultivalueDelimiter(",");
        config.setMultivalueAttributes("memberOf");

        Set<String> values =Set.of(
                "\"account\"+memberOf -# \"group\"+id",
                "\"group\"+memberOf -# \"group\"+id"
        );

        config.setManagedAssociationPairs(values.toArray(new String[values.size()]));

        File groupsProperties = new File("./target/groups-memberOf.properties");
        groupsProperties.delete();
        config.setObjectClassDefinition(groupsProperties);
        FileUtils.copyFile(new File(TEMPLATE_FOLDER_PATH + "/groups-memberOf.properties"), groupsProperties);

        File groupsCsv = new File("./target/groups-memberOf.csv");
        groupsCsv.delete();
        FileUtils.copyFile(new File(TEMPLATE_FOLDER_PATH + "/groups-memberOf.csv"), groupsCsv);

        ConnectorFacade connector = setupConnector("/create-memberOf.csv", config);

        Uid expected = new Uid(USER_MEMBER_UID);

        ConnectorObjectReference reference = new ConnectorObjectReference(buildConnectorObject(GROUP_MEMBER_UID_NEW,
                GROUP_MEMBER_UID_NEW, null, new ObjectClass("group")));

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build(ASSOC_ATTR_GROUP, reference));
        Uid real = connector.update(ObjectClass.ACCOUNT, expected, attributes, null);

        assertEquals(expected, real);

        ConnectorObject object = connector.getObject(ObjectClass.ACCOUNT, real, null);
        assertNotNull(object);

        attributes = new HashSet<>();
        attributes.add(new Name(USER_MEMBER_UID));
        attributes.add(createAttribute(Uid.NAME, USER_MEMBER_UID));
        attributes.add(createAttribute(ASSOC_ATTR_GROUP, reference));
        attributes.add(createAttribute(ATTR_FIRST_NAME, USER_MEMBER_FIRST_NAME));
        attributes.add(createAttribute(ATTR_LAST_NAME, USER_MEMBER_LAST_NAME));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(USER_MEMBER_PASSWORD.toCharArray())));
        assertConnectorObject(attributes, object);

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_UID, USER_MEMBER_UID);
        expectedRecord.put(ATTR_FIRST_NAME, USER_MEMBER_FIRST_NAME);
        expectedRecord.put(ATTR_LAST_NAME, USER_MEMBER_LAST_NAME );
        expectedRecord.put(ATTR_PASSWORD, USER_MEMBER_PASSWORD);
        expectedRecord.put(ATTR_MEMBER_OF, GROUP_MEMBER_UID_NEW);

        Map<String, String> realRecord = CsvTestUtil.findRecord(createConfigurationNameEqualsUid(), USER_MEMBER_UID);
        assertEquals(expectedRecord, realRecord);
    }

    @Test
    public void updateReferenceAttributeOnSubjectSameOc() throws Exception {
        CsvConfiguration config = createConfigurationNameEqualsUid();
        config.setMultivalueDelimiter(",");
        config.setMultivalueAttributes("memberOf");

        Set<String> values =Set.of(
                "\"account\"+memberOf -# \"group\"+id",
                "\"group\"+memberOf -# \"group\"+id"
        );

        config.setManagedAssociationPairs(values.toArray(new String[values.size()]));

        File groupsProperties = new File("./target/groups-memberOf.properties");
        groupsProperties.delete();
        config.setObjectClassDefinition(groupsProperties);
        FileUtils.copyFile(new File(TEMPLATE_FOLDER_PATH + "/groups-memberOf.properties"), groupsProperties);

        File groupsCsv = new File("./target/groups-memberOf.csv");
        groupsCsv.delete();
        FileUtils.copyFile(new File(TEMPLATE_FOLDER_PATH + "/groups-memberOf.csv"), groupsCsv);

        ConnectorFacade connector = setupConnector("/create-memberOf.csv", config);

        Uid expected = new Uid("4");


        ConnectorObjectReference reference = new ConnectorObjectReference(buildConnectorObject(NEW_REFERENCE_ID,
                NEW_REFERENCE_ID, null, new ObjectClass("group")));

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build(ASSOC_ATTR_GROUP, reference));
        Uid real = connector.update(new ObjectClass("group"), expected, attributes, null);

        assertEquals(expected, real);

        ConnectorObject object = connector.getObject(new ObjectClass("group"), real, null);
        assertNotNull(object);

        attributes = new HashSet<>();
        attributes.add(new Name(GROUP_MEMBER_UPDATED_UID));
        attributes.add(createAttribute(Uid.NAME, GROUP_MEMBER_UPDATED_UID));
        attributes.add(createAttribute(ASSOC_ATTR_GROUP, reference));
        attributes.add(createAttribute(ATTR_DESCRIPTION, GROUP_MEMBER_UPDATED_DESCRIPTION));
        assertConnectorObject(attributes, object);

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_ID, GROUP_MEMBER_UPDATED_UID);
        expectedRecord.put(ATTR_DESCRIPTION, GROUP_MEMBER_UPDATED_DESCRIPTION);
        expectedRecord.put(ATTR_MEMBER_OF, GROUP_MEMBER_UPDATED_MEMBER_OF);

        Map<String, String> realRecord = CsvTestUtil.findRecord(createConfigurationReferencedObjectClass(
                "./target/groups-memberOf.csv", ATTR_ID), ATTR_ID, GROUP_MEMBER_UPDATED_UID);
        assertEquals(expectedRecord, realRecord);
    }

    @Test
    public void updateReferenceAttributeOnObjectComplex() throws Exception {
        CsvConfiguration config = createConfiguration();
        config.setUniqueAttribute("id");
        config.setNameAttribute("id");
        config.setTrim(true);
        config.setPasswordAttribute(null);

        Set<String> values = Set.of(
                "\"account\"+id -# \"group\"+members-test",
                "\"account\"+id -# \"group\"+members-default",
                "\"account\"+id -# \"group\"+members-admin"
        );

        config.setManagedAssociationPairs(values.toArray(new String[values.size()]));

        File groupsProperties = new File("./target/groupsAccessParameters.properties");
        groupsProperties.delete();
        config.setObjectClassDefinition(groupsProperties);
        FileUtils.copyFile(new File(TEMPLATE_FOLDER_PATH + "/groupsAccessParameters.properties"), groupsProperties);
        File groupsCsv = new File("./target/groups-access.csv");
        groupsCsv.delete();
        FileUtils.copyFile(new File(TEMPLATE_FOLDER_PATH + "/groups-access.csv"), groupsCsv);

        ConnectorFacade connector = setupConnector("/schema-user-basic.csv", config);

        String valueUserIdUpdateAccessOnObject = "2";
        Uid expected = new Uid(valueUserIdUpdateAccessOnObject);

        Set<Attribute> referenceAttributes = new HashSet<>();
        referenceAttributes.add(createAttribute(Uid.NAME, NEW_REFERENCE_ID));
        referenceAttributes.add(createAttribute(Name.NAME, "users-all"));
        referenceAttributes.add(createAttribute(ATTR_DESCRIPTION, "ua"));
        referenceAttributes.add(createAttribute(ATTR_MEMBERS_TEST, valueUserIdUpdateAccessOnObject));
        referenceAttributes.add(createAttribute(ATTR_MEMBERS_DEFAULT, valueUserIdUpdateAccessOnObject));
        referenceAttributes.add(createAttribute(ATTR_MEMBERS_ADMIN, valueUserIdUpdateAccessOnObject));

        ConnectorObjectReference reference = new ConnectorObjectReference(buildConnectorObject(NEW_REFERENCE_ID,
                NEW_REFERENCE_ID, referenceAttributes, new ObjectClass("group")));

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build(ASSOC_ATTR_GROUP, reference));
        Uid real = connector.update(ObjectClass.ACCOUNT, expected, attributes, null);

        assertEquals(expected, real);

        ConnectorObject object = connector.getObject(ObjectClass.ACCOUNT, real, null);
        assertNotNull(object);

        attributes = new HashSet<>();
        attributes.add(new Name(valueUserIdUpdateAccessOnObject));
        attributes.add(createAttribute(Uid.NAME, valueUserIdUpdateAccessOnObject));
        attributes.add(createAttribute(ATTR_NAME, "jack"));
        attributes.add(createAttribute(ASSOC_ATTR_GROUP, reference));
        attributes.add(createAttribute(ATTR_EMPL, "234"));
        attributes.add(createAttribute(ATTR_TITLE, "manager"));
        assertConnectorObject(attributes, object);

        assertReferenceAndReturnReferenceObject(referenceAttributes,
                object.getAttributeByName(ASSOC_ATTR_GROUP), new Uid(NEW_REFERENCE_ID));

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_ID, valueUserIdUpdateAccessOnObject);
        expectedRecord.put(ATTR_EMPL, "234");
        expectedRecord.put(ATTR_NAME, "jack");
        expectedRecord.put(ATTR_TITLE, "manager");

        Map<String, String> realRecord = CsvTestUtil.findRecord(config, ATTR_ID,
                valueUserIdUpdateAccessOnObject);
        assertEquals(expectedRecord, realRecord);
    }

    @Test
    public void updateReferenceAttributeOnAccessComplex() throws Exception {
        CsvConfiguration config = createConfiguration();
        config.setUniqueAttribute("id");
        config.setNameAttribute("id");
        config.setTrim(true);
        config.setPasswordAttribute(null);

        Set<String> values =Set.of(
                "\"account\"+id -# \"access\"+subject_id",
                "\"access\"+object_id #- \"group\"+id"
        );

        config.setManagedAssociationPairs(values.toArray(new String[values.size()]));

        File groupsProperties = new File("./target/groupsAndAccessObjectClass.properties");
        groupsProperties.delete();
        config.setObjectClassDefinition(groupsProperties);
        FileUtils.copyFile(new File(TEMPLATE_FOLDER_PATH + "/groupsAndAccessObjectClass.properties"), groupsProperties);
        File groupsCsv = new File("./target/groups-no-member.csv");
        groupsCsv.delete();
        FileUtils.copyFile(new File(TEMPLATE_FOLDER_PATH + "/groups-no-member.csv"), groupsCsv);

        File accessCsv = new File("./target/access.csv");
        accessCsv.delete();
        FileUtils.copyFile(new File(TEMPLATE_FOLDER_PATH + "/access.csv"), accessCsv);

        ConnectorFacade connector = setupConnector("/schema-user-basic.csv", config);

        String valueUserIdUpdateAccessOnObject = "2";
        Uid expected = new Uid(valueUserIdUpdateAccessOnObject);

        Set<Attribute> secondLvlReferenceObjectAttributes = new HashSet<>();
        secondLvlReferenceObjectAttributes.add(createAttribute(Uid.NAME, "2"));
        secondLvlReferenceObjectAttributes.add(createAttribute(Name.NAME, "guests"));

        ConnectorObjectReference connectorObjectReferenceSecondLvl = new ConnectorObjectReference(buildConnectorObject("2",
                "2", secondLvlReferenceObjectAttributes, new ObjectClass("group")));

        Set<Attribute> referenceAttributes = new HashSet<>();
        referenceAttributes.add(createAttribute(Uid.NAME, NEW_REFERENCE_ID));
        referenceAttributes.add(createAttribute(Name.NAME, NEW_REFERENCE_ID));
        referenceAttributes.add(createAttribute(ATTR_LEVEL, "test"));
        referenceAttributes.add(createAttribute(ATTR_DESCRIPTION, "test accounts"));
        referenceAttributes.add(createAttribute(ATTR_SUBJECT_ID, valueUserIdUpdateAccessOnObject));
        referenceAttributes.add(createAttribute(ASSOC_ATTR_GROUP, connectorObjectReferenceSecondLvl));

        ConnectorObjectReference connectorObjectReference = new ConnectorObjectReference(buildConnectorObject(NEW_REFERENCE_ID,
                NEW_REFERENCE_ID, referenceAttributes, new ObjectClass("access")));

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build(ASSOC_ATTR_ACCESS, connectorObjectReference));
        Uid real = connector.update(ObjectClass.ACCOUNT, expected, attributes, null);

        assertEquals(expected, real);

        ConnectorObject object = connector.getObject(ObjectClass.ACCOUNT, real, null);
        assertNotNull(object);

        attributes = new HashSet<>();
        attributes.add(new Name(valueUserIdUpdateAccessOnObject));
        attributes.add(createAttribute(Uid.NAME, valueUserIdUpdateAccessOnObject));
        attributes.add(createAttribute(ATTR_NAME, "jack"));
        attributes.add(createAttribute(ASSOC_ATTR_ACCESS, connectorObjectReference));
        attributes.add(createAttribute(ATTR_EMPL, "234"));
        attributes.add(createAttribute(ATTR_TITLE, "manager"));
        assertConnectorObject(attributes, object);

        BaseConnectorObject secondLvlReferenceObject = assertReferenceAndReturnReferenceObject(referenceAttributes,
                object.getAttributeByName(ASSOC_ATTR_ACCESS), new Uid (NEW_REFERENCE_ID));
        assertNotNull(secondLvlReferenceObject);

        assertReferenceAndReturnReferenceObject(secondLvlReferenceObjectAttributes,
                secondLvlReferenceObject.getAttributeByName(ASSOC_ATTR_GROUP));

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_ID, valueUserIdUpdateAccessOnObject);
        expectedRecord.put(ATTR_EMPL, "234");
        expectedRecord.put(ATTR_NAME, "jack");
        expectedRecord.put(ATTR_TITLE, "manager");

        Map<String, String> realRecord = CsvTestUtil.findRecord(config, ATTR_ID,
                valueUserIdUpdateAccessOnObject);
        assertEquals(expectedRecord, realRecord);
    }
}
