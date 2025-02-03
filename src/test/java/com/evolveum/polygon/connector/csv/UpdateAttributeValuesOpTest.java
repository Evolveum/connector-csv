package com.evolveum.polygon.connector.csv;

import com.evolveum.polygon.connector.csv.util.CsvTestUtil;
import org.apache.commons.io.FileUtils;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.*;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.io.File;
import java.util.*;

import static com.evolveum.polygon.connector.csv.util.Util.ASSOC_ATTR_ACCESS;
import static com.evolveum.polygon.connector.csv.util.Util.ASSOC_ATTR_GROUP;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

/**
 * Created by Viliam Repan (lazyman).
 */
public class UpdateAttributeValuesOpTest extends UpdateOpTest {

    @Test(expectedExceptions = ConnectorException.class)
    public void addValueToAttributeDelimiterUndefined() throws Exception {
        ConnectorFacade connector = setupConnector(TEMPLATE_UPDATE, createConfigurationNameEqualsUid());

        Uid expected = new Uid(VILO_UID);

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build(ATTR_LAST_NAME, CHANGED_VALUE));
        connector.addAttributeValues(ObjectClass.ACCOUNT, expected, attributes, null);
    }

    @Test
    public void addValueToAttribute() throws Exception {
        CsvConfiguration config = createConfigurationNameEqualsUid();
        config.setMultivalueDelimiter(",");
        ConnectorFacade connector = setupConnector(TEMPLATE_UPDATE, config);

        Uid expected = new Uid(VILO_UID);

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build(ATTR_LAST_NAME, CHANGED_VALUE));
        Uid real = connector.addAttributeValues(ObjectClass.ACCOUNT, expected, attributes, null);

        AssertJUnit.assertEquals(expected, real);

        ConnectorObject object = connector.getObject(ObjectClass.ACCOUNT, real, null);
        assertNotNull(object);

        attributes = new HashSet<>();
        attributes.add(new Name(VILO_UID));
        attributes.add(createAttribute(Uid.NAME, VILO_UID));
        attributes.add(createAttribute(ATTR_FIRST_NAME, VILO_FIRST_NAME));
        attributes.add(createAttribute(ATTR_LAST_NAME, VILO_LAST_NAME, CHANGED_VALUE));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(VILO_PASSWORD.toCharArray())));
        assertConnectorObject(attributes, object);

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_UID, VILO_UID);
        expectedRecord.put(ATTR_FIRST_NAME, VILO_FIRST_NAME);
        expectedRecord.put(ATTR_LAST_NAME, VILO_LAST_NAME + "," + CHANGED_VALUE);
        expectedRecord.put(ATTR_PASSWORD, VILO_PASSWORD);

        Map<String, String> realRecord = CsvTestUtil.findRecord(createConfigurationNameEqualsUid(), VILO_UID);
        assertEquals(expectedRecord, realRecord);
    }

    @Test
    public void addDuplicateValueToAttribute() throws Exception {
        CsvConfiguration config = createConfigurationNameEqualsUid();
        config.setMultivalueDelimiter(",");
        ConnectorFacade connector = setupConnector(TEMPLATE_UPDATE, config);

        Uid expected = new Uid(VILO_UID);

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build(ATTR_LAST_NAME, VILO_LAST_NAME));
        Uid real = connector.addAttributeValues(ObjectClass.ACCOUNT, expected, attributes, null);

        AssertJUnit.assertEquals(expected, real);

        ConnectorObject object = connector.getObject(ObjectClass.ACCOUNT, real, null);
        assertNotNull(object);

        attributes = new HashSet<>();
        attributes.add(new Name(VILO_UID));
        attributes.add(createAttribute(Uid.NAME, VILO_UID));
        attributes.add(createAttribute(ATTR_FIRST_NAME, VILO_FIRST_NAME));
        attributes.add(createAttribute(ATTR_LAST_NAME, VILO_LAST_NAME));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(VILO_PASSWORD.toCharArray())));
        assertConnectorObject(attributes, object);

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_UID, VILO_UID);
        expectedRecord.put(ATTR_FIRST_NAME, VILO_FIRST_NAME);
        expectedRecord.put(ATTR_LAST_NAME, VILO_LAST_NAME);
        expectedRecord.put(ATTR_PASSWORD, VILO_PASSWORD);

        Map<String, String> realRecord = CsvTestUtil.findRecord(createConfigurationNameEqualsUid(), VILO_UID);
        assertEquals(expectedRecord, realRecord);
    }

    @Test
    public void removeValueFromAttribute() throws Exception {
        CsvConfiguration config = createConfigurationNameEqualsUid();
        config.setMultivalueDelimiter(",");
        ConnectorFacade connector = setupConnector(TEMPLATE_UPDATE, config);

        Uid expected = new Uid(VILO_UID);

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build(ATTR_LAST_NAME, VILO_LAST_NAME));
        Uid real = connector.removeAttributeValues(ObjectClass.ACCOUNT, expected, attributes, null);

        AssertJUnit.assertEquals(expected, real);

        ConnectorObject object = connector.getObject(ObjectClass.ACCOUNT, real, null);
        assertNotNull(object);

        attributes = new HashSet<>();
        attributes.add(new Name(VILO_UID));
        attributes.add(createAttribute(Uid.NAME, VILO_UID));
        attributes.add(createAttribute(ATTR_FIRST_NAME, VILO_FIRST_NAME));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(VILO_PASSWORD.toCharArray())));
        assertConnectorObject(attributes, object);

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_UID, VILO_UID);
        expectedRecord.put(ATTR_FIRST_NAME, VILO_FIRST_NAME);
        expectedRecord.put(ATTR_LAST_NAME, "");
        expectedRecord.put(ATTR_PASSWORD, VILO_PASSWORD);

        Map<String, String> realRecord = CsvTestUtil.findRecord(createConfigurationNameEqualsUid(), VILO_UID);
        assertEquals(expectedRecord, realRecord);
    }

    @Test
    public void removeNonExistingValueFromAttribute() throws Exception {
        CsvConfiguration config = createConfigurationNameEqualsUid();
        config.setMultivalueDelimiter(",");
        ConnectorFacade connector = setupConnector(TEMPLATE_UPDATE, config);

        Uid expected = new Uid(VILO_UID);

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build(ATTR_LAST_NAME, "unexisting value"));
        Uid real = connector.removeAttributeValues(ObjectClass.ACCOUNT, expected, attributes, null);

        AssertJUnit.assertEquals(expected, real);

        ConnectorObject object = connector.getObject(ObjectClass.ACCOUNT, real, null);
        assertNotNull(object);

        attributes = new HashSet<>();
        attributes.add(new Name(VILO_UID));
        attributes.add(createAttribute(Uid.NAME, VILO_UID));
        attributes.add(createAttribute(ATTR_FIRST_NAME, VILO_FIRST_NAME));
        attributes.add(createAttribute(ATTR_LAST_NAME, VILO_LAST_NAME));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(VILO_PASSWORD.toCharArray())));
        assertConnectorObject(attributes, object);

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_UID, VILO_UID);
        expectedRecord.put(ATTR_FIRST_NAME, VILO_FIRST_NAME);
        expectedRecord.put(ATTR_LAST_NAME, VILO_LAST_NAME);
        expectedRecord.put(ATTR_PASSWORD, VILO_PASSWORD);

        Map<String, String> realRecord = CsvTestUtil.findRecord(createConfigurationNameEqualsUid(), VILO_UID);
        assertEquals(expectedRecord, realRecord);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void addValueFromUniqueAttribute() throws Exception {
        CsvConfiguration config = createConfigurationNameEqualsUid();
        config.setMultivalueDelimiter(",");
        ConnectorFacade connector = setupConnector(TEMPLATE_UPDATE, config);

        Uid expected = new Uid(VILO_UID);

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build(ATTR_UID, "second value"));
        connector.addAttributeValues(ObjectClass.ACCOUNT, expected, attributes, null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void removeValueFromUniqueAttribute() throws Exception {
        CsvConfiguration config = createConfigurationNameEqualsUid();
        config.setMultivalueDelimiter(",");
        ConnectorFacade connector = setupConnector(TEMPLATE_UPDATE, config);

        Uid expected = new Uid(VILO_UID);

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build(ATTR_UID, VILO_UID));
        connector.removeAttributeValues(ObjectClass.ACCOUNT, expected, attributes, null);
    }

    @Test
    public void addReferenceAttributeOnSubjectDifferentOc() throws Exception {
        CsvConfiguration config = createConfigurationNameEqualsUid();
        config.setMultivalueDelimiter(",");
        config.setMultivalueAttributes("memberOf");

        Set<String> values = Set.of(
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

        Uid expected = new Uid(USER_MEMBER_ID);

        Set<Attribute> referenceAttributes = new HashSet<>();
        referenceAttributes.add(new Name(GROUP_MEMBER_UID_NEW));

        ConnectorObjectReference reference = new ConnectorObjectReference(buildConnectorObject(GROUP_MEMBER_UID_NEW,
                GROUP_MEMBER_UID_NEW, referenceAttributes, new ObjectClass("group")));


        Set<Attribute> origReferenceAttributes = new HashSet<>();
        origReferenceAttributes.add(new Name("1"));

        ConnectorObjectReference origReference = new ConnectorObjectReference(buildConnectorObject("1",
                "1", origReferenceAttributes, new ObjectClass("group")));

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build(ASSOC_ATTR_GROUP, reference));
        Uid real = connector.addAttributeValues(ObjectClass.ACCOUNT, expected, attributes, null);

        assertEquals(expected, real);

        ConnectorObject object = connector.getObject(ObjectClass.ACCOUNT, real, null);
        assertNotNull(object);

        attributes = new HashSet<>();
        attributes.add(new Name(USER_MEMBER_ID));
        attributes.add(createAttribute(Uid.NAME, USER_MEMBER_ID));
        attributes.add(createAttribute(ASSOC_ATTR_GROUP, reference, origReference));
        attributes.add(createAttribute(ATTR_FIRST_NAME, USER_MEMBER_FIRST_NAME));
        attributes.add(createAttribute(ATTR_LAST_NAME, USER_MEMBER_LAST_NAME));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(USER_MEMBER_PASSWORD.toCharArray())));
        assertConnectorObject(attributes, object, Name.NAME);

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_UID, USER_MEMBER_ID);
        expectedRecord.put(ATTR_FIRST_NAME, USER_MEMBER_FIRST_NAME);
        expectedRecord.put(ATTR_LAST_NAME, USER_MEMBER_LAST_NAME);
        expectedRecord.put(ATTR_PASSWORD, USER_MEMBER_PASSWORD);
        expectedRecord.put(ATTR_MEMBER_OF, "1," + GROUP_MEMBER_UID_NEW);

        Map<String, String> realRecord = CsvTestUtil.findRecord(createConfigurationNameEqualsUid(), USER_MEMBER_ID);
        assertEquals(expectedRecord, realRecord);
    }

    @Test
    public void removeReferenceAttributeOnSubjectDifferentOc() throws Exception {
        CsvConfiguration config = createConfigurationNameEqualsUid();
        config.setMultivalueDelimiter(",");
        config.setMultivalueAttributes("memberOf");

        Set<String> values = Set.of(
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

        Uid expected = new Uid(USER_MEMBER_ID);

        Set<Attribute> referenceAttributes = new HashSet<>();
        referenceAttributes.add(new Name("1"));

        ConnectorObjectReference origReference = new ConnectorObjectReference(buildConnectorObject("1",
                "1", referenceAttributes, new ObjectClass("group")));

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build(ASSOC_ATTR_GROUP, origReference));
        Uid real = connector.removeAttributeValues(ObjectClass.ACCOUNT, expected, attributes, null);

        assertEquals(expected, real);

        ConnectorObject object = connector.getObject(ObjectClass.ACCOUNT, real, null);
        assertNotNull(object);

        attributes = new HashSet<>();
        attributes.add(new Name(USER_MEMBER_ID));
        attributes.add(createAttribute(Uid.NAME, USER_MEMBER_ID));
        attributes.add(createAttribute(ATTR_FIRST_NAME, USER_MEMBER_FIRST_NAME));
        attributes.add(createAttribute(ATTR_LAST_NAME, USER_MEMBER_LAST_NAME));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(USER_MEMBER_PASSWORD.toCharArray())));
        assertConnectorObject(attributes, object);

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_UID, USER_MEMBER_ID);
        expectedRecord.put(ATTR_FIRST_NAME, USER_MEMBER_FIRST_NAME);
        expectedRecord.put(ATTR_LAST_NAME, USER_MEMBER_LAST_NAME);
        expectedRecord.put(ATTR_PASSWORD, USER_MEMBER_PASSWORD);
        expectedRecord.put(ATTR_MEMBER_OF, "");

        Map<String, String> realRecord = CsvTestUtil.findRecord(createConfigurationNameEqualsUid(), USER_MEMBER_ID);
        assertEquals(expectedRecord, realRecord);
    }

    @Test
    public void addReferenceAttributeOnSubjectSameOc() throws Exception {
        CsvConfiguration config = createConfigurationNameEqualsUid();
        config.setMultivalueDelimiter(",");
        config.setMultivalueAttributes("memberOf");

        Set<String> values = Set.of(
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

        Set<Attribute> referenceAttributes = new HashSet<>();
        referenceAttributes.add(new Name(NEW_REFERENCE_ID));

        ConnectorObjectReference reference = new ConnectorObjectReference(buildConnectorObject(NEW_REFERENCE_ID,
                NEW_REFERENCE_ID, referenceAttributes, new ObjectClass("group")));

        Set<Attribute> oldReferenceAttributes = new HashSet<>();
        oldReferenceAttributes.add(new Name("2"));

        ConnectorObjectReference oldReference = new ConnectorObjectReference(buildConnectorObject("2",
                "2", oldReferenceAttributes, new ObjectClass("group")));

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build(ASSOC_ATTR_GROUP, reference));
        Uid real = connector.addAttributeValues(new ObjectClass("group"), expected, attributes, null);

        assertEquals(expected, real);

        ConnectorObject object = connector.getObject(new ObjectClass("group"), real, null);
        assertNotNull(object);

        attributes = new HashSet<>();
        attributes.add(new Name(GROUP_MEMBER_UPDATED_UID));
        attributes.add(createAttribute(Uid.NAME, GROUP_MEMBER_UPDATED_UID));
        attributes.add(createAttribute(ASSOC_ATTR_GROUP, oldReference, reference));
        attributes.add(createAttribute(ATTR_DESCRIPTION, GROUP_MEMBER_UPDATED_DESCRIPTION));
        assertConnectorObject(attributes, object, Name.NAME);

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_ID, GROUP_MEMBER_UPDATED_UID);
        expectedRecord.put(ATTR_DESCRIPTION, GROUP_MEMBER_UPDATED_DESCRIPTION);
        expectedRecord.put(ATTR_MEMBER_OF, "2," + GROUP_MEMBER_UPDATED_MEMBER_OF);

        Map<String, String> realRecord = CsvTestUtil.findRecord(createConfigurationReferencedObjectClass(
                "./target/groups-memberOf.csv", ATTR_ID), ATTR_ID, GROUP_MEMBER_UPDATED_UID);
        assertEquals(expectedRecord, realRecord);
    }

    @Test
    public void removeReferenceAttributeOnSubjectSameOc() throws Exception {
        CsvConfiguration config = createConfigurationNameEqualsUid();
        config.setMultivalueDelimiter(",");
        config.setMultivalueAttributes("memberOf");

        Set<String> values = Set.of(
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

        Set<Attribute> referenceAttributes = new HashSet<>();
        referenceAttributes.add(new Name("2"));

        ConnectorObjectReference oldReference = new ConnectorObjectReference(buildConnectorObject("2",
                "2", referenceAttributes, new ObjectClass("group")));

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build(ASSOC_ATTR_GROUP, oldReference));
        Uid real = connector.removeAttributeValues(new ObjectClass("group"), expected, attributes, null);

        assertEquals(expected, real);

        ConnectorObject object = connector.getObject(new ObjectClass("group"), real, null);
        assertNotNull(object);

        attributes = new HashSet<>();
        attributes.add(new Name(GROUP_MEMBER_UPDATED_UID));
        attributes.add(createAttribute(Uid.NAME, GROUP_MEMBER_UPDATED_UID));
        attributes.add(createAttribute(ATTR_DESCRIPTION, GROUP_MEMBER_UPDATED_DESCRIPTION));
        assertConnectorObject(attributes, object);

        assertTrue(object.getAttributeByName(ASSOC_ATTR_GROUP) == null);

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_ID, GROUP_MEMBER_UPDATED_UID);
        expectedRecord.put(ATTR_DESCRIPTION, GROUP_MEMBER_UPDATED_DESCRIPTION);
        expectedRecord.put(ATTR_MEMBER_OF, "");

        Map<String, String> realRecord = CsvTestUtil.findRecord(createConfigurationReferencedObjectClass(
                "./target/groups-memberOf.csv", ATTR_ID), ATTR_ID, GROUP_MEMBER_UPDATED_UID);
        assertEquals(expectedRecord, realRecord);
    }

    @Test
    public void addReferenceAttributeOnObjectComplex() throws Exception {

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
        Uid real = connector.addAttributeValues(ObjectClass.ACCOUNT, expected, attributes, null);

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

        Set<Attribute> referenceAttributesToCheck = new HashSet<>();
        referenceAttributesToCheck.add(createAttribute(Uid.NAME, NEW_REFERENCE_ID));
        referenceAttributesToCheck.add(createAttribute(Name.NAME, "users-all"));
        referenceAttributesToCheck.add(createAttribute(ATTR_DESCRIPTION, "ua"));
        referenceAttributesToCheck.add(createAttribute(ATTR_MEMBERS_TEST, "1", valueUserIdUpdateAccessOnObject));
        referenceAttributesToCheck.add(createAttribute(ATTR_MEMBERS_DEFAULT, valueUserIdUpdateAccessOnObject));
        referenceAttributesToCheck.add(createAttribute(ATTR_MEMBERS_ADMIN, valueUserIdUpdateAccessOnObject));

        assertReferenceAndReturnReferenceObject(referenceAttributesToCheck,
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
    public void removeReferenceAttributeOnObjectComplexSpecial() throws Exception {

        //TODO # A Should this remove the accounts reference to the group
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
        referenceAttributes.add(createAttribute(ATTR_MEMBERS_TEST, "1"));

        ConnectorObjectReference reference = new ConnectorObjectReference(buildConnectorObject(NEW_REFERENCE_ID,
                NEW_REFERENCE_ID, referenceAttributes, new ObjectClass("group")));

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build(ASSOC_ATTR_GROUP, reference));
        Uid real = connector.removeAttributeValues(ObjectClass.ACCOUNT, expected, attributes, null);

        assertEquals(expected, real);

        ConnectorObject object = connector.getObject(ObjectClass.ACCOUNT, real, null);
        assertNotNull(object);

        attributes = new HashSet<>();
        attributes.add(new Name(valueUserIdUpdateAccessOnObject));
        attributes.add(createAttribute(Uid.NAME, valueUserIdUpdateAccessOnObject));
        attributes.add(createAttribute(ATTR_NAME, "jack"));
        attributes.add(createAttribute(ATTR_EMPL, "234"));
        attributes.add(createAttribute(ATTR_TITLE, "manager"));
        assertConnectorObject(attributes, object);

        assertTrue(object.getAttributeByName(ASSOC_ATTR_GROUP) == null);

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
    public void removeReferenceAttributeOnObjectComplex() throws Exception {

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

        Set<Attribute> referenceAttributesRemoved = new HashSet<>();
        referenceAttributesRemoved.add(createAttribute(Uid.NAME, NEW_REFERENCE_ID));
        referenceAttributesRemoved.add(createAttribute(Name.NAME, "users-all"));
        referenceAttributesRemoved.add(createAttribute(ATTR_DESCRIPTION, "ua"));
        referenceAttributesRemoved.add(createAttribute(ATTR_MEMBERS_ADMIN, "2"));

        ConnectorObjectReference referenceRemoval = new ConnectorObjectReference(buildConnectorObject(NEW_REFERENCE_ID,
                NEW_REFERENCE_ID, referenceAttributesRemoved, new ObjectClass("group")));

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build(ASSOC_ATTR_GROUP, referenceRemoval));
        Uid real = connector.removeAttributeValues(ObjectClass.ACCOUNT, expected, attributes, null);

        assertEquals(expected, real);

        ConnectorObject object = connector.getObject(ObjectClass.ACCOUNT, real, null);
        assertNotNull(object);

        Set<Attribute> referenceAttributesToCheck = new HashSet<>();
        referenceAttributesToCheck.add(createAttribute(Uid.NAME, NEW_REFERENCE_ID));
        referenceAttributesToCheck.add(createAttribute(Name.NAME, "users-all"));
        referenceAttributesToCheck.add(createAttribute(ATTR_DESCRIPTION, "ua"));
        referenceAttributesToCheck.add(createAttribute(ATTR_MEMBERS_TEST, "1"));
        referenceAttributesToCheck.add(createAttribute(ATTR_MEMBERS_DEFAULT, "2"));

        ConnectorObjectReference referenceToCheck = new ConnectorObjectReference(buildConnectorObject(NEW_REFERENCE_ID,
                NEW_REFERENCE_ID, referenceAttributesRemoved, new ObjectClass("group")));

        attributes = new HashSet<>();
        attributes.add(new Name(valueUserIdUpdateAccessOnObject));
        attributes.add(createAttribute(Uid.NAME, valueUserIdUpdateAccessOnObject));
        attributes.add(createAttribute(ASSOC_ATTR_GROUP, referenceToCheck));
        attributes.add(createAttribute(ATTR_NAME, "jack"));
        attributes.add(createAttribute(ATTR_EMPL, "234"));
        attributes.add(createAttribute(ATTR_TITLE, "manager"));
        assertConnectorObject(attributes, object);

        assertReferenceAndReturnReferenceObject(referenceAttributesToCheck,
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
    public void addReferenceAttributeOnAccessComplex() throws Exception {
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
        secondLvlReferenceObjectAttributes.add(createAttribute(Uid.NAME, "1"));
        secondLvlReferenceObjectAttributes.add(createAttribute(Name.NAME, "users"));

        ConnectorObjectReference connectorObjectReferenceSecondLvl = new ConnectorObjectReference(buildConnectorObject("1",
                "1", secondLvlReferenceObjectAttributes, new ObjectClass("group")));

        Set<Attribute> referenceAttributes = new HashSet<>();
        referenceAttributes.add(createAttribute(Uid.NAME, NEW_REFERENCE_ID));
        referenceAttributes.add(createAttribute(Name.NAME, NEW_REFERENCE_ID));
        referenceAttributes.add(createAttribute(ATTR_LEVEL, "test"));
        referenceAttributes.add(createAttribute(ATTR_DESCRIPTION, "test accounts"));
        referenceAttributes.add(createAttribute(ATTR_SUBJECT_ID, valueUserIdUpdateAccessOnObject));
        referenceAttributes.add(createAttribute(ASSOC_ATTR_GROUP, connectorObjectReferenceSecondLvl));

        ConnectorObjectReference connectorObjectReference = new ConnectorObjectReference(buildConnectorObject(NEW_REFERENCE_ID,
                NEW_REFERENCE_ID, referenceAttributes, new ObjectClass("access")));
        ConnectorObjectReference connectorObjectReferenceExpectedOne = new ConnectorObjectReference(buildConnectorObject("2",
                "2", Set.of(createAttribute(Uid.NAME, "2")), new ObjectClass("access")));
        ConnectorObjectReference connectorObjectReferenceExpectedTwo = new ConnectorObjectReference(buildConnectorObject("3",
                "3", Set.of(createAttribute(Uid.NAME, "3")), new ObjectClass("access")));

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build(ASSOC_ATTR_ACCESS, connectorObjectReference));
        Uid real = connector.addAttributeValues(ObjectClass.ACCOUNT, expected, attributes, null);

        assertEquals(expected, real);

        ConnectorObject object = connector.getObject(ObjectClass.ACCOUNT, real, null);
        assertNotNull(object);

        attributes = new HashSet<>();
        attributes.add(new Name(valueUserIdUpdateAccessOnObject));
        attributes.add(createAttribute(Uid.NAME, valueUserIdUpdateAccessOnObject));
        attributes.add(createAttribute(ATTR_NAME, "jack"));
        attributes.add(createAttribute(ASSOC_ATTR_ACCESS, connectorObjectReference,
                connectorObjectReferenceExpectedOne ,connectorObjectReferenceExpectedTwo));
        attributes.add(createAttribute(ATTR_EMPL, "234"));
        attributes.add(createAttribute(ATTR_TITLE, "manager"));
        assertConnectorObject(attributes, object);

        Set<Attribute> referenceAttributesExpected = new HashSet<>();
        referenceAttributesExpected.add(createAttribute(Uid.NAME, NEW_REFERENCE_ID));
        referenceAttributesExpected.add(createAttribute(Name.NAME, NEW_REFERENCE_ID));
        referenceAttributesExpected.add(createAttribute(ATTR_LEVEL, "test"));
        referenceAttributesExpected.add(createAttribute(ATTR_DESCRIPTION, "test accounts"));
        referenceAttributesExpected.add(createAttribute(ATTR_SUBJECT_ID, "1", valueUserIdUpdateAccessOnObject));
        referenceAttributesExpected.add(createAttribute(ASSOC_ATTR_GROUP, connectorObjectReferenceSecondLvl));

        BaseConnectorObject secondLvlReferenceObject = assertReferenceAndReturnReferenceObject(
                referenceAttributesExpected, object.getAttributeByName(ASSOC_ATTR_ACCESS), new Uid (NEW_REFERENCE_ID));
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

    @Test
    public void addReferenceAttributeOnAccessNew() throws Exception {
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
        secondLvlReferenceObjectAttributes.add(createAttribute(Uid.NAME, GROUP_MEMBER_UID_NEW));
        secondLvlReferenceObjectAttributes.add(createAttribute(Name.NAME, "guests"));

        ConnectorObjectReference connectorObjectReferenceSecondLvl = new ConnectorObjectReference(buildConnectorObject("1",
                "1", secondLvlReferenceObjectAttributes, new ObjectClass("group")));

        Set<Attribute> referenceAttributes = new HashSet<>();
        referenceAttributes.add(createAttribute(Uid.NAME, ACCESS_MEMBER_NEW_UID));
        referenceAttributes.add(createAttribute(Name.NAME, ACCESS_MEMBER_NEW_UID));
        referenceAttributes.add(createAttribute(ATTR_LEVEL, "test"));
        referenceAttributes.add(createAttribute(ATTR_DESCRIPTION, "test accounts guests"));
        referenceAttributes.add(createAttribute(ATTR_SUBJECT_ID, valueUserIdUpdateAccessOnObject));
        referenceAttributes.add(createAttribute(ASSOC_ATTR_GROUP, connectorObjectReferenceSecondLvl));

        ConnectorObjectReference connectorObjectReference = new ConnectorObjectReference(buildConnectorObject(NEW_REFERENCE_ID,
                NEW_REFERENCE_ID, referenceAttributes, new ObjectClass("access")));

        ConnectorObjectReference connectorObjectReferenceExpectedOne = new ConnectorObjectReference(buildConnectorObject("2",
                "2", Set.of(createAttribute(Uid.NAME, "2")), new ObjectClass("access")));
        ConnectorObjectReference connectorObjectReferenceExpectedTwo = new ConnectorObjectReference(buildConnectorObject("3",
                "3", Set.of(createAttribute(Uid.NAME, "3")), new ObjectClass("access")));

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build(ASSOC_ATTR_ACCESS, connectorObjectReference));
        Uid real = connector.addAttributeValues(ObjectClass.ACCOUNT, expected, attributes, null);

        assertEquals(expected, real);

        ConnectorObject object = connector.getObject(ObjectClass.ACCOUNT, real, null);
        assertNotNull(object);

        attributes = new HashSet<>();
        attributes.add(new Name(valueUserIdUpdateAccessOnObject));
        attributes.add(createAttribute(Uid.NAME, valueUserIdUpdateAccessOnObject));
        attributes.add(createAttribute(ATTR_NAME, "jack"));
        attributes.add(createAttribute(ASSOC_ATTR_ACCESS, connectorObjectReference,
                connectorObjectReferenceExpectedOne ,connectorObjectReferenceExpectedTwo));
        attributes.add(createAttribute(ATTR_EMPL, "234"));
        attributes.add(createAttribute(ATTR_TITLE, "manager"));
        assertConnectorObject(attributes, object);

        Set<Attribute> referenceAttributesExpected = new HashSet<>();
        referenceAttributesExpected.add(createAttribute(Uid.NAME, ACCESS_MEMBER_NEW_UID));
        referenceAttributesExpected.add(createAttribute(Name.NAME, ACCESS_MEMBER_NEW_UID));
        referenceAttributesExpected.add(createAttribute(ATTR_LEVEL, "test"));
        referenceAttributesExpected.add(createAttribute(ATTR_DESCRIPTION, "test accounts guests"));
        referenceAttributesExpected.add(createAttribute(ATTR_SUBJECT_ID, valueUserIdUpdateAccessOnObject));
        referenceAttributesExpected.add(createAttribute(ASSOC_ATTR_GROUP, connectorObjectReferenceSecondLvl));

        BaseConnectorObject secondLvlReferenceObject = assertReferenceAndReturnReferenceObject(
                referenceAttributesExpected, object.getAttributeByName(ASSOC_ATTR_ACCESS), new Uid ("4"));
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

    @Test
    public void removeReferenceAttributeOnAccessComplex() throws Exception {

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

        String valueUserIdUpdateAccessOnObject = "1";
        Uid expected = new Uid(valueUserIdUpdateAccessOnObject);

        Set<Attribute> secondLvlReferenceObjectAttributes = new HashSet<>();
        secondLvlReferenceObjectAttributes.add(createAttribute(Uid.NAME, "1"));
        secondLvlReferenceObjectAttributes.add(createAttribute(Name.NAME, "users"));

        ConnectorObjectReference connectorObjectReferenceSecondLvl = new ConnectorObjectReference(buildConnectorObject("1",
                "1", secondLvlReferenceObjectAttributes, new ObjectClass("group")));

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
        Uid real = connector.removeAttributeValues(ObjectClass.ACCOUNT, expected, attributes, null);

        assertEquals(expected, real);

        ConnectorObject object = connector.getObject(ObjectClass.ACCOUNT, real, null);
       assertNotNull(object);

        attributes = new HashSet<>();
        attributes.add(new Name(valueUserIdUpdateAccessOnObject));
        attributes.add(createAttribute(Uid.NAME, valueUserIdUpdateAccessOnObject));
        attributes.add(createAttribute(ATTR_NAME, "john"));
        attributes.add(createAttribute(ATTR_EMPL, "123"));
        attributes.add(createAttribute(ATTR_TITLE, "engineer"));
        assertConnectorObject(attributes, object);

        assertTrue(object.getAttributeByName(ASSOC_ATTR_ACCESS) == null);

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_ID, valueUserIdUpdateAccessOnObject);
        expectedRecord.put(ATTR_EMPL, "123");
        expectedRecord.put(ATTR_NAME, "john");
        expectedRecord.put(ATTR_TITLE, "engineer");

        Map<String, String> realRecord = CsvTestUtil.findRecord(config, ATTR_ID,
                valueUserIdUpdateAccessOnObject);
        assertEquals(expectedRecord, realRecord);

        Map<String, String> accessRecord = CsvTestUtil.findRecord(createConfigurationReferencedObjectClass(
                "./target/access.csv", ATTR_ID), ATTR_ID, "1");
        assertEquals(null, accessRecord);
    }
}
