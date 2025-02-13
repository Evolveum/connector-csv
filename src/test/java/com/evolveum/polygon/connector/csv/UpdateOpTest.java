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

        Uid expected = new Uid(USER_MEMBER_ID);

        Set<Attribute> referenceAttributes = new HashSet<>();
        referenceAttributes.add(new Name(GROUP_MEMBER_UID_NEW));

        ConnectorObjectReference reference = new ConnectorObjectReference(buildConnectorObject(GROUP_MEMBER_UID_NEW,
                GROUP_MEMBER_UID_NEW, referenceAttributes, new ObjectClass("group")));

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build(ASSOC_ATTR_GROUP, reference));
        Uid real = connector.update(ObjectClass.ACCOUNT, expected, attributes, null);

        assertEquals(expected, real);

        ConnectorObject object = connector.getObject(ObjectClass.ACCOUNT, real, null);
        assertNotNull(object);

        attributes = new HashSet<>();
        attributes.add(new Name(USER_MEMBER_ID));
        attributes.add(createAttribute(Uid.NAME, USER_MEMBER_ID));
        attributes.add(createAttribute(ASSOC_ATTR_GROUP, reference));
        attributes.add(createAttribute(ATTR_FIRST_NAME, USER_MEMBER_FIRST_NAME));
        attributes.add(createAttribute(ATTR_LAST_NAME, USER_MEMBER_LAST_NAME));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(USER_MEMBER_PASSWORD.toCharArray())));
        assertConnectorObject(attributes, object, Name.NAME);

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_UID, USER_MEMBER_ID);
        expectedRecord.put(ATTR_FIRST_NAME, USER_MEMBER_FIRST_NAME);
        expectedRecord.put(ATTR_LAST_NAME, USER_MEMBER_LAST_NAME );
        expectedRecord.put(ATTR_PASSWORD, USER_MEMBER_PASSWORD);
        expectedRecord.put(ATTR_MEMBER_OF, GROUP_MEMBER_UID_NEW);

        Map<String, String> realRecord = CsvTestUtil.findRecord(createConfigurationNameEqualsUid(), USER_MEMBER_ID);
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

        Set<Attribute> referenceAttrs = new HashSet<>();
        referenceAttrs.add(AttributeBuilder.build(Name.NAME, NEW_REFERENCE_ID));
        referenceAttrs.add(AttributeBuilder.build(ATTR_ID, NEW_REFERENCE_ID));

        ConnectorObjectReference reference = new ConnectorObjectReference(buildConnectorObject(NEW_REFERENCE_ID,
                NEW_REFERENCE_ID, referenceAttrs, new ObjectClass("group"), true));

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
        assertConnectorObject(attributes, object, ATTR_ID);

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
        attributes.add(AttributeBuilder.build(ASSOC_ATTR_GROUP+"-"+ATTR_MEMBERS_TEST, reference));
        attributes.add(AttributeBuilder.build(ASSOC_ATTR_GROUP+"-"+ATTR_MEMBERS_DEFAULT, reference));
        attributes.add(AttributeBuilder.build(ASSOC_ATTR_GROUP+"-"+ATTR_MEMBERS_ADMIN, reference));
        Uid real = connector.update(ObjectClass.ACCOUNT, expected, attributes, null);

        assertEquals(expected, real);

        ConnectorObject object = connector.getObject(ObjectClass.ACCOUNT, real, null);
        assertNotNull(object);

        attributes = new HashSet<>();
        attributes.add(new Name(valueUserIdUpdateAccessOnObject));
        attributes.add(createAttribute(Uid.NAME, valueUserIdUpdateAccessOnObject));
        attributes.add(createAttribute(ATTR_NAME, "jack"));
        attributes.add(createAttribute(ASSOC_ATTR_GROUP+"-"+ATTR_MEMBERS_TEST, reference));
        attributes.add(createAttribute(ASSOC_ATTR_GROUP+"-"+ATTR_MEMBERS_DEFAULT, reference));
        attributes.add(createAttribute(ASSOC_ATTR_GROUP+"-"+ATTR_MEMBERS_ADMIN, reference));
        attributes.add(createAttribute(ATTR_EMPL, "234"));
        attributes.add(createAttribute(ATTR_TITLE, "manager"));
        assertConnectorObject(attributes, object);

        assertReferenceAndReturnReferenceObject(referenceAttributes,
                object.getAttributeByName(ASSOC_ATTR_GROUP+"-"+ATTR_MEMBERS_TEST), new Uid(NEW_REFERENCE_ID));
        assertReferenceAndReturnReferenceObject(referenceAttributes,
                object.getAttributeByName(ASSOC_ATTR_GROUP+"-"+ATTR_MEMBERS_DEFAULT), new Uid(NEW_REFERENCE_ID));
        assertReferenceAndReturnReferenceObject(referenceAttributes,
                object.getAttributeByName(ASSOC_ATTR_GROUP+"-"+ATTR_MEMBERS_ADMIN), new Uid(NEW_REFERENCE_ID));

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
        referenceAttributes.add(createAttribute(Uid.NAME, ACCESS_BASIC_ID));
        referenceAttributes.add(createAttribute(Name.NAME, ACCESS_BASIC_ID));
        referenceAttributes.add(createAttribute(ATTR_LEVEL, ACCESS_BASIC_LEVEL));
        referenceAttributes.add(createAttribute(ATTR_DESCRIPTION, ACCESS_BASIC_DESCRIPTION));
        referenceAttributes.add(createAttribute(ATTR_SUBJECT_ID, valueUserIdUpdateAccessOnObject));
        referenceAttributes.add(createAttribute(ASSOC_ATTR_GROUP, connectorObjectReferenceSecondLvl));

        ConnectorObjectReference connectorObjectReference = new ConnectorObjectReference(buildConnectorObject(NEW_REFERENCE_ID,
                ACCESS_BASIC_ID, referenceAttributes, new ObjectClass("access")));
        ConnectorObjectReference connectorObjectReferenceExpectedOne = new ConnectorObjectReference(buildConnectorObject("2",
                "2", Set.of(createAttribute(Uid.NAME, "2")), new ObjectClass("access")));
        ConnectorObjectReference connectorObjectReferenceExpectedTwo = new ConnectorObjectReference(buildConnectorObject("3",
                "3", Set.of(createAttribute(Uid.NAME, "3")), new ObjectClass("access")));

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
        attributes.add(createAttribute(ASSOC_ATTR_ACCESS, connectorObjectReference, connectorObjectReferenceExpectedOne,
                connectorObjectReferenceExpectedTwo));
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

    @Test
    public void uidUpdateOfReferencedSubjectWithReferentialIntegrityAccess() throws Exception {
        String uidValueForUpdate = "42";

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

        Uid expected = new Uid(USER_BASIC_ID);

        Set<Attribute> attributes = new HashSet<>();

        attributes.add(AttributeBuilder.build(ATTR_ID, uidValueForUpdate));
        Uid real = connector.update(ObjectClass.ACCOUNT, expected, attributes, null);

        ConnectorObject object = connector.getObject(ObjectClass.ACCOUNT, real, null);
        assertNotNull(object);

        Set<Attribute> secondLvlReferenceObjectAttributes = new HashSet<>();
        secondLvlReferenceObjectAttributes.add(createAttribute(Uid.NAME, ACCESS_BASIC_OBJECT_ID));
        secondLvlReferenceObjectAttributes.add(createAttribute(Name.NAME, "users"));

        ConnectorObjectReference connectorObjectReferenceSecondLvl = new ConnectorObjectReference(buildConnectorObject(ACCESS_BASIC_OBJECT_ID,
                ACCESS_BASIC_OBJECT_ID, secondLvlReferenceObjectAttributes, new ObjectClass("group")));

        Set<Attribute> referenceAttributes = new HashSet<>();
        referenceAttributes.add(createAttribute(Uid.NAME, ACCESS_BASIC_ID));
        referenceAttributes.add(createAttribute(Name.NAME, ACCESS_BASIC_ID));
        referenceAttributes.add(createAttribute(ATTR_LEVEL, ACCESS_BASIC_LEVEL));
        referenceAttributes.add(createAttribute(ATTR_DESCRIPTION, ACCESS_BASIC_DESCRIPTION));
        referenceAttributes.add(createAttribute(ATTR_SUBJECT_ID, uidValueForUpdate));
        referenceAttributes.add(createAttribute(ASSOC_ATTR_GROUP, connectorObjectReferenceSecondLvl));

        ConnectorObjectReference connectorObjectReference = new ConnectorObjectReference(buildConnectorObject(ACCESS_BASIC_ID,
                ACCESS_BASIC_ID, referenceAttributes, new ObjectClass("access")));

        attributes = new HashSet<>();
        attributes.add(new Name(uidValueForUpdate));
        attributes.add(createAttribute(Uid.NAME, uidValueForUpdate));
        attributes.add(createAttribute(ATTR_NAME, USER_BASIC_NAME));
        attributes.add(createAttribute(ATTR_EMPL, USER_BASIC_EMPL));
        attributes.add(createAttribute(ATTR_TITLE, USER_BASIC_TITLE));
        attributes.add(createAttribute(ASSOC_ATTR_ACCESS, connectorObjectReference));
        assertConnectorObject(attributes, object);

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_ID, uidValueForUpdate);
        expectedRecord.put(ATTR_EMPL, USER_BASIC_EMPL);
        expectedRecord.put(ATTR_NAME, USER_BASIC_NAME);
        expectedRecord.put(ATTR_TITLE, USER_BASIC_TITLE);

        Map<String, String> realRecord = CsvTestUtil.findRecord(config, ATTR_ID,
                uidValueForUpdate);
        assertEquals(expectedRecord, realRecord);

        Map<String, String> expectedAccessRecord = new HashMap<>();
        expectedAccessRecord.put(ATTR_ID, ACCESS_BASIC_ID);
        expectedAccessRecord.put(ATTR_LEVEL, ACCESS_BASIC_LEVEL);
        expectedAccessRecord.put(ATTR_DESCRIPTION, ACCESS_BASIC_DESCRIPTION);
        expectedAccessRecord.put(ATTR_SUBJECT_ID, uidValueForUpdate);
        expectedAccessRecord.put(ATTR_OBJECT_ID, ACCESS_BASIC_OBJECT_ID);

        Map<String, String> accessRecord = CsvTestUtil.findRecord(createConfigurationReferencedObjectClass(
                "./target/access.csv", ATTR_ID), ATTR_ID, ACCESS_BASIC_ID);
      assertEquals(expectedAccessRecord, accessRecord);
    }

    @Test
    public void uidUpdateOfReferencedObjectWithReferentialIntegrityAccess() throws Exception {
        String uidValueForUpdate = "2520";

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

        Uid expected = new Uid(GROUP_NO_MEMBER_ID);

        Set<Attribute> attributes = new HashSet<>();

        attributes.add(AttributeBuilder.build(ATTR_ID, uidValueForUpdate));
        Uid real = connector.update(new ObjectClass("group"), expected, attributes, null);

        ConnectorObject groupObject = connector.getObject(new ObjectClass("group"), real, null);
        assertNotNull(groupObject);

        ConnectorObject userObject = connector.getObject(ObjectClass.ACCOUNT, new Uid(USER_BASIC_ID), null);
        assertNotNull(userObject);

        Set<Attribute> secondLvlReferenceObjectAttributes = new HashSet<>();
        secondLvlReferenceObjectAttributes.add(createAttribute(Uid.NAME, uidValueForUpdate));
        secondLvlReferenceObjectAttributes.add(createAttribute(Name.NAME, GROUP_NO_MEMBER_NAME));

        ConnectorObjectReference connectorObjectReferenceSecondLvl = new ConnectorObjectReference(buildConnectorObject(uidValueForUpdate,
                uidValueForUpdate, secondLvlReferenceObjectAttributes, new ObjectClass("group")));

        Set<Attribute> referenceAttributes = new HashSet<>();
        referenceAttributes.add(createAttribute(Uid.NAME, ACCESS_BASIC_ID));
        referenceAttributes.add(createAttribute(Name.NAME, ACCESS_BASIC_ID));
        referenceAttributes.add(createAttribute(ATTR_LEVEL, ACCESS_BASIC_LEVEL));
        referenceAttributes.add(createAttribute(ATTR_DESCRIPTION, ACCESS_BASIC_DESCRIPTION));
        referenceAttributes.add(createAttribute(ATTR_SUBJECT_ID, USER_BASIC_ID));
        referenceAttributes.add(createAttribute(ASSOC_ATTR_GROUP, connectorObjectReferenceSecondLvl));

        ConnectorObjectReference connectorObjectReference = new ConnectorObjectReference(buildConnectorObject(ACCESS_BASIC_ID,
                ACCESS_BASIC_ID, referenceAttributes, new ObjectClass("access")));

        attributes = new HashSet<>();
        attributes.add(new Name(USER_BASIC_ID));
        attributes.add(createAttribute(Uid.NAME, USER_BASIC_ID));
        attributes.add(createAttribute(ATTR_NAME, USER_BASIC_NAME));
        attributes.add(createAttribute(ATTR_EMPL, USER_BASIC_EMPL));
        attributes.add(createAttribute(ATTR_TITLE, USER_BASIC_TITLE));
        attributes.add(createAttribute(ASSOC_ATTR_ACCESS, connectorObjectReference));
        assertConnectorObject(attributes, userObject);

        attributes = new HashSet<>();
        attributes.add(new Name(GROUP_NO_MEMBER_NAME));
        attributes.add(createAttribute(Uid.NAME, uidValueForUpdate));

        assertConnectorObject(attributes, groupObject);

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_ID, uidValueForUpdate);
        expectedRecord.put(ATTR_DESCRIPTION, GROUP_NO_MEMBER_DESC);
        expectedRecord.put(ATTR_NAME, GROUP_NO_MEMBER_NAME);

        Map<String, String> groupRecord = CsvTestUtil.findRecord(createConfigurationReferencedObjectClass(
                "./target/groups-no-member.csv", ATTR_ID), ATTR_ID, uidValueForUpdate);
        assertEquals(expectedRecord, groupRecord);

        Map<String, String> expectedAccessRecord = new HashMap<>();
        expectedAccessRecord.put(ATTR_ID, ACCESS_BASIC_ID);
        expectedAccessRecord.put(ATTR_LEVEL, ACCESS_BASIC_LEVEL);
        expectedAccessRecord.put(ATTR_DESCRIPTION, ACCESS_BASIC_DESCRIPTION);
        expectedAccessRecord.put(ATTR_SUBJECT_ID, USER_BASIC_ID);
        expectedAccessRecord.put(ATTR_OBJECT_ID, uidValueForUpdate);

        Map<String, String> accessRecord = CsvTestUtil.findRecord(createConfigurationReferencedObjectClass(
                "./target/access.csv", ATTR_ID), ATTR_ID, ACCESS_BASIC_ID);
        assertEquals(expectedAccessRecord, accessRecord);
    }
}
