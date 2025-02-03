package com.evolveum.polygon.connector.csv;

import com.evolveum.polygon.connector.csv.util.CsvTestUtil;
import org.apache.commons.io.FileUtils;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.exceptions.InvalidAttributeValueException;
import org.identityconnectors.framework.common.objects.*;
import org.testng.annotations.Test;

import java.io.File;
import java.util.*;

import static com.evolveum.polygon.connector.csv.util.Util.ASSOC_ATTR_ACCESS;
import static com.evolveum.polygon.connector.csv.util.Util.ASSOC_ATTR_GROUP;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

/**
 * Created by Viliam Repan (lazyman).
 */
public class CreateOpTest extends BaseTest {

    private static final String NEW_UID = "uid=vilo,dc=example,dc=com";
    private static final String NEW_FIRST_NAME = "viliam";
    private static final String NEW_LAST_NAME = "repan";
    private static final String NEW_PASSWORD = "asdf123";


    private static final String NEW_GROUP_UID = "6";
    private static final String NEW_GROUP_DESCRIPTION = "hr";
    private static final String NEW_GROUP_REFERENCE_ID = "1";

    private static final String NEW_USER_NAME = "jacqueline";
    private static final String NEW_USER_EMPL = "345";
    private static final String NEW_USER_TITLE = "hr";
    private static final String NEW_USER_REFERENCE_ID = "1";


    @Test(expectedExceptions = ConnectorException.class)
    public void readOnlyMode() throws Exception {
        CsvConfiguration config = new CsvConfiguration();

        config.setFilePath(new File(BaseTest.CSV_FILE_PATH));
        config.setTmpFolder(null);
        config.setUniqueAttribute(ATTR_UID);
        config.setPasswordAttribute(ATTR_PASSWORD);
        config.setReadOnly(true);

        ConnectorFacade connector = setupConnector("/create.csv", config);
        Set<Attribute> attributes = new HashSet<>();
        attributes.add(new Name(NEW_UID));
        attributes.add(createAttribute(ATTR_UID, NEW_UID));
        attributes.add(createAttribute(ATTR_FIRST_NAME, NEW_FIRST_NAME));
        attributes.add(createAttribute(ATTR_LAST_NAME, NEW_LAST_NAME));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(NEW_PASSWORD.toCharArray())));
        connector.create(ObjectClass.ACCOUNT, attributes, null);

    }

    @Test
    public void createAccountAllAttributesNameEqualsUid() throws Exception {
        ConnectorFacade connector = setupConnector("/create.csv", createConfigurationNameEqualsUid());

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(new Name(NEW_UID));
        attributes.add(createAttribute(ATTR_UID, NEW_UID));
        attributes.add(createAttribute(ATTR_FIRST_NAME, NEW_FIRST_NAME));
        attributes.add(createAttribute(ATTR_LAST_NAME, NEW_LAST_NAME));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(NEW_PASSWORD.toCharArray())));
        Uid uid = connector.create(ObjectClass.ACCOUNT, attributes, null);
        assertNotNull(uid);
        assertEquals(NEW_UID, uid.getUidValue());

        ConnectorObject newObject = connector.getObject(ObjectClass.ACCOUNT, uid, null);
        assertNotNull(newObject);
        attributes = new HashSet<>();
        attributes.add(new Name(NEW_UID));
        attributes.add(createAttribute(Uid.NAME, NEW_UID));
        attributes.add(createAttribute(ATTR_FIRST_NAME, NEW_FIRST_NAME));
        attributes.add(createAttribute(ATTR_LAST_NAME, NEW_LAST_NAME));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(NEW_PASSWORD.toCharArray())));
        assertConnectorObject(attributes, newObject);

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_UID, NEW_UID);
        expectedRecord.put(ATTR_FIRST_NAME, NEW_FIRST_NAME);
        expectedRecord.put(ATTR_LAST_NAME, NEW_LAST_NAME);
        expectedRecord.put(ATTR_PASSWORD, NEW_PASSWORD);

        Map<String, String> realRecord = CsvTestUtil.findRecord(createConfigurationNameEqualsUid(), NEW_UID);
        assertEquals(expectedRecord, realRecord);
    }

    @Test(expectedExceptions = InvalidAttributeValueException.class)
    public void createAccountNameEqualsUidWrongName() throws Exception {
        ConnectorFacade connector = setupConnector("/create-empty.csv", createConfigurationNameEqualsUid());

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(new Name(NEW_UID + "different"));
        attributes.add(createAttribute(ATTR_UID, NEW_UID));
        attributes.add(createAttribute(ATTR_FIRST_NAME, NEW_FIRST_NAME));
        attributes.add(createAttribute(ATTR_LAST_NAME, NEW_LAST_NAME));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(NEW_PASSWORD.toCharArray())));

        connector.create(ObjectClass.ACCOUNT, attributes, null);
    }

    @Test
    public void createAccountNameEqualsUidNoUid() throws Exception {
        ConnectorFacade connector = setupConnector("/create-empty.csv", createConfigurationNameEqualsUid());

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(new Name(NEW_UID));
        attributes.add(createAttribute(ATTR_FIRST_NAME, NEW_FIRST_NAME));
        attributes.add(createAttribute(ATTR_LAST_NAME, NEW_LAST_NAME));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(NEW_PASSWORD.toCharArray())));

        Uid uid = connector.create(ObjectClass.ACCOUNT, attributes, null);
        assertNotNull(uid);
        assertEquals(NEW_UID, uid.getUidValue());

        ConnectorObject newObject = connector.getObject(ObjectClass.ACCOUNT, uid, null);
        assertNotNull(newObject);
        attributes = new HashSet<>();
        attributes.add(new Name(NEW_UID));
        attributes.add(createAttribute(ATTR_FIRST_NAME, NEW_FIRST_NAME));
        attributes.add(createAttribute(ATTR_LAST_NAME, NEW_LAST_NAME));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(NEW_PASSWORD.toCharArray())));
        attributes.add(createAttribute(Uid.NAME, NEW_UID));
        assertConnectorObject(attributes, newObject);

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_UID, NEW_UID);
        expectedRecord.put(ATTR_FIRST_NAME, NEW_FIRST_NAME);
        expectedRecord.put(ATTR_LAST_NAME, NEW_LAST_NAME);
        expectedRecord.put(ATTR_PASSWORD, NEW_PASSWORD);

        Map<String, String> realRecord = CsvTestUtil.findRecord(createConfigurationNameEqualsUid(), NEW_UID);
        assertEquals(expectedRecord, realRecord);
    }

    @Test(expectedExceptions = InvalidAttributeValueException.class)
    public void createAccountNoUidAtAll() throws Exception {
        ConnectorFacade connector = setupConnector("/create-empty.csv", createConfigurationNameEqualsUid());

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(createAttribute(ATTR_FIRST_NAME, NEW_FIRST_NAME));
        attributes.add(createAttribute(ATTR_LAST_NAME, NEW_LAST_NAME));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(NEW_PASSWORD.toCharArray())));

        connector.create(ObjectClass.ACCOUNT, attributes, null);
    }

    @Test
    public void createAccountNameEqualsUidNoName() throws Exception {
        ConnectorFacade connector = setupConnector("/create-empty.csv", createConfigurationNameEqualsUid());

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(createAttribute(ATTR_UID, NEW_UID));
        attributes.add(createAttribute(ATTR_FIRST_NAME, NEW_FIRST_NAME));
        attributes.add(createAttribute(ATTR_LAST_NAME, NEW_LAST_NAME));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(NEW_PASSWORD.toCharArray())));

        Uid uid = connector.create(ObjectClass.ACCOUNT, attributes, null);
        assertNotNull(uid);
        assertEquals(NEW_UID, uid.getUidValue());

        ConnectorObject newObject = connector.getObject(ObjectClass.ACCOUNT, uid, null);
        assertNotNull(newObject);
        attributes = new HashSet<>();
        attributes.add(createAttribute(Uid.NAME, NEW_UID));
        attributes.add(createAttribute(ATTR_FIRST_NAME, NEW_FIRST_NAME));
        attributes.add(createAttribute(ATTR_LAST_NAME, NEW_LAST_NAME));
        attributes.add(new Name(NEW_UID));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(NEW_PASSWORD.toCharArray())));
        assertConnectorObject(attributes, newObject);

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_UID, NEW_UID);
        expectedRecord.put(ATTR_FIRST_NAME, NEW_FIRST_NAME);
        expectedRecord.put(ATTR_LAST_NAME, NEW_LAST_NAME);
        expectedRecord.put(ATTR_PASSWORD, NEW_PASSWORD);

        Map<String, String> realRecord = CsvTestUtil.findRecord(createConfigurationNameEqualsUid(), NEW_UID);
        assertEquals(expectedRecord, realRecord);
    }

    @Test(expectedExceptions = ConnectorException.class)
    public void createAccountAllAttributesDifferent() throws Exception {
        ConnectorFacade connector = setupConnector("/create-empty.csv", createConfigurationDifferent());

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(new Name(NEW_UID));
        attributes.add(createAttribute(ATTR_UID, NEW_UID));
        attributes.add(createAttribute(ATTR_FIRST_NAME, NEW_FIRST_NAME));
        attributes.add(createAttribute(ATTR_LAST_NAME, NEW_LAST_NAME));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(NEW_PASSWORD.toCharArray())));

        connector.create(ObjectClass.ACCOUNT, attributes, null);
    }

    @Test
    public void createAccountDifferentNameNotEqualUid() throws Exception {
        ConnectorFacade connector = setupConnector("/create-empty.csv", createConfigurationDifferent());

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(new Name(NEW_LAST_NAME));
        attributes.add(createAttribute(ATTR_UID, NEW_UID));
        attributes.add(createAttribute(ATTR_FIRST_NAME, NEW_FIRST_NAME));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(NEW_PASSWORD.toCharArray())));
        Uid uid = connector.create(ObjectClass.ACCOUNT, attributes, null);
        assertNotNull(uid);
        assertEquals(NEW_UID, uid.getUidValue());

        ConnectorObject newObject = connector.getObject(ObjectClass.ACCOUNT, uid, null);
        assertNotNull(newObject);
        attributes = new HashSet<>();
        attributes.add(new Name(NEW_LAST_NAME));
        attributes.add(createAttribute(Uid.NAME, NEW_UID));
        attributes.add(createAttribute(ATTR_FIRST_NAME, NEW_FIRST_NAME));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(NEW_PASSWORD.toCharArray())));
        assertConnectorObject(attributes, newObject);

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_UID, NEW_UID);
        expectedRecord.put(ATTR_FIRST_NAME, NEW_FIRST_NAME);
        expectedRecord.put(ATTR_LAST_NAME, NEW_LAST_NAME);
        expectedRecord.put(ATTR_PASSWORD, NEW_PASSWORD);

        Map<String, String> realRecord = CsvTestUtil.findRecord(createConfigurationNameEqualsUid(), NEW_UID);
        assertEquals(expectedRecord, realRecord);
    }

    @Test
    public void createAccountAllAttributesNameEqualsUidMultilineMultivalue() throws Exception {
        CsvConfiguration config = createConfigurationNameEqualsUid();
        config.setMultivalueDelimiter(",");
        ConnectorFacade connector = setupConnector("/create.csv", config);

        final String SECOND_LAST_NAME = "von\nBahnhof";

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(new Name(NEW_UID));
        attributes.add(createAttribute(ATTR_UID, NEW_UID));
        attributes.add(createAttribute(ATTR_FIRST_NAME, NEW_FIRST_NAME));
        attributes.add(createAttribute(ATTR_LAST_NAME, NEW_LAST_NAME, SECOND_LAST_NAME));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(NEW_PASSWORD.toCharArray())));
        Uid uid = connector.create(ObjectClass.ACCOUNT, attributes, null);
        assertNotNull(uid);
        assertEquals(NEW_UID, uid.getUidValue());

        ConnectorObject newObject = connector.getObject(ObjectClass.ACCOUNT, uid, null);
        assertNotNull(newObject);
        attributes = new HashSet<>();
        attributes.add(new Name(NEW_UID));
        attributes.add(createAttribute(Uid.NAME, NEW_UID));
        attributes.add(createAttribute(ATTR_FIRST_NAME, NEW_FIRST_NAME));
        attributes.add(createAttribute(ATTR_LAST_NAME, NEW_LAST_NAME, SECOND_LAST_NAME));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(NEW_PASSWORD.toCharArray())));
        assertConnectorObject(attributes, newObject);

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_UID, NEW_UID);
        expectedRecord.put(ATTR_FIRST_NAME, NEW_FIRST_NAME);
        expectedRecord.put(ATTR_LAST_NAME, NEW_LAST_NAME + "," + SECOND_LAST_NAME);
        expectedRecord.put(ATTR_PASSWORD, NEW_PASSWORD);

        Map<String, String> realRecord = CsvTestUtil.findRecord(createConfigurationNameEqualsUid(), NEW_UID);
        assertEquals(expectedRecord, realRecord);
    }


    @Test
    public void createComplexAccountWithReferenceOnSubjectDifferentOc() throws Exception {
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

        Set<Attribute> referenceAttributes = new HashSet<>();
        referenceAttributes.add(new Name(NEW_REFERENCE_ID));

        ConnectorObjectReference reference = new ConnectorObjectReference(buildConnectorObject(NEW_REFERENCE_ID,
                NEW_REFERENCE_ID, referenceAttributes, new ObjectClass("group")));

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(new Name(NEW_UID));
        attributes.add(createAttribute(ATTR_UID, NEW_UID));
        attributes.add(createAttribute(ATTR_FIRST_NAME, NEW_FIRST_NAME));
        attributes.add(createAttribute(ATTR_LAST_NAME, NEW_LAST_NAME));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(NEW_PASSWORD.toCharArray())));
        attributes.add(createAttribute(ASSOC_ATTR_GROUP, reference));
        Uid uid = connector.create(ObjectClass.ACCOUNT, attributes, null);
        assertNotNull(uid);
        assertEquals(NEW_UID, uid.getUidValue());

        ConnectorObject newObject = connector.getObject(ObjectClass.ACCOUNT, uid, null);
        assertNotNull(newObject);
        attributes = new HashSet<>();
        attributes.add(new Name(NEW_UID));
        attributes.add(createAttribute(Uid.NAME, NEW_UID));
        attributes.add(createAttribute(ATTR_FIRST_NAME, NEW_FIRST_NAME));
        attributes.add(createAttribute(ATTR_LAST_NAME, NEW_LAST_NAME));
        attributes.add(createAttribute(ASSOC_ATTR_GROUP, reference));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(NEW_PASSWORD.toCharArray())));
        assertConnectorObject(attributes, newObject, Name.NAME);

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_UID, NEW_UID);
        expectedRecord.put(ATTR_FIRST_NAME, NEW_FIRST_NAME);
        expectedRecord.put(ATTR_PASSWORD, NEW_PASSWORD);
        expectedRecord.put(ATTR_LAST_NAME, NEW_LAST_NAME);
        expectedRecord.put(ATTR_MEMBER_OF, NEW_REFERENCE_ID);

        Map<String, String> realRecord = CsvTestUtil.findRecord(createConfigurationNameEqualsUid(), NEW_UID);
        assertEquals(expectedRecord, realRecord);
    }

    @Test
    public void createComplexAccountWithReferenceOnSubjectSameOc() throws Exception {
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

        Set<Attribute> referenceAttributes = new HashSet<>();
        referenceAttributes.add(new Name(NEW_REFERENCE_ID));

        ConnectorObjectReference reference = new ConnectorObjectReference(buildConnectorObject(NEW_REFERENCE_ID,
                NEW_REFERENCE_ID, referenceAttributes, new ObjectClass("group")));

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(new Name(NEW_GROUP_UID));
        attributes.add(createAttribute(ATTR_ID, NEW_GROUP_UID));
        attributes.add(createAttribute(ATTR_DESCRIPTION, NEW_GROUP_DESCRIPTION));
        attributes.add(createAttribute(ASSOC_ATTR_GROUP, reference));
        Uid uid = connector.create(new ObjectClass("group"), attributes, null);
        assertNotNull(uid);
        assertEquals(NEW_GROUP_UID, uid.getUidValue());

        ConnectorObject newObject = connector.getObject(new ObjectClass("group"), uid, null);
        assertNotNull(newObject);

        attributes = new HashSet<>();
        attributes.add(new Name(NEW_GROUP_UID));
        attributes.add(createAttribute(Uid.NAME, NEW_GROUP_UID));
        attributes.add(createAttribute(ATTR_DESCRIPTION, NEW_GROUP_DESCRIPTION));
        attributes.add(createAttribute(ASSOC_ATTR_GROUP, reference));
        assertConnectorObject(attributes, newObject, Name.NAME);

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_ID, NEW_GROUP_UID);
        expectedRecord.put(ATTR_DESCRIPTION, NEW_GROUP_DESCRIPTION);
        expectedRecord.put(ATTR_MEMBER_OF, NEW_REFERENCE_ID);

        Map<String, String> realRecord = CsvTestUtil.findRecord(createConfigurationReferencedObjectClass(
                "./target/groups-memberOf.csv", ATTR_ID), ATTR_ID, NEW_GROUP_UID);
        assertEquals(expectedRecord, realRecord);
    }

    @Test
    public void createComplexAccountWithReferenceOnObject() throws Exception {
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

        Set<Attribute> referenceAttributes = new HashSet<>();
        referenceAttributes.add(createAttribute(Uid.NAME, NEW_REFERENCE_ID));
        referenceAttributes.add(createAttribute(Name.NAME, "users-all"));
        referenceAttributes.add(createAttribute(ATTR_DESCRIPTION, "ua"));
        referenceAttributes.add(createAttribute(ATTR_MEMBERS_TEST, NEW_USER_UID, "1"));
        referenceAttributes.add(createAttribute(ATTR_MEMBERS_DEFAULT,  "2"));
        referenceAttributes.add(createAttribute(ATTR_MEMBERS_ADMIN, "2"));

        ConnectorObjectReference reference = new ConnectorObjectReference(buildConnectorObject(NEW_REFERENCE_ID,
                NEW_REFERENCE_ID, referenceAttributes, new ObjectClass("group")));

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(new Name(NEW_USER_UID));
        attributes.add(createAttribute(ATTR_ID, NEW_USER_UID));
        attributes.add(createAttribute(ATTR_NAME, NEW_USER_NAME));
        attributes.add(createAttribute(ATTR_EMPL, NEW_USER_EMPL));
        attributes.add(createAttribute(ATTR_TITLE, NEW_USER_TITLE));
        attributes.add(createAttribute(ASSOC_ATTR_GROUP, reference));
        Uid uid = connector.create(ObjectClass.ACCOUNT, attributes, null);
        assertNotNull(uid);
        assertEquals(NEW_USER_UID, uid.getUidValue());

        ConnectorObject newObject = connector.getObject(ObjectClass.ACCOUNT, uid, null);
        assertNotNull(newObject);

        attributes = new HashSet<>();
        attributes.add(new Name(NEW_USER_UID));
        attributes.add(createAttribute(Uid.NAME, NEW_USER_UID));
        attributes.add(createAttribute(ATTR_NAME, NEW_USER_NAME));
        attributes.add(createAttribute(ATTR_EMPL, NEW_USER_EMPL));
        attributes.add(createAttribute(ATTR_TITLE, NEW_USER_TITLE));
        attributes.add(createAttribute(ASSOC_ATTR_GROUP, reference));
        assertConnectorObject(attributes, newObject);

        for(Attribute attr : newObject.getAttributes()) {
            if (ASSOC_ATTR_GROUP.equals(attr.getName())) {
                List<Object> valueList = attr.getValue();

                if (valueList!=null && !valueList.isEmpty() && valueList.size() == 1) {
                    Object cor = valueList.get(0);
                    if (cor instanceof ConnectorObjectReference) {
                        BaseConnectorObject bco = ((ConnectorObjectReference) cor).getValue();
                        assertConnectorObject(referenceAttributes, bco);
                    }
                }
            }
        }

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_ID, NEW_USER_UID);
        expectedRecord.put(ATTR_NAME, NEW_USER_NAME);
        expectedRecord.put(ATTR_EMPL, NEW_USER_EMPL);
        expectedRecord.put(ATTR_TITLE, NEW_USER_TITLE);

        Map<String, String> realRecord = CsvTestUtil.findRecord(config, ATTR_ID, NEW_USER_UID);
        assertEquals(expectedRecord, realRecord);
    }

    @Test
    public void createComplexAccountWithReferenceOnObjectMultiple() throws Exception {
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

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(new Name(NEW_USER_UID));
        attributes.add(createAttribute(ATTR_ID, NEW_USER_UID));
        attributes.add(createAttribute(ATTR_NAME, NEW_USER_NAME));
        attributes.add(createAttribute(ATTR_EMPL, NEW_USER_EMPL));
        attributes.add(createAttribute(ATTR_TITLE, NEW_USER_TITLE));

        Set<Attribute> referenceAttributes = new HashSet<>();
        referenceAttributes.add(createAttribute(Uid.NAME, NEW_REFERENCE_ID));
        referenceAttributes.add(createAttribute(Name.NAME, "users-all"));
        referenceAttributes.add(createAttribute(ATTR_DESCRIPTION, "ua"));
        referenceAttributes.add(createAttribute(ATTR_MEMBERS_TEST, NEW_USER_UID, "1"));
        referenceAttributes.add(createAttribute(ATTR_MEMBERS_DEFAULT, NEW_USER_UID, "2"));
        referenceAttributes.add(createAttribute(ATTR_MEMBERS_ADMIN, "2"));

        ConnectorObjectReference connectorObjectReference = new ConnectorObjectReference(buildConnectorObject(NEW_REFERENCE_ID,
                NEW_REFERENCE_ID, referenceAttributes, new ObjectClass("group")));


        attributes.add(createAttribute(ASSOC_ATTR_GROUP, connectorObjectReference));
        Uid uid = connector.create(ObjectClass.ACCOUNT, attributes, null);
        assertNotNull(uid);
        assertEquals(NEW_USER_UID, uid.getUidValue());

        ConnectorObject newObject = connector.getObject(ObjectClass.ACCOUNT, uid, null);
        assertNotNull(newObject);

        attributes = new HashSet<>();
        attributes.add(new Name(NEW_USER_UID));
        attributes.add(createAttribute(Uid.NAME, NEW_USER_UID));
        attributes.add(createAttribute(ATTR_NAME, NEW_USER_NAME));
        attributes.add(createAttribute(ATTR_EMPL, NEW_USER_EMPL));
        attributes.add(createAttribute(ATTR_TITLE, NEW_USER_TITLE));
        attributes.add(createAttribute(ASSOC_ATTR_GROUP, connectorObjectReference));
        assertConnectorObject(attributes, newObject);

        for(Attribute attr : newObject.getAttributes()) {
            if (ASSOC_ATTR_GROUP.equals(attr.getName())) {
                List<Object> valueList = attr.getValue();

                if (!valueList.isEmpty() && valueList.size() == 1) {
                    Object cor = valueList.get(0);
                    if (cor instanceof ConnectorObjectReference) {
                        BaseConnectorObject bco = ((ConnectorObjectReference) cor).getValue();
                        assertConnectorObject(referenceAttributes, bco);
                    }
                }
            }
        }

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_ID, NEW_USER_UID);
        expectedRecord.put(ATTR_NAME, NEW_USER_NAME);
        expectedRecord.put(ATTR_EMPL, NEW_USER_EMPL);
        expectedRecord.put(ATTR_TITLE, NEW_USER_TITLE);

        Map<String, String> realRecord = CsvTestUtil.findRecord(config, ATTR_ID, NEW_USER_UID);
        assertEquals(expectedRecord, realRecord);
    }

    @Test
    public void createComplexAccountWithReferenceOnObjectIndirect() throws Exception {
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

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(new Name(NEW_USER_UID));
        attributes.add(createAttribute(ATTR_ID, NEW_USER_UID));
        attributes.add(createAttribute(ATTR_NAME, NEW_USER_NAME));
        attributes.add(createAttribute(ATTR_EMPL, NEW_USER_EMPL));
        attributes.add(createAttribute(ATTR_TITLE, NEW_USER_TITLE));

        Set<Attribute> secondLvlReferenceObjectAttributes = new HashSet<>();
        secondLvlReferenceObjectAttributes.add(createAttribute(Uid.NAME, NEW_REFERENCE_ID));
        secondLvlReferenceObjectAttributes.add(createAttribute(Name.NAME, "users"));

        ConnectorObjectReference connectorObjectReferenceSecondLvl = new ConnectorObjectReference(buildConnectorObject(NEW_REFERENCE_ID,
                NEW_REFERENCE_ID, secondLvlReferenceObjectAttributes, new ObjectClass("group")));

        Set<Attribute> referenceAttributes = new HashSet<>();
        referenceAttributes.add(createAttribute(Uid.NAME, NEW_REFERENCE_ID));
        referenceAttributes.add(createAttribute(Name.NAME, NEW_REFERENCE_ID));
        referenceAttributes.add(createAttribute(ATTR_LEVEL, "test"));
        referenceAttributes.add(createAttribute(ATTR_DESCRIPTION, "test accounts"));
        referenceAttributes.add(createAttribute(ATTR_SUBJECT_ID, "1", NEW_USER_UID));
        referenceAttributes.add(createAttribute(ASSOC_ATTR_GROUP, connectorObjectReferenceSecondLvl));

        ConnectorObjectReference connectorObjectReference = new ConnectorObjectReference(buildConnectorObject(NEW_REFERENCE_ID,
                NEW_REFERENCE_ID, referenceAttributes, new ObjectClass("access")));

        attributes.add(createAttribute(ASSOC_ATTR_ACCESS, connectorObjectReference));

        Uid uid = connector.create(ObjectClass.ACCOUNT, attributes, null);
        assertNotNull(uid);
        assertEquals(NEW_USER_UID, uid.getUidValue());

        ConnectorObject newObject = connector.getObject(ObjectClass.ACCOUNT, uid, null);
        assertNotNull(newObject);

        attributes = new HashSet<>();
        attributes.add(new Name(NEW_USER_UID));
        attributes.add(createAttribute(Uid.NAME, NEW_USER_UID));
        attributes.add(createAttribute(ATTR_NAME, NEW_USER_NAME));
        attributes.add(createAttribute(ATTR_EMPL, NEW_USER_EMPL));
        attributes.add(createAttribute(ATTR_TITLE, NEW_USER_TITLE));
        attributes.add(createAttribute(ASSOC_ATTR_ACCESS, connectorObjectReference));
        assertConnectorObject(attributes, newObject);
        
        BaseConnectorObject secondLvlReferenceObject = assertReferenceAndReturnReferenceObject(referenceAttributes,
                newObject.getAttributeByName(ASSOC_ATTR_ACCESS));
        assertNotNull(secondLvlReferenceObject);

        assertReferenceAndReturnReferenceObject(secondLvlReferenceObjectAttributes,
                secondLvlReferenceObject.getAttributeByName(ASSOC_ATTR_GROUP));

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_ID, NEW_USER_UID);
        expectedRecord.put(ATTR_NAME, NEW_USER_NAME);
        expectedRecord.put(ATTR_EMPL, NEW_USER_EMPL);
        expectedRecord.put(ATTR_TITLE, NEW_USER_TITLE);

        Map<String, String> realRecord = CsvTestUtil.findRecord(config, ATTR_ID, NEW_USER_UID);
        assertEquals(expectedRecord, realRecord);
    }
}
