package com.evolveum.polygon.connector.csv;

import com.evolveum.polygon.connector.csv.util.CsvTestUtil;
import com.evolveum.polygon.connector.csv.util.ListResultHandler;
import org.apache.commons.io.FileUtils;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.exceptions.UnknownUidException;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.Uid;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

/**
 * Created by Viliam Repan (lazyman).
 */
public class DeleteOpTest extends BaseTest {

    private ConnectorFacade connector;

    @BeforeMethod
    public void beforeMethod() throws Exception {
        connector = setupConnector("/delete.csv");
    }

    @BeforeMethod
    public void afterMethod() {
        connector = null;
    }

    @Test(expectedExceptions = ConnectorException.class)
    public void badObjectClass() {
        connector.delete(ObjectClass.GROUP, new Uid("vilo"), null);
    }

    @Test(expectedExceptions = UnknownUidException.class)
    public void notExistingUid() {
        connector.delete(ObjectClass.ACCOUNT, new Uid("unknown"), null);
    }

    @Test
    public void correctDelete() throws Exception {
        connector.delete(ObjectClass.ACCOUNT, new Uid("vilo"), null);

        ListResultHandler handler = new ListResultHandler();
        connector.search(ObjectClass.ACCOUNT, null, handler, null);

        AssertJUnit.assertEquals(0, handler.getObjects().size());
    }

    @Test
    public void deleteReferenceSubjectWithReferentialIntegrityReferenceOnObject() throws Exception {

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

        ConnectorFacade connector = setupConnector("/schema-repeating-column.csv", config);

        connector.delete(ObjectClass.ACCOUNT, new Uid("1"), null);

        ListResultHandler handler = new ListResultHandler();
        connector.search(ObjectClass.ACCOUNT, null, handler, null);

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_ID, "1");
        expectedRecord.put(ATTR_NAME, "users-all");
        expectedRecord.put(ATTR_DESCRIPTION, "ua");
        expectedRecord.put(ATTR_MEMBERS_TEST, "");
        expectedRecord.put(ATTR_MEMBERS_DEFAULT, "2");
        expectedRecord.put(ATTR_MEMBERS_ADMIN, "2");

        Map<String, String> realRecord = CsvTestUtil.findRecord(createConfigurationReferencedObjectClass(
                "./target/groups-access.csv", ATTR_ID), ATTR_ID, "1");
        assertEquals(expectedRecord, realRecord);

        AssertJUnit.assertEquals(1, handler.getObjects().size());
    }

    @Test
    public void deleteReferenceObjectWithReferentialIntegrityReferenceOnSubject() throws Exception {
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

        connector.delete(new ObjectClass("group"), new Uid("1"), null);

        ListResultHandler handler = new ListResultHandler();
        connector.search(new ObjectClass("group"), null, handler, null);

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_UID, USER_MEMBER_ID);
        expectedRecord.put(ATTR_FIRST_NAME, USER_MEMBER_FIRST_NAME);
        expectedRecord.put(ATTR_LAST_NAME, USER_MEMBER_LAST_NAME);
        expectedRecord.put(ATTR_PASSWORD, USER_MEMBER_PASSWORD);
        expectedRecord.put(ATTR_MEMBER_OF, "");

        Map<String, String> realRecord = CsvTestUtil.findRecord(createConfigurationNameEqualsUid(), USER_MEMBER_ID);
        assertEquals(expectedRecord, realRecord);

        AssertJUnit.assertEquals(4, handler.getObjects().size());
    }

    @Test
    public void deleteReferenceSubjectWithReferentialIntegrityReferenceIsAccess() throws Exception {
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

        connector.delete(ObjectClass.ACCOUNT, new Uid("2"), null);

        ListResultHandler handler = new ListResultHandler();
        connector.search(ObjectClass.ACCOUNT, null, handler, null);

        Map<String, String> realRecord = CsvTestUtil.findRecord(createConfigurationReferencedObjectClass(
                "./target/access.csv", ATTR_ID), ATTR_ID, "2");
        assertEquals(null, realRecord);

        AssertJUnit.assertEquals(1, handler.getObjects().size());
    }

    @Test
    public void deleteReferenceObjectWithReferentialIntegrityReferenceIsAccess() throws Exception {
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

        connector.delete(new ObjectClass("group"), new Uid("1"), null);

        ListResultHandler handler = new ListResultHandler();
        connector.search(new ObjectClass("group"), null, handler, null);

        Map<String, String> realRecord = CsvTestUtil.findRecord(createConfigurationReferencedObjectClass(
                "./target/access.csv", ATTR_ID), ATTR_ID, "2");
        assertEquals(null, realRecord);

        AssertJUnit.assertEquals(1, handler.getObjects().size());
    }
}
