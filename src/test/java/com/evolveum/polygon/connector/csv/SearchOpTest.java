package com.evolveum.polygon.connector.csv;

import com.evolveum.polygon.connector.csv.util.ListResultHandler;
import org.apache.commons.io.FileUtils;

import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.*;
import org.identityconnectors.framework.common.objects.filter.EqualsFilter;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.io.File;
import java.util.*;

import static com.evolveum.polygon.connector.csv.util.Util.ASSOC_ATTR_ACCESS;
import static com.evolveum.polygon.connector.csv.util.Util.ASSOC_ATTR_GROUP;

/**
 * Created by Viliam Repan (lazyman).
 */
public class SearchOpTest extends BaseTest {
    private static final Log LOG = Log.getLog(SearchOpTest.class);

    @Test
    public void findAllAccounts() throws Exception {
        ConnectorFacade connector = setupConnector("/search.csv");

        ListResultHandler handler = new ListResultHandler();
        connector.search(ObjectClass.ACCOUNT, null, handler, null);

        List<ConnectorObject> objects = handler.getObjects();
        AssertJUnit.assertEquals(2, objects.size());
    }

    @Test
    public void findAllAccountsNativeAssociationsGroupMultiParameters() throws Exception {
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

        ListResultHandler handler = new ListResultHandler();
        connector.search(ObjectClass.ACCOUNT, null, handler, null);

        List<ConnectorObject> objects = handler.getObjects();
        AssertJUnit.assertEquals(2, objects.size());

        Map<String, Set<Uid>> listOfReferencedObjectsBySubject = new HashMap<>();
        listOfReferencedObjectsBySubject.put("1", Set.of(new Uid("1"), new Uid("2"), new Uid("3")));
        listOfReferencedObjectsBySubject.put("2", Set.of(new Uid("1")));

        if (objects != null && !objects.isEmpty()) {
            for (ConnectorObject o : objects) {
                Uid uid = o.getUid();

                String uidVal = uid.getUidValue();
                Set<Uid> referencedUidList = listOfReferencedObjectsBySubject.get(uidVal);
                objectContainsReferenceToObject(o, new ObjectClass("group"), referencedUidList,
                        null, Set.of(
                                ASSOC_ATTR_GROUP+"-"+ATTR_MEMBERS_TEST,
                                ASSOC_ATTR_GROUP+"-"+ATTR_MEMBERS_ADMIN,
                                ASSOC_ATTR_GROUP+"-"+ATTR_MEMBERS_DEFAULT));
            }
        }
    }

    @Test
    public void findAllAccountsNativeAssociationsMemberOf() throws Exception {

        CsvConfiguration config = createConfiguration();
        config.setUniqueAttribute("id");
        config.setNameAttribute("id");
        config.setMultivalueDelimiter(",");
        config.setMultivalueAttributes("memberOf");
        config.setTrim(true);
        config.setPasswordAttribute(null);

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

        ConnectorFacade connector = setupConnector("/account-memberOf.csv", config);

        ListResultHandler groupHandler = new ListResultHandler();
        connector.search(new ObjectClass("group"), null, groupHandler, null);

        List<ConnectorObject> groupObjects = groupHandler.getObjects();

        Map<String, Set<Uid>> listOfReferencedObjectsBySubject = new HashMap<>();
        listOfReferencedObjectsBySubject.put("2", Set.of(new Uid("1")));
        listOfReferencedObjectsBySubject.put("3", Set.of(new Uid("2"), new Uid("5")));
        listOfReferencedObjectsBySubject.put("4", Set.of(new Uid("2")));
        listOfReferencedObjectsBySubject.put("5", Set.of(new Uid("2")));

        if(groupObjects!=null && !groupObjects.isEmpty()){

            for (ConnectorObject o : groupObjects) {
                Uid uid = o.getUid();

                String uidVal = uid.getUidValue();
                Set<Uid> referencedUidList = listOfReferencedObjectsBySubject.get(uidVal);
                objectContainsReferenceToObject(o, new ObjectClass("group"),
                        referencedUidList, null, ASSOC_ATTR_GROUP);
            }

        }
        AssertJUnit.assertEquals(5, groupObjects.size());

        ListResultHandler accountHandler = new ListResultHandler();
        connector.search(ObjectClass.ACCOUNT, null, accountHandler, null);

        List<ConnectorObject> accountObjects = accountHandler.getObjects();

        listOfReferencedObjectsBySubject = new HashMap<>();
        listOfReferencedObjectsBySubject.put("123", Set.of(new Uid("1"), new Uid("2")));
        listOfReferencedObjectsBySubject.put("321", Set.of(new Uid("1"), new Uid("3")));
        listOfReferencedObjectsBySubject.put("111", Set.of(new Uid("4"), new Uid("5")));

        if(accountObjects!=null && !accountObjects.isEmpty()){

            for (ConnectorObject o : accountObjects) {
                Uid uid = o.getUid();

                String uidVal = uid.getUidValue();
                Set<Uid> referencedUidSet = listOfReferencedObjectsBySubject.get(uidVal);
                objectContainsReferenceToObject(o, new ObjectClass("group"), referencedUidSet,
                        null, ASSOC_ATTR_GROUP);
            }
        }
        AssertJUnit.assertEquals(4, accountObjects.size());
    }

    @Test
    public void findAllAccountsNativeAssociationsAccess() throws Exception {

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

        ConnectorFacade connector = setupConnector("/schema-repeating-column.csv", config);

        ListResultHandler groupHandler = new ListResultHandler();
        connector.search(new ObjectClass("group"), null, groupHandler, null);

        List<ConnectorObject> groupObjects = groupHandler.getObjects();
        AssertJUnit.assertEquals(2, groupObjects.size());

        ListResultHandler accountHandler = new ListResultHandler();
        connector.search(ObjectClass.ACCOUNT, null, accountHandler, null);

        List<ConnectorObject> accountObjects = accountHandler.getObjects();

        Map<String,  Map<Uid, Set<Uid>>> listOfReferencedObjectsBySubject = new HashMap<>();
        listOfReferencedObjectsBySubject.put("1", Map.of(new Uid("1"), Set.of(new Uid("1"))));
        listOfReferencedObjectsBySubject.put("2", Map.of(new Uid("2"), Set.of(new Uid("1")),
                new Uid("3"), Set.of(new Uid("1"))));

        if(accountObjects!=null && !accountObjects.isEmpty()){

            for (ConnectorObject o : accountObjects) {
                Uid uid = o.getUid();

                String uidVal = uid.getUidValue();
                Map<Uid, Set<Uid>> referencedUidMap = listOfReferencedObjectsBySubject.get(uidVal);
                objectContainsReferenceToObject(o, new ObjectClass("access"),
                        new ObjectClass("group"), referencedUidMap, null, ASSOC_ATTR_ACCESS);
            }
        }
        AssertJUnit.assertEquals(2, accountObjects.size());
    }

    @Test
    public void findOneWithRepeatingColumn() throws Exception {
        CsvConfiguration config = new CsvConfiguration();
        config.setFilePath(new File(CSV_FILE_PATH));
        config.setUniqueAttribute("id");
        config.setTrim(true);
        ConnectorFacade connector = setupConnector("/schema-repeating-column.csv", config);

        ListResultHandler handler = new ListResultHandler();
        EqualsFilter ef = new EqualsFilter(new Uid("1"));
        connector.search(ObjectClass.ACCOUNT, ef, handler, null);

        List<ConnectorObject> objects = handler.getObjects();
        AssertJUnit.assertEquals(1, objects.size());

        //todo asserts
    }

    @Test
    public void findAllAccountsRepeatingColumn() throws Exception {
        CsvConfiguration config = new CsvConfiguration();
        config.setFilePath(new File(CSV_FILE_PATH));
        config.setUniqueAttribute("id");
        config.setTrim(true);
        ConnectorFacade connector = setupConnector("/schema-repeating-column.csv", config);

        ListResultHandler handler = new ListResultHandler();
        connector.search(ObjectClass.ACCOUNT, null, handler, null);

        List<ConnectorObject> objects = handler.getObjects();
        AssertJUnit.assertEquals(2, objects.size());

        //todo asserts
    }

    @Test(expectedExceptions = ConnectorException.class)
    public void searchWrongNumberColumnCountInRow() throws Exception {
        CsvConfiguration config = new CsvConfiguration();
        config.setFilePath(new File(CSV_FILE_PATH));
        config.setQuoteMode("ALL");
        config.setFieldDelimiter(",");
        config.setMultivalueDelimiter(";");
        config.setUniqueAttribute("login");
        config.setPasswordAttribute("password");

        ConnectorFacade connector = setupConnector("/search-wrong-column-count-row.csv", config);
        connector.search(ObjectClass.ACCOUNT, null, new ListResultHandler(), null);
    }

}
