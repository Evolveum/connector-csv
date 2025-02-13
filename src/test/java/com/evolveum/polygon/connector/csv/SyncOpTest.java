package com.evolveum.polygon.connector.csv;

import static com.evolveum.polygon.connector.csv.util.Util.ASSOC_ATTR_ACCESS;
import static com.evolveum.polygon.connector.csv.util.Util.ASSOC_ATTR_GROUP;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.*;

import org.apache.commons.io.FileUtils;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.*;
import org.identityconnectors.framework.spi.SyncTokenResultsHandler;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.evolveum.polygon.connector.csv.util.CsvTestUtil;
import com.evolveum.polygon.connector.csv.util.Util;

/**
 * Created by Viliam Repan (lazyman).
 */
public class SyncOpTest extends BaseTest {

    private static final Log LOG = Log.getLog(SyncOpTest.class);

    @Test(expectedExceptions = ConnectorException.class)
    public void syncLock() throws Exception {
        CsvConfiguration config = createConfiguration();
        config.setTrim(true);
        ConnectorFacade connector = setupConnector("/sync.csv", config);

        File oldSyncFile = new File("./target/data.csv.sync.1300734815289");
        FileUtils.copyFile(new File(TEMPLATE_FOLDER_PATH, "sync.csv.1300734815289"), oldSyncFile);

        File lock = new File("./target/data.csv." + Util.SYNC_LOCK_EXTENSION);
        lock.createNewFile();

        try {
//            SyncToken oldToken = connector.getLatestSyncToken(ObjectClass.ACCOUNT);
//            assertEquals("1300734815289", oldToken.getValue());

            final List<SyncDelta> deltas = new ArrayList<>();
            connector.sync(ObjectClass.ACCOUNT, new SyncToken("1300734815289"), delta -> true, null);
        } finally {
            CsvTestUtil.deleteAllSyncFiles();
            lock.delete();
        }
    }

    @Test(expectedExceptions = ConnectorException.class)
    public void badHeaders() throws Exception {
        ConnectorFacade connector = setupConnector("/sync-bad.csv", createConfiguration());

        File oldSyncFile = new File("./target/data.csv.sync.1300734815289");
        FileUtils.copyFile(new File(TEMPLATE_FOLDER_PATH, "sync-bad.csv.1300734815289"), oldSyncFile);

        try {
//            SyncToken oldToken = connector.getLatestSyncToken(ObjectClass.ACCOUNT);
//            assertEquals("1300734815289", oldToken.getValue());
            connector.sync(ObjectClass.ACCOUNT, new SyncToken("1300734815289"), delta -> {
                Assert.fail("This test should fail on headers check.");
                return false;
            }, null);
        } finally {
            CsvTestUtil.deleteAllSyncFiles();
        }

        Assert.fail("This test should fail on headers check.");
    }

    @Test
    public void syncTest() throws Exception {
        CsvConfiguration config = createConfiguration();
        config.setTrim(true);
        ConnectorFacade connector = setupConnector("/sync.csv", config);

        File oldSyncFile = new File("./target/data.csv.sync.1300734815289");
        FileUtils.copyFile(new File(TEMPLATE_FOLDER_PATH, "sync.csv.1300734815289"), oldSyncFile);

        try {
//            SyncToken oldToken = connector.getLatestSyncToken(ObjectClass.ACCOUNT);
//            assertEquals("1300734815289", oldToken.getValue());

            final List<SyncDelta> deltas = new ArrayList<>();
            connector.sync(ObjectClass.ACCOUNT, new SyncToken("1300734815289"), delta -> {
                deltas.add(delta);
                return true;
            }, null);

            AssertJUnit.assertEquals(3, deltas.size());

            SyncToken token = deltas.get(0).getToken();

            Map<String, SyncDelta> deltaMap = createSyncDeltaTestMap(token);
            for (SyncDelta delta : deltas) {
                SyncDelta syncDelta = deltaMap.get(delta.getUid().getUidValue());
                deltaMap.remove(delta.getUid().getUidValue());
                assertEquals(syncDelta, delta);
            }
            assertTrue(deltaMap.isEmpty(), "deltas didn't match");
        } finally {
            CsvTestUtil.deleteAllSyncFiles();
        }
    }

    @Test
    public void syncActualTokenTest() throws Exception {
        CsvConfiguration config = createConfiguration();
        config.setTrim(true);
        ConnectorFacade connector = setupConnector("/sync.csv", config);

        try {
            long timestampBefore = System.currentTimeMillis();
            SyncToken token = connector.getLatestSyncToken(ObjectClass.ACCOUNT);
            long timestampToken = Long.valueOf((String) token.getValue());
            long timestampAfter = System.currentTimeMillis();

            assertTrue(timestampToken >= timestampBefore && timestampToken <= timestampAfter,
                    "wrong token " + timestampToken + ", expected token between " + timestampBefore + " and " + timestampAfter);
        } finally {
            CsvTestUtil.deleteAllSyncFiles();
        }
    }

    private Map<String, SyncDelta> createSyncDeltaTestMap(SyncToken token) {
        Map<String, SyncDelta> map = new HashMap<String, SyncDelta>();

        SyncDeltaBuilder builder = new SyncDeltaBuilder();
        builder.setDeltaType(SyncDeltaType.DELETE);
        builder.setObjectClass(ObjectClass.ACCOUNT);
        builder.setToken(token);
        builder.setUid(new Uid("vilo"));

        ConnectorObjectBuilder cBuilder = new ConnectorObjectBuilder();
        cBuilder.setName("vilo");
        cBuilder.setUid("vilo");
        cBuilder.setObjectClass(ObjectClass.ACCOUNT);
        cBuilder.addAttribute(ATTR_FIRST_NAME, "viliam");
        cBuilder.addAttribute(ATTR_LAST_NAME, "repan");
        cBuilder.addAttribute(OperationalAttributes.PASSWORD_NAME, new GuardedString("Z29vZA==".toCharArray()));
        builder.setObject(cBuilder.build());

        map.put("vilo", builder.build());

        // =====================

        builder = new SyncDeltaBuilder();
        builder.setDeltaType(SyncDeltaType.UPDATE);
        builder.setToken(token);
        builder.setUid(new Uid("miso"));

        cBuilder = new ConnectorObjectBuilder();
        cBuilder.setName("miso");
        cBuilder.setUid("miso");
        cBuilder.setObjectClass(ObjectClass.ACCOUNT);
        cBuilder.addAttribute(ATTR_FIRST_NAME, "michal");
        cBuilder.addAttribute(ATTR_LAST_NAME, "LastnameChange");
        cBuilder.addAttribute(OperationalAttributes.PASSWORD_NAME, new GuardedString("Z29vZA==".toCharArray()));

        builder.setObject(cBuilder.build());
        map.put("miso", builder.build());

        // =====================

        builder = new SyncDeltaBuilder();
        builder.setDeltaType(SyncDeltaType.CREATE);
        builder.setToken(token);
        builder.setUid(new Uid("apple"));

        cBuilder = new ConnectorObjectBuilder();
        cBuilder.setName("apple");
        cBuilder.setUid("apple");
        cBuilder.setObjectClass(ObjectClass.ACCOUNT);
        cBuilder.addAttribute(ATTR_FIRST_NAME, "small");
        cBuilder.addAttribute(ATTR_LAST_NAME, "smallAppleChange");
        cBuilder.addAttribute(OperationalAttributes.PASSWORD_NAME, new GuardedString("Z29vZA==".toCharArray()));

        builder.setObject(cBuilder.build());
        map.put("apple", builder.build());

        return map;
    }

    @Test
    public void loopSync() throws Exception {
        final long SYNC_WAIT_TIME = 200;
        final long RUN_TIME = 15 * 1000;

        CsvTestUtil.deleteAllSyncFiles();

        CsvConfiguration config = createConfiguration();
        config.setTrim(true);
        ConnectorFacade connector = setupConnector("/sync-loop-1.csv", config);

        int runCount = 0;
        try {
            SyncToken startToken = connector.getLatestSyncToken(ObjectClass.ACCOUNT);
            switchCsvFile(true);

            long oldTokenValue;
            boolean useSecond = false;

            SyncToken token = startToken;
            long startTime = System.currentTimeMillis();
            while (startTime + RUN_TIME > System.currentTimeMillis()) {
                runCount++;

                oldTokenValue = Long.parseLong((String) token.getValue());

                final SyncToken[] tokenHolder = new SyncToken[1];

                final List<SyncDelta> deltas = new ArrayList<>();
                connector.sync(ObjectClass.ACCOUNT, token, new SyncTokenResultsHandler() {

                    @Override
                    public void handleResult(SyncToken syncToken) {
                        tokenHolder[0] = syncToken;
                    }

                    @Override
                    public boolean handle(SyncDelta delta) {
                        deltas.add(delta);
                        return true;
                    }
                }, null);

                Thread.sleep(SYNC_WAIT_TIME);

                AssertJUnit.assertEquals(4, deltas.size());

                SyncToken newToken = tokenHolder[0];
                if (newToken == null && !deltas.isEmpty()) {
                    newToken = deltas.get(0).getToken();
                } else {
                    newToken = startToken;
                }

                long newTokenValue = Long.parseLong((String) newToken.getValue());
                AssertJUnit.assertTrue(newTokenValue > oldTokenValue);

                token = newToken;

                switchCsvFile(useSecond);
                useSecond = !useSecond;
            }
        } finally {
            LOG.info("Run count: {0}", runCount);
            CsvTestUtil.deleteAllSyncFiles();
        }
    }

    private void switchCsvFile(boolean useSecond) throws IOException {
        String file = useSecond ? "sync-loop-2.csv" : "sync-loop-1.csv";

        File csv = new File(CSV_FILE_PATH);
        FileUtils.copyFile(new File(TEMPLATE_FOLDER_PATH, file), csv);
        FileUtils.touch(csv);

        LOG.info("Using second={0}, time: {1}", useSecond, csv.lastModified());
    }

    @Test
    public void syncTokenTest() throws Exception {
        CsvTestUtil.deleteAllSyncFiles();

        CsvConfiguration config = createConfiguration();
        config.setTrim(true);
        ConnectorFacade connector = setupConnector("/sync-loop-1.csv", config);

        SyncToken token = connector.getLatestSyncToken(ObjectClass.ACCOUNT);
        switchCsvFile(true);

        int count = 0;
        SyncToken token2 = null;
        while (count < 15) {
            count++;

            token2 = doSync(connector, token);
            switchCsvFile(false);

            File toDelete = Util.createSyncFileName(Long.parseLong((String) token2.getValue()), config.getConfig());
            if (!toDelete.delete()) {
                throw new RuntimeException("Couldn't delete " + toDelete.getName());
            }

            doSync(connector, token2);
        }
    }

    private SyncToken doSync(ConnectorFacade connector, SyncToken token) throws Exception {
        final List<SyncDelta> deltas = new ArrayList<>();

        SyncToken newToken = connector.sync(ObjectClass.ACCOUNT, token, new SyncTokenResultsHandler() {

            @Override
            public void handleResult(SyncToken syncToken) {
            }

            @Override
            public boolean handle(SyncDelta delta) {
                deltas.add(delta);
                return true;
            }
        }, null);

        Thread.sleep(200);

        if (newToken == null && !deltas.isEmpty()) {
            newToken = deltas.get(0).getToken();
        }

        LOG.info("New token is {0}", newToken);

        return newToken;
    }

    @Test
    public void sync_ALL_WithSimpleAccountAndGroupAssociationsTest() throws Exception {

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

        File oldSyncFile = new File("./target/groups-access.csv.sync.1300734815289");
        FileUtils.copyFile(new File(TEMPLATE_FOLDER_PATH, "groups-access.csv.sync.1300734815289"), oldSyncFile);

        File oldDataSyncFile = new File("./target/data.csv.sync.1300734815289");
        FileUtils.copyFile(new File(TEMPLATE_FOLDER_PATH, "data.csv.sync.1300734815289"), oldDataSyncFile);

        try {

            final List<SyncDelta> deltas = new ArrayList<>();
            connector.sync(ObjectClass.ALL, new SyncToken("1300734815289"), delta -> {
                deltas.add(delta);
                return true;
            }, null);

            Map<ObjectClass, Map<String, Set<Uid>>> mapOfReferencedObjectsBySubject = new HashMap<>();
            mapOfReferencedObjectsBySubject.put(ObjectClass.ACCOUNT, Map.of("2", Set.of(new Uid("1"))));


            for (SyncDelta delta : deltas) {
                ConnectorObject o = delta.getObject();
                Uid uid = o.getUid();
                ObjectClass oc = o.getObjectClass();
                String uidVal = uid.getUidValue();

                if (mapOfReferencedObjectsBySubject.containsKey(oc)) {
                    Map<String, Set<Uid>> objectsByObjectClass = mapOfReferencedObjectsBySubject.get(oc);
                    if (objectsByObjectClass.containsKey(uidVal)) {
                        Set<Uid> referencedUidList = objectsByObjectClass.get(uidVal);
                        objectContainsReferenceToObject(o, new ObjectClass("group"), referencedUidList,
                                null, Set.of(ASSOC_ATTR_GROUP+"-"+ATTR_MEMBERS_DEFAULT,
                                        ASSOC_ATTR_GROUP+"-"+ATTR_MEMBERS_ADMIN));
                    }
                }
            }

            AssertJUnit.assertEquals(3, deltas.size());

        } finally {
            CsvTestUtil.deleteAllSyncFiles();
        }
    }
    @Test
    public void sync_ALL_WithComplexAccountAndGroupAssociationsTest() throws Exception {

        CsvConfiguration config = createConfiguration();
        config.setUniqueAttribute("id");
        config.setNameAttribute("id");
        config.setTrim(true);
        config.setPasswordAttribute(null);

        Set<String> values = Set.of(
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

        File oldSyncFile = new File("./target/access.csv.sync.1300734815289");
        FileUtils.copyFile(new File(TEMPLATE_FOLDER_PATH, "access.csv.sync.1300734815289"), oldSyncFile);

        File oldAccountDataSyncFile = new File("./target/data.csv.sync.1300734815289");
        FileUtils.copyFile(new File(TEMPLATE_FOLDER_PATH, "data.csv.sync.1300734815289"), oldAccountDataSyncFile);

        File oldgroupSyncFile = new File("./target/groups-no-member.csv.sync.1300734815289");
        FileUtils.copyFile(new File(TEMPLATE_FOLDER_PATH, "groups-no-member.csv.sync.1300734815289"), oldgroupSyncFile);

        try {

            final List<SyncDelta> deltas = new ArrayList<>();
            connector.sync(ObjectClass.ALL, new SyncToken("1300734815289"), delta -> {
                deltas.add(delta);
                return true;
            }, null);

            Map<ObjectClass, Map<String, Object>> mapOfReferencedObjectsBySubject = new HashMap<>();
            mapOfReferencedObjectsBySubject.put(new ObjectClass("access"), Map.of("2", Set.of(new Uid("1"))));
            mapOfReferencedObjectsBySubject.put(ObjectClass.ACCOUNT, Map.of("1", Map.of(new Uid("1"),
                    Set.of(new Uid("1")))));
            mapOfReferencedObjectsBySubject.put(ObjectClass.ACCOUNT, Map.of("2", Map.of(new Uid("2"),
                    Set.of(new Uid("1")), new Uid("3"), Set.of(new Uid("1")))));

            for (SyncDelta delta : deltas) {
                ConnectorObject o = delta.getObject();
                Uid uid = o.getUid();
                ObjectClass oc = o.getObjectClass();
                String uidVal = uid.getUidValue();

                if (mapOfReferencedObjectsBySubject.containsKey(oc)) {

                    Map<String, Object> objectsByObjectClassWithAssociationId =
                            mapOfReferencedObjectsBySubject.get(oc);
                    if (objectsByObjectClassWithAssociationId.containsKey(uidVal)) {
                        Object referencedUidList = objectsByObjectClassWithAssociationId.get(uidVal);

                        if (referencedUidList instanceof Map<?, ?>) {
                            objectContainsReferenceToObject(o, new ObjectClass("access"), new ObjectClass("group"),
                                    (Map<Uid, Set<Uid>>) referencedUidList, null, ASSOC_ATTR_ACCESS);
                        } else {
                            objectContainsReferenceToObject(o, new ObjectClass("group"),
                                    (Set<Uid>) referencedUidList, null, ASSOC_ATTR_GROUP);
                        }
                    }
                }
            }

            AssertJUnit.assertEquals(4, deltas.size());

        } finally {
            CsvTestUtil.deleteAllSyncFiles();
        }
    }
    @Test
    public void sync_ALL_WithAccountsNativeAssociationsMemberOf() throws Exception {
        CsvConfiguration config = createConfiguration();
        config.setUniqueAttribute("id");
        config.setNameAttribute("id");
        config.setMultivalueDelimiter(",");
        config.setMultivalueAttributes("memberOf");
        config.setTrim(true);
        config.setPasswordAttribute(null);

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

        ConnectorFacade connector = setupConnector("/account-memberOf.csv", config);

        File oldAccountDataSyncFile = new File("./target/data.csv.sync.1300734815299");
        FileUtils.copyFile(new File(TEMPLATE_FOLDER_PATH, "data.csv.sync.1300734815299"), oldAccountDataSyncFile);

        File oldgroupSyncFile = new File("./target/groups-memberOf.csv.sync.1300734815299");
        FileUtils.copyFile(new File(TEMPLATE_FOLDER_PATH, "groups-memberOf.csv.sync.1300734815299"), oldgroupSyncFile);

        try {
            final List<SyncDelta> deltas = new ArrayList<>();
            connector.sync(ObjectClass.ALL, new SyncToken("1300734815299"), delta -> {
                deltas.add(delta);
                return true;
            }, null);

            AssertJUnit.assertEquals(6, deltas.size());

            SyncToken token = deltas.get(0).getToken();

            Map<String, SyncDelta> deltaMap = createSyncDeltaTestMap(token);

            Map<ObjectClass, Map<String, Set<Uid>>> mapOfReferencedObjectsBySubject = new HashMap<>();
            mapOfReferencedObjectsBySubject.put(ObjectClass.ACCOUNT, Map.of("123", Set.of(new Uid("2"),
                    new Uid("1"))));
            mapOfReferencedObjectsBySubject.put(ObjectClass.ACCOUNT, Map.of("111", Set.of(new Uid("4"),
                    new Uid("5"))));

            mapOfReferencedObjectsBySubject.put(new ObjectClass("group"), Map.of("3", Set.of(new Uid("2"),
                    new Uid("5"))));
            mapOfReferencedObjectsBySubject.put(new ObjectClass("group"), Map.of("4",
                    Set.of(new Uid("2"))));
            mapOfReferencedObjectsBySubject.put(new ObjectClass("group"), Map.of("5",
                    Set.of(new Uid("2"))));


            for (SyncDelta delta : deltas) {
                ConnectorObject o = delta.getObject();
                Uid uid = o.getUid();
                ObjectClass oc = o.getObjectClass();
                String uidVal = uid.getUidValue();

                if (mapOfReferencedObjectsBySubject.containsKey(oc)) {
                    Map<String, Set<Uid>> objectsByObjectClass = mapOfReferencedObjectsBySubject.get(oc);
                    if (objectsByObjectClass.containsKey(uidVal)) {
                        Set<Uid> referencedUidList = objectsByObjectClass.get(uidVal);
                        objectContainsReferenceToObject(o, new ObjectClass("group"), referencedUidList,
                                null, ASSOC_ATTR_GROUP);
                    }
                }
            }
        } finally {
            CsvTestUtil.deleteAllSyncFiles();
        }
    }
}
