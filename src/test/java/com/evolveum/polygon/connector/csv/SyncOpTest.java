package com.evolveum.polygon.connector.csv;

import com.evolveum.polygon.connector.csv.util.CsvTestUtil;
import com.evolveum.polygon.connector.csv.util.Util;
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

import javax.xml.ws.Holder;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;

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
            SyncToken oldToken = connector.getLatestSyncToken(ObjectClass.ACCOUNT);
            assertEquals("1300734815289", oldToken.getValue());

            final List<SyncDelta> deltas = new ArrayList<>();
            connector.sync(ObjectClass.ACCOUNT, oldToken, delta -> true, null);
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
            SyncToken oldToken = connector.getLatestSyncToken(ObjectClass.ACCOUNT);
            assertEquals("1300734815289", oldToken.getValue());
            connector.sync(ObjectClass.ACCOUNT, oldToken, delta -> {
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
            SyncToken oldToken = connector.getLatestSyncToken(ObjectClass.ACCOUNT);
            assertEquals("1300734815289", oldToken.getValue());

            final List<SyncDelta> deltas = new ArrayList<>();
            connector.sync(ObjectClass.ACCOUNT, oldToken, delta -> {
                deltas.add(delta);
                return true;
            }, null);

            AssertJUnit.assertEquals(3, deltas.size());

            SyncToken token = connector.getLatestSyncToken(ObjectClass.ACCOUNT);

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

                final Holder<SyncToken> tokenHolder = new Holder();

                final List<SyncDelta> deltas = new ArrayList<>();
                connector.sync(ObjectClass.ACCOUNT, token, new SyncTokenResultsHandler() {

                    @Override
                    public void handleResult(SyncToken syncToken) {
                        tokenHolder.value = syncToken;
                    }

                    @Override
                    public boolean handle(SyncDelta delta) {
                        deltas.add(delta);
                        return true;
                    }
                }, null);

                Thread.sleep(SYNC_WAIT_TIME);

                AssertJUnit.assertEquals(4, deltas.size());

                SyncToken newToken = tokenHolder.value;
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
}
