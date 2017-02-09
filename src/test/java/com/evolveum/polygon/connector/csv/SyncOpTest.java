package com.evolveum.polygon.connector.csv;

import com.evolveum.polygon.connector.csv.util.CsvTestUtil;
import org.apache.commons.io.FileUtils;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.*;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.io.File;
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
            connector.sync(ObjectClass.ACCOUNT, oldToken, delta ->  {
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
}
