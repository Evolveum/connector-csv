package com.evolveum.polygon.connector.csv;

import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.testng.annotations.Test;

/**
 * Created by Viliam Repan (lazyman).
 */
public class TestOpTest extends BaseTest {

    @Test
    public void testGoodConfiguration() throws Exception {
        ConnectorFacade connector = setupConnector("/create-backup-empty.csv");
        connector.test();
    }

    @Test(expectedExceptions = ConfigurationException.class)
    public void badHeader() throws Exception {
        ConnectorFacade connector = setupConnector("/test-bad.csv");
        connector.test();
    }

    @Test(expectedExceptions = ConfigurationException.class)
    public void noHeader() throws Exception {
        ConnectorFacade connector = setupConnector("/test-bad-1.csv");
        connector.test();
    }
}
