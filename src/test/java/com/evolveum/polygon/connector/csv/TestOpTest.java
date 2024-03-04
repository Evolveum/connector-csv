package com.evolveum.polygon.connector.csv;

import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.testng.annotations.Test;

import java.io.File;

/**
 * Created by Viliam Repan (lazyman).
 */
public class TestOpTest extends BaseTest {

    @Test
    public void testGoodConfiguration() throws Exception {
        ConnectorFacade connector = setupConnector("/create-empty.csv");
        connector.test();

        //todo asserts
    }

    @Test
    public void badHeader() throws Exception {
        ConnectorFacade connector = setupConnector("/test-bad.csv");
        connector.test();

        //todo asserts
    }

    @Test(expectedExceptions = ConfigurationException.class)
    public void noHeader() throws Exception {
        ConnectorFacade connector = setupConnector("/test-bad-1.csv");
        connector.test();
    }

    @Test
    public void headerWithUTF8BOM() throws Exception {
        CsvConfiguration config = new CsvConfiguration();
        config.setFilePath(new File(CSV_FILE_PATH));
        config.setUniqueAttribute("userid");
        config.setTrim(true);

        ConnectorFacade connector = setupConnector("/test-bom.csv", config);
        connector.test();
    }
}
