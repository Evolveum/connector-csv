package com.evolveum.polygon.connector.csv;

import com.evolveum.polygon.connector.csv.util.ListResultHandler;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.io.File;

/**
 * Created by lazyman on 22/05/2017.
 */
public class ConfigurationTest extends BaseTest {

    @Test
    public void readOnlyMode() throws Exception {
        CsvConfiguration config = new CsvConfiguration();

        File data = new File(BaseTest.CSV_FILE_PATH);
        config.setFilePath(data);
        config.setTmpFolder(null);
        config.setUniqueAttribute(ATTR_UID);
        config.setPasswordAttribute(ATTR_PASSWORD);
        config.setReadOnly(true);

        ConnectorFacade connector = setupConnector("/create.csv", config);

        data.setWritable(false);

        ListResultHandler handler = new ListResultHandler();
        connector.search(ObjectClass.ACCOUNT, null, handler, null);

        AssertJUnit.assertEquals(1, handler.getObjects().size());

        data.setWritable(true);
    }

    @Test(expectedExceptions = ConfigurationException.class)
    public void testMissingMultivalueDelimiter() throws Exception {
        CsvConfiguration config = new CsvConfiguration();
        config.setUniqueAttribute("uid");
        config.setMultivalueAttributes("lastName");
        //missing multivalueDelimiter
        config.setFieldDelimiter(";");
        config.setReadOnly(true);

        ConnectorFacade connector = setupConnector("/create.csv", config);
        connector.test();
    }

    @Test
    public void testTabDelimiter() throws Exception {
        CsvConfiguration config = new CsvConfiguration();
        config.setUniqueAttribute("uid");
        config.setFieldDelimiter("\\t");
        config.setReadOnly(true);

        ConnectorFacade connector = setupConnector("/create-tabs.tsv", config);
        connector.test();

    }
}
