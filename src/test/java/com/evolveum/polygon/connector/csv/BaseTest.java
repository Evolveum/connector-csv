package com.evolveum.polygon.connector.csv;

import org.apache.commons.io.FileUtils;
import org.identityconnectors.framework.api.APIConfiguration;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.api.ConnectorFacadeFactory;
import org.identityconnectors.test.common.TestHelpers;

import java.io.File;
import java.io.IOException;

/**
 * Created by Viliam Repan (lazyman).
 */
public abstract class BaseTest {

    private static final String CSV_FILE_PATH = "./target/data.csv";

    protected CsvConfiguration createConfiguration() {
        CsvConfiguration config = new CsvConfiguration();
        config.setUniqueAttribute("uid");
        config.setPasswordAttribute("password");

        return config;
    }

    protected ConnectorFacade setupConnector(String csvTemplate) throws IOException {
        return setupConnector(csvTemplate, createConfiguration());
    }

    protected ConnectorFacade setupConnector(String csvTemplate, CsvConfiguration config) throws IOException {
        File file = new File(CSV_FILE_PATH);
        file.delete();

        FileUtils.copyFile(new File("./src/test/resources" + csvTemplate), new File(CSV_FILE_PATH));

        config.setFilePath(new File(CSV_FILE_PATH));

        config.validate();

        ConnectorFacadeFactory factory = ConnectorFacadeFactory.getInstance();

        APIConfiguration impl = TestHelpers.createTestConfiguration(CsvConnector.class, config);
        return factory.newInstance(impl);
    }
}
