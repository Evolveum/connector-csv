package com.evolveum.polygon.connector.csv;

import com.evolveum.polygon.connector.csv.util.ListResultHandler;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.exceptions.InvalidAttributeValueException;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.ConnectorObject;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.framework.common.objects.filter.EqualsFilter;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;

/**
 * Created by Viliam Repan (lazyman).
 */
public class SearchOpTest extends BaseTest {

    @Test
    public void findAllAccounts() throws Exception {
        ConnectorFacade connector = setupConnector("/search.csv");

        ListResultHandler handler = new ListResultHandler();
        connector.search(ObjectClass.ACCOUNT, null, handler, null);

        List<ConnectorObject> objects = handler.getObjects();
        AssertJUnit.assertEquals(2, objects.size());
    }

    @Test
    public void emptyQuotedUniqueAttributeValue() throws Exception {
        assertSearchFails(
                "/search-empty-unique-quoted.csv"
        );
    }

    @Test
    public void emptyUnquotedUniqueAttributeValue() throws Exception {
        assertSearchFails(
                "/search-empty-unique-unquoted.csv"
        );
    }

    @Test
    public void blankUniqueAttributeValue() throws Exception {
        assertSearchFails(
                "/search-blank-unique.csv"
        );
    }

    @Test
    public void validRowsWithUniqueAttribute() throws Exception {
        ConnectorFacade connector = setupConnector("/search-empnum-valid.csv", createEmpnumConfiguration());

        ListResultHandler handler = new ListResultHandler();
        connector.search(ObjectClass.ACCOUNT, null, handler, null);

        List<ConnectorObject> objects = handler.getObjects();
        AssertJUnit.assertEquals(2, objects.size());
        AssertJUnit.assertEquals(new Uid("1001"), objects.get(0).getUid());
        AssertJUnit.assertEquals(new Uid("1002"), objects.get(1).getUid());
    }

    @Test
    public void noConfiguredNameAttributeUsesUniqueAttributeFallback() throws Exception {
        ConnectorFacade connector = setupConnector("/search-empnum-valid.csv", createEmpnumConfiguration());

        ListResultHandler handler = new ListResultHandler();
        connector.search(ObjectClass.ACCOUNT, null, handler, null);

        List<ConnectorObject> objects = handler.getObjects();
        AssertJUnit.assertEquals(2, objects.size());
        AssertJUnit.assertEquals("1001", objects.get(0).getName().getNameValue());
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

    /**
     * MID-10391
     */
    @Test
    public void searchWithCustomFieldDelimiter() throws Exception {
        CsvConfiguration config = new CsvConfiguration();
        config.setFilePath(new File(CSV_FILE_PATH));
        config.setUniqueAttribute("id");
        config.setTrim(true);
        config.setMultivalueDelimiter("\\|");
        ConnectorFacade connector = setupConnector("/schema-custom-field-delimiter.csv", config);

        ListResultHandler handler = new ListResultHandler();
        connector.search(ObjectClass.ACCOUNT, null, handler, null);

        List<ConnectorObject> objects = handler.getObjects();
        AssertJUnit.assertEquals(1, objects.size());

        ConnectorObject object = objects.get(0);
        AssertJUnit.assertEquals(new Uid("1"), object.getUid());

        Attribute organization = object.getAttributeByName("organization");
        List<Object> organizations = organization.getValue();
        AssertJUnit.assertEquals(2, organizations.size());

        AssertJUnit.assertTrue(organizations.contains("org1"));
        AssertJUnit.assertTrue(organizations.contains("org2"));
    }

    private CsvConfiguration createEmpnumConfiguration() {
        CsvConfiguration config = new CsvConfiguration();
        config.setFilePath(new File(CSV_FILE_PATH));
        config.setFieldDelimiter(",");
        config.setUniqueAttribute("empnum");
        config.setPasswordAttribute(null);
        return config;
    }

    private void assertSearchFails(String csvTemplate) throws Exception {
        ConnectorFacade connector = setupConnector(csvTemplate, createEmpnumConfiguration());

        try {
            connector.search(ObjectClass.ACCOUNT, null, new ListResultHandler(), null);
            AssertJUnit.fail("Expected " + InvalidAttributeValueException.class.getSimpleName());
        } catch (Exception ex) {
            AssertJUnit.assertEquals(InvalidAttributeValueException.class, ex.getClass());
            AssertJUnit.assertEquals("CSV validation failed. Required unique attribute 'empnum' is empty at record 2. " +
                    "Each record must contain a non-empty value for 'empnum'.", ex.getMessage());
        }
    }
}
