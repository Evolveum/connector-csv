package com.evolveum.polygon.connector.csv;

import com.evolveum.polygon.connector.csv.util.ListResultHandler;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeUtil;
import org.identityconnectors.framework.common.objects.ConnectorObject;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.framework.common.objects.filter.EqualsFilter;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Collections;
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

    @Test
    public void findOneWithGroupByEnabled() throws Exception {
        CsvConfiguration config = new CsvConfiguration();
        config.setFilePath(new File(CSV_FILE_PATH));
        config.setUniqueAttribute("id");
        config.setMultivalueAttributes("tel");
        config.setMultivalueDelimiter(",");
        config.setGroupByEnabled(true);
        ConnectorFacade connector = setupConnector("/search-grouping.csv", config);

        ListResultHandler handler = new ListResultHandler();
        EqualsFilter ef = new EqualsFilter(new Uid("1"));
        connector.search(ObjectClass.ACCOUNT, ef, handler, null);

        List<ConnectorObject> objects = handler.getObjects();

        AssertJUnit.assertEquals(1, objects.size());
        ConnectorObject connectorObject = objects.get(0);
        Attribute id = connectorObject.getAttributeByName(Uid.NAME);
        AssertJUnit.assertEquals("1", AttributeUtil.getStringValue(id));
        Attribute dept = connectorObject.getAttributeByName("dept");
        AssertJUnit.assertEquals("abc", AttributeUtil.getStringValue(dept));
        Attribute title = connectorObject.getAttributeByName("title");
        AssertJUnit.assertEquals("engineer", AttributeUtil.getStringValue(title));
        Attribute tel = connectorObject.getAttributeByName("tel");
        AssertJUnit.assertEquals(List.of("1111"), tel.getValue());
        Attribute rawJson = connectorObject.getAttributeByName("__RAW_JSON__");
        AssertJUnit.assertNotNull(rawJson);
        String jsonValue = AttributeUtil.getStringValue(rawJson);
        AssertJUnit.assertEquals("[{\"name\":\"john\",\"tel\":[\"1111\"],\"id\":\"1\",\"dept\":\"abc\",\"title\":\"engineer\"},{\"name\":\"john\",\"tel\":[\"1111\"],\"id\":\"1\",\"dept\":\"efg\",\"title\":\"manager\"}]", jsonValue);
    }

    @Test
    public void findAllWithGroupByEnabled() throws Exception {
        CsvConfiguration config = new CsvConfiguration();
        config.setFilePath(new File(CSV_FILE_PATH));
        config.setUniqueAttribute("id");
        config.setMultivalueAttributes("tel");
        config.setMultivalueDelimiter(",");
        config.setGroupByEnabled(true);
        ConnectorFacade connector = setupConnector("/search-grouping.csv", config);

        ListResultHandler handler = new ListResultHandler();
        connector.search(ObjectClass.ACCOUNT, null, handler, null);

        List<ConnectorObject> objects = handler.getObjects();

        AssertJUnit.assertEquals(3, objects.size());

        ConnectorObject connectorObject = objects.get(0);
        Attribute id = connectorObject.getAttributeByName(Uid.NAME);
        AssertJUnit.assertEquals("1", AttributeUtil.getStringValue(id));
        Attribute name = connectorObject.getAttributeByName("name");
        AssertJUnit.assertEquals("john", AttributeUtil.getStringValue(name));
        Attribute dept = connectorObject.getAttributeByName("dept");
        AssertJUnit.assertEquals("abc", AttributeUtil.getStringValue(dept));
        Attribute title = connectorObject.getAttributeByName("title");
        AssertJUnit.assertEquals("engineer", AttributeUtil.getStringValue(title));
        Attribute tel = connectorObject.getAttributeByName("tel");
        AssertJUnit.assertEquals(List.of("1111"), tel.getValue());
        Attribute rawJson = connectorObject.getAttributeByName("__RAW_JSON__");
        AssertJUnit.assertNotNull(rawJson);
        String jsonValue = AttributeUtil.getStringValue(rawJson);
        AssertJUnit.assertEquals("[{\"name\":\"john\",\"tel\":[\"1111\"],\"id\":\"1\",\"dept\":\"abc\",\"title\":\"engineer\"},{\"name\":\"john\",\"tel\":[\"1111\"],\"id\":\"1\",\"dept\":\"efg\",\"title\":\"manager\"}]", jsonValue);

        connectorObject = objects.get(1);
        id = connectorObject.getAttributeByName(Uid.NAME);
        AssertJUnit.assertEquals("2", AttributeUtil.getStringValue(id));
        name = connectorObject.getAttributeByName("name");
        AssertJUnit.assertEquals("jack", AttributeUtil.getStringValue(name));
        dept = connectorObject.getAttributeByName("dept");
        AssertJUnit.assertEquals("abc", AttributeUtil.getStringValue(dept));
        title = connectorObject.getAttributeByName("title");
        AssertJUnit.assertEquals("manager", AttributeUtil.getStringValue(title));
        tel = connectorObject.getAttributeByName("tel");
        AssertJUnit.assertEquals(List.of("1111", "2222"), tel.getValue());
        rawJson = connectorObject.getAttributeByName("__RAW_JSON__");
        AssertJUnit.assertNotNull(rawJson);
        jsonValue = AttributeUtil.getStringValue(rawJson);
        AssertJUnit.assertEquals("[{\"name\":\"jack\",\"tel\":[\"1111\",\"2222\"],\"id\":\"2\",\"dept\":\"abc\",\"title\":\"manager\"}]", jsonValue);

        connectorObject = objects.get(2);
        id = connectorObject.getAttributeByName(Uid.NAME);
        AssertJUnit.assertEquals("3", AttributeUtil.getStringValue(id));
        name = connectorObject.getAttributeByName("name");
        AssertJUnit.assertEquals("bob", AttributeUtil.getStringValue(name));
        dept = connectorObject.getAttributeByName("dept");
        AssertJUnit.assertNull(dept);
        title = connectorObject.getAttributeByName("title");
        AssertJUnit.assertNull(title);
        tel = connectorObject.getAttributeByName("tel");
        AssertJUnit.assertNull(tel);
        rawJson = connectorObject.getAttributeByName("__RAW_JSON__");
        AssertJUnit.assertNotNull(rawJson);
        jsonValue = AttributeUtil.getStringValue(rawJson);
        AssertJUnit.assertEquals("[{\"name\":\"bob\",\"tel\":[],\"id\":\"3\",\"dept\":\"\",\"title\":\"\"}]", jsonValue);
    }
}
