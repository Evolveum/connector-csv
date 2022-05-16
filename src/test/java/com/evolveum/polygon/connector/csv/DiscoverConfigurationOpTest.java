package com.evolveum.polygon.connector.csv;

import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.identityconnectors.framework.common.objects.SuggestedValues;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;
import java.util.Map;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

/**
 * Created by Viliam Repan (lazyman).
 */
public class DiscoverConfigurationOpTest extends BaseTest {

    @Test
    public void testPartialConfiguration() throws Exception {
        ConnectorFacade connector = setupConnector("/discover-configuration.csv");
        connector.testPartialConfiguration();
    }

    @Test (expectedExceptions = ConfigurationException.class)
    public void testPartialConfigurationFail() throws Exception {
        CsvConfiguration config = new CsvConfiguration();
        config.setFilePath(new File("nonexist_file.csv"));
        ConnectorFacade connector = createNewInstance(config);
        connector.testPartialConfiguration();
    }

    @Test
    public void testDiscoverConfiguration() throws Exception {
        ConnectorFacade connector = setupConnector("/discover-configuration.csv");
        Map<String, SuggestedValues> suggestions = connector.discoverConfiguration();

        assertSuggestion(suggestions, "passwordAttribute", "myPasswordAttr");
        assertSuggestion(suggestions, "multivalueDelimiter", ';');
        assertSuggestion(suggestions, "quote", '"');
        assertSuggestion(suggestions, "commentMarker", '#');
        assertSuggestion(suggestions, "recordSeparator", '|');
        assertSuggestion(suggestions, "nameAttribute", "myNameAttr");
    }

    private void assertSuggestion(Map<String, SuggestedValues> suggestions, String attributeName, Object expectedValue) {
        assertTrue("Suggestions not contain suggestion for attribute " + attributeName, suggestions.containsKey(attributeName));
        List<Object> values = suggestions.get(attributeName).getValues();
        assertTrue("Suggestions contains wrong suggestion value for attribute " + attributeName, values.contains(expectedValue));
    }

    protected CsvConfiguration createConfiguration() {
        return createConfigurationNameEqualsUid();
    }

}
