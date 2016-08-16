package com.evolveum.polygon.connector.csv;

import org.apache.commons.csv.*;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.exceptions.ConnectorIOException;
import org.identityconnectors.framework.common.objects.*;
import org.identityconnectors.framework.common.objects.filter.AbstractFilterTranslator;
import org.identityconnectors.framework.common.objects.filter.FilterTranslator;
import org.identityconnectors.framework.spi.Configuration;
import org.identityconnectors.framework.spi.Connector;
import org.identityconnectors.framework.spi.ConnectorClass;
import org.identityconnectors.framework.spi.operations.*;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.*;

/**
 * //todo locking!
 *
 * Created by Viliam Repan (lazyman).
 */
@ConnectorClass(
        displayNameKey = "UI_CSV_CONNECTOR_NAME",
        configurationClass = CsvConfiguration.class)
public class CsvConnector implements Connector, CreateOp, DeleteOp, TestOp, SchemaOp, SearchOp<String>,
        UpdateAttributeValuesOp, AuthenticateOp, ResolveUsernameOp, SyncOp {

    public static final String TMP_EXTENSION = ".tmp";

    private static final Log LOG = Log.getLog(CsvConnector.class);

    private CsvConfiguration configuration;

    @Override
    public Configuration getConfiguration() {
        return configuration;
    }

    @Override
    public void init(Configuration configuration) {
        if (!(configuration instanceof CsvConfiguration)) {
            throw new ConfigurationException("Configuration is not instance of " + CsvConfiguration.class.getName());
        }

        this.configuration = (CsvConfiguration) configuration;
    }

    @Override
    public void dispose() {
    }

    @Override
    public Uid create(ObjectClass objectClass, Set<Attribute> set, OperationOptions options) {
        //todo implement
        return null;
    }

    @Override
    public void delete(ObjectClass objectClass, Uid uid, OperationOptions options) {
        Util.assertAccount(objectClass);
        Util.notNull(uid, "Uid must not be null");

        File tmp = new File(configuration.getFilePath().getPath() + "." + System.currentTimeMillis() + TMP_EXTENSION);

        CSVFormat csv = createCsvFormat();
        try (Reader reader = Util.createReader(configuration);
             Writer writer = Util.createWriter(tmp, false, configuration)) {

            CSVPrinter printer = csv.print(writer);
            CSVParser parser = csv.parse(reader);
            Iterator<CSVRecord> iterator = parser.iterator();
            while (iterator.hasNext()) {
                CSVRecord record = iterator.next();
                ConnectorObject obj = createConnectorObject(record);

                if (uid.equals(obj.getUid().getUidValue())) {
                    continue;
                }

                printer.print(record);
            }

            configuration.getFilePath().delete();
            tmp.renameTo(configuration.getFilePath());
        } catch (Exception ex) {
            handleGenericException(ex, "Error during account '" + uid + "' delete");
        }
    }

    @Override
    public Schema schema() {
        LOG.info("schema::begin");

        SchemaBuilder builder = new SchemaBuilder(CsvConnector.class);

        try (Reader reader = Util.createReader(configuration)) {
            CSVFormat csv = createCsvFormat();
            CSVParser parser = csv.parse(reader);

            Map<String, Integer> headers = parser.getHeaderMap();
            if (headers == null || headers.isEmpty()) {
                throw new ConfigurationException("Schema can't be generated. First line in CSV is missing");
            }

            ObjectClassInfoBuilder objClassBuilder = new ObjectClassInfoBuilder();
            objClassBuilder.addAllAttributeInfo(createAttributeInfo(headers));

            builder.defineObjectClass(objClassBuilder.build());
        } catch (Exception ex) {
            handleGenericException(ex, "Couldn't generate connector schema");
        }

        LOG.info("schema::end");
        return builder.build();
    }

    @Override
    public void test() {
        LOG.info("test::begin");

        LOG.info("Validating configuration.");
        configuration.validate();

        LOG.info("Validating header.");
        try (Reader reader = Util.createReader(configuration)) {
            CSVFormat csv = createCsvFormat();
            CSVParser parser = csv.parse(reader);

            Map<String, Integer> headers = parser.getHeaderMap();
            testHeader(headers);

            LOG.info("Test configuration was successful.");
        } catch (Exception ex) {
            LOG.error("Test configuration was unsuccessful, reason: {0}.", ex.getMessage());
            handleGenericException(ex, "Test configuration was unsuccessful");
        }

        LOG.info("test::end");
    }

    @Override
    public FilterTranslator<String> createFilterTranslator(ObjectClass objectClass, OperationOptions operationOptions) {
        Util.assertAccount(objectClass);

        return new AbstractFilterTranslator<String>() {
        };
    }

    @Override
    public void executeQuery(ObjectClass objectClass, String uid, ResultsHandler handler, OperationOptions options) {
        Util.assertAccount(objectClass);

        CSVFormat csv = createCsvFormat();
        try (Reader reader = Util.createReader(configuration)) {
            CSVParser parser = csv.parse(reader);
            Iterator<CSVRecord> iterator = parser.iterator();
            while (iterator.hasNext()) {
                ConnectorObject obj = createConnectorObject(iterator.next());

                if (StringUtil.isEmpty(uid) || !uid.equals(obj.getUid().getUidValue())) {
                    continue;
                }

                if (!handler.handle(obj)) {
                    break;
                }
            }
        } catch (Exception ex) {
            handleGenericException(ex, "Error during query execution");
        }
    }

    @Override
    public Uid authenticate(ObjectClass objectClass, String s, GuardedString guardedString, OperationOptions options) {
        //todo implement
        return null;
    }

    @Override
    public Uid resolveUsername(ObjectClass objectClass, String s, OperationOptions operationOptions) {
        //todo implement
        return null;
    }

    @Override
    public void sync(ObjectClass objectClass, SyncToken token, SyncResultsHandler handler, OperationOptions options) {
        //todo implement
    }

    @Override
    public SyncToken getLatestSyncToken(ObjectClass objectClass) {
        //todo implement
        return null;
    }

    @Override
    public Uid addAttributeValues(ObjectClass objectClass, Uid uid, Set<Attribute> set, OperationOptions options) {
        //todo implement
        return null;
    }

    @Override
    public Uid removeAttributeValues(ObjectClass objectClass, Uid uid, Set<Attribute> set, OperationOptions options) {
        //todo implement
        return null;
    }

    @Override
    public Uid update(ObjectClass objectClass, Uid uid, Set<Attribute> set, OperationOptions options) {
        //todo implement
        return null;
    }

    private void testHeader(Map<String, Integer> headers) {
        boolean uniqueFound = false;
        boolean passwordFound = false;

        Map<String, Integer> headerCount = new HashMap<>();
        for (String header : headers.keySet()) {
            if (!headerCount.containsKey(header)) {
                headerCount.put(header, 0);
            }

            headerCount.put(header, headerCount.get(header) + 1);
        }

        for (String header : headers.keySet()) {
            int count = headerCount.containsKey(header) ? headerCount.get(header) : 0;

            if (count != 1) {
                throw new ConfigurationException("Column header '" + header
                        + "' occurs more than once (" + count + ").");
            }

            if (header.equals(configuration.getUniqueAttribute())) {
                uniqueFound = true;
                continue;
            }

            if (StringUtil.isNotEmpty(configuration.getPasswordAttribute())
                    && header.equals(configuration.getPasswordAttribute())) {
                passwordFound = true;
            }

            if (uniqueFound && passwordFound) {
                break;
            }
        }

        if (!uniqueFound) {
            throw new ConfigurationException("Header in csv file doesn't contain "
                    + "unique attribute name as defined in configuration.");
        }

        if (StringUtil.isNotEmpty(configuration.getPasswordAttribute()) && !passwordFound) {
            throw new ConfigurationException("Header in csv file doesn't contain "
                    + "password attribute name as defined in configuration.");
        }
    }

    private CSVFormat createCsvFormat() {
        return CSVFormat.valueOf(configuration.getFieldDelimiter())
                .withAllowMissingColumnNames(false)
                .withEscape(Util.toCharacter(configuration.getEscape()))
                .withCommentMarker(Util.toCharacter(configuration.getCommentMarker()))
                .withIgnoreEmptyLines(configuration.isIgnoreEmptyLines())
                .withIgnoreHeaderCase(false)
                .withIgnoreSurroundingSpaces(configuration.isIgnoreSurroundingSpaces())
                .withQuote(Util.toCharacter(configuration.getQuote()))
                .withQuoteMode(QuoteMode.valueOf(configuration.getQuoteMode()))
                .withRecordSeparator(Util.toCharacter(configuration.getRecordSeparator()))
                .withTrailingDelimiter(configuration.isTrailingDelimiter())
                .withTrim(configuration.isTrim())
                .withFirstRecordAsHeader();
    }

    private ConnectorObject createConnectorObject(CSVRecord record) {
        ConnectorObjectBuilder builder = new ConnectorObjectBuilder();

        Map<String, String> map = record.toMap();
        for (String name : map.keySet()) {
            if (StringUtil.isEmpty(map.get(name))) {
                continue;
            }

            String value = map.get(name);
            if (name.equals(configuration.getUniqueAttribute())) {
                builder.setUid(value);

                if (!isUniqueAndNameAttributeEqual()) {
                    continue;
                }
            }

            if (name.equals(configuration.getNameAttribute())) {
                builder.setName(new Name(value));
                continue;
            }

            if (name.equals(configuration.getPasswordAttribute())) {
                builder.addAttribute(OperationalAttributes.PASSWORD_NAME, new GuardedString(value.toCharArray()));
                continue;
            }

            builder.addAttribute(name, createAttributeValues(value));
        }

        return builder.build();
    }

    private List<String> createAttributeValues(String attributeValue) {
        List<String> values = new ArrayList<>();
        values.add(attributeValue);

        return values;
    }

    private boolean isUniqueAndNameAttributeEqual() {
        String uniqueAttribute = configuration.getUniqueAttribute();
        String nameAttribute = configuration.getNameAttribute();

        return uniqueAttribute == null ? nameAttribute == null : uniqueAttribute.equals(nameAttribute);
    }

    private void handleGenericException(Exception ex, String message) {
        if (ex instanceof ConnectorException) {
            throw (ConnectorException) ex;
        }

        if (ex instanceof IOException) {
            throw new ConnectorIOException(message + ", IO exception occurred, reason: " + ex.getMessage(), ex);
        }

        throw new ConnectorException(message + ", reason: " + ex.getMessage(), ex);
    }

    private List<AttributeInfo> createAttributeInfo(Map<String, Integer> names) {
        List<AttributeInfo> infos = new ArrayList<>();
        for (String name : names.keySet()) {
            if (name.equals(configuration.getUniqueAttribute())) {
                continue;
            }
            if (name.equals(configuration.getNameAttribute())) {
                continue;
            }
            if (name.equals(configuration.getPasswordAttribute())) {
                infos.add(OperationalAttributeInfos.PASSWORD);
                continue;
            }

            AttributeInfoBuilder builder = new AttributeInfoBuilder(name);
            if (name.equals(configuration.getPasswordAttribute())) {
                builder.setType(GuardedString.class);
            } else {
                builder.setType(String.class);
            }
            infos.add(builder.build());
        }

        return infos;
    }
}
