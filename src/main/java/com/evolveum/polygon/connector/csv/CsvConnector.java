package com.evolveum.polygon.connector.csv;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.identityconnectors.framework.common.objects.*;
import org.identityconnectors.framework.common.objects.filter.AbstractFilterTranslator;
import org.identityconnectors.framework.common.objects.filter.FilterTranslator;
import org.identityconnectors.framework.spi.Configuration;
import org.identityconnectors.framework.spi.Connector;
import org.identityconnectors.framework.spi.ConnectorClass;
import org.identityconnectors.framework.spi.operations.*;

import java.util.Set;

/**
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
    public Uid create(ObjectClass objectClass, Set<Attribute> set, OperationOptions operationOptions) {
        return null;
    }

    @Override
    public void delete(ObjectClass objectClass, Uid uid, OperationOptions operationOptions) {

    }

    @Override
    public Schema schema() {

//        try (Reader in = Util.createReader(configuration)) {
//            Iterable<CSVRecord> records = CSVFormat.RFC4180.withFirstRowAsHeader().parse(in);
//            for (CSVRecord record : records) {
//                String id = record.get("ID");
//                String customerNo = record.get("CustomerNo");
//                String name = record.get("Name");
//            }
//        } catch (IOException ex) {
//            //todo handle
//            ex.printStackTrace();
//        }

        return null;
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

    @Override
    public void test() {

    }

    @Override
    public FilterTranslator<String> createFilterTranslator(ObjectClass objectClass, OperationOptions operationOptions) {
        Util.assertAccount(objectClass);

        return new AbstractFilterTranslator<String>() {

        };
    }

    @Override
    public void executeQuery(ObjectClass objectClass, String s, ResultsHandler resultsHandler, OperationOptions operationOptions) {

    }

    @Override
    public Uid authenticate(ObjectClass objectClass, String s, GuardedString guardedString, OperationOptions operationOptions) {
        return null;
    }

    @Override
    public Uid resolveUsername(ObjectClass objectClass, String s, OperationOptions operationOptions) {
        return null;
    }

    @Override
    public void sync(ObjectClass objectClass, SyncToken syncToken, SyncResultsHandler syncResultsHandler, OperationOptions operationOptions) {

    }

    @Override
    public SyncToken getLatestSyncToken(ObjectClass objectClass) {
        return null;
    }

    @Override
    public Uid addAttributeValues(ObjectClass objectClass, Uid uid, Set<Attribute> set, OperationOptions operationOptions) {
        return null;
    }

    @Override
    public Uid removeAttributeValues(ObjectClass objectClass, Uid uid, Set<Attribute> set, OperationOptions operationOptions) {
        return null;
    }

    @Override
    public Uid update(ObjectClass objectClass, Uid uid, Set<Attribute> set, OperationOptions operationOptions) {
        return null;
    }
}
