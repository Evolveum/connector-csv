package com.evolveum.polygon.connector.csv;

import com.evolveum.polygon.connector.csv.util.Util;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.objects.*;
import org.identityconnectors.framework.common.objects.filter.FilterTranslator;
import org.identityconnectors.framework.spi.operations.*;

import java.io.Reader;
import java.util.*;

import static com.evolveum.polygon.connector.csv.util.Util.handleGenericException;

/**
 * Created by lazyman on 27/01/2017.
 */
public class ObjectClassHandler implements CreateOp, DeleteOp, TestOp, SchemaOp, SearchOp<String>,
        UpdateAttributeValuesOp, AuthenticateOp, ResolveUsernameOp, SyncOp {

    private enum Operation {

        DELETE, UPDATE, ADD_ATTR_VALUE, REMOVE_ATTR_VALUE;
    }

    private static final Log LOG = Log.getLog(ObjectClassHandler.class);

    private ObjectClassHandlerConfiguration configuration;

    private Map<String, Integer> header;

    public ObjectClassHandler(ObjectClassHandlerConfiguration configuration) {
        this.configuration = configuration;

        init();
    }

    private void init() {
        if (configuration.isHeaderExists()) {

        } else {

        }

        //todo implement, load header map
    }

    public ObjectClass getObjectClass() {
        return configuration.getObjectClass();
    }

    public void schema(SchemaBuilder schema) {
        //todo implement
    }

    @Override
    public Uid authenticate(ObjectClass oc, String username, GuardedString password, OperationOptions oo) {
        return null; //todo implement
    }

    @Override
    public Uid create(ObjectClass oc, Set<Attribute> set, OperationOptions oo) {
        return null; //todo implement
    }

    @Override
    public void delete(ObjectClass oc, Uid uid, OperationOptions oo) {

    }

    @Override
    public Uid resolveUsername(ObjectClass oc, String username, OperationOptions oo) {
        return null; //todo implement
    }

    @Override
    public Schema schema() {
        return null; //todo implement
    }

    @Override
    public FilterTranslator<String> createFilterTranslator(ObjectClass oc, OperationOptions oo) {
        return new CsvFilterTranslator();
    }

    @Override
    public void executeQuery(ObjectClass oc, String uid, ResultsHandler handler, OperationOptions oo) {
        CSVFormat csv = Util.createCsvFormatReader(configuration);
        try (Reader reader = Util.createReader(configuration)) {

            CSVParser parser = csv.parse(reader);
            Iterator<CSVRecord> iterator = parser.iterator();
            while (iterator.hasNext()) {
                CSVRecord record = iterator.next();
                if (isRecordEmpty(record)) {
                    continue;
                }

                ConnectorObject obj = createConnectorObject(record);

                if (uid == null) {
                    if (!handler.handle(obj)) {
                        break;
                    } else {
                        continue;
                    }
                }

                if (!uid.equals(obj.getUid().getUidValue())) {
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
    public void sync(ObjectClass oc, SyncToken token, SyncResultsHandler handler, OperationOptions oo) {
        // todo implement
    }

    @Override
    public SyncToken getLatestSyncToken(ObjectClass oc) {
        return null; //todo implement
    }

    @Override
    public void test() {
        // todo implement
    }

    @Override
    public Uid addAttributeValues(ObjectClass oc, Uid uid, Set<Attribute> set, OperationOptions oo) {
        return null; //todo implement
    }

    @Override
    public Uid removeAttributeValues(ObjectClass oc, Uid uid, Set<Attribute> set, OperationOptions oo) {
        return null; //todo implement
    }

    @Override
    public Uid update(ObjectClass oc, Uid uid, Set<Attribute> set, OperationOptions oo) {
        return null; //todo implement
    }

    private boolean isRecordEmpty(CSVRecord record) {
        if (!configuration.isIgnoreEmptyLines()) {
            return false;
        }

        Collection<String> values = record.toMap().values();
        boolean empty = true;
        for (String value : values) {
            if (StringUtil.isNotBlank(value)) {
                empty = false;
            }
        }

        return empty;
    }

    private ConnectorObject createConnectorObject(CSVRecord record) {
        ConnectorObjectBuilder builder = new ConnectorObjectBuilder();

        Map<String, String> map = record.toMap();
        for (String name : map.keySet()) {
            String value = map.get(name);

            if (StringUtil.isEmpty(value)) {
                continue;
            }

            // TODO: better solution?
            // ignore columns without header name
            if (StringUtil.isEmpty(name)) {
                continue;
            }

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

    private boolean isUniqueAndNameAttributeEqual() {
        String uniqueAttribute = configuration.getUniqueAttribute();
        String nameAttribute = configuration.getNameAttribute();

        return uniqueAttribute == null ? nameAttribute == null : uniqueAttribute.equals(nameAttribute);
    }

    private List<String> createAttributeValues(String attributeValue) {
        List<String> values = new ArrayList<>();

        if (StringUtil.isEmpty(configuration.getMultivalueDelimiter())) {
            values.add(attributeValue);
        } else {
            String[] array = attributeValue.split(configuration.getMultivalueDelimiter());
            for (String item : array) {
                if (StringUtil.isEmpty(item)) {
                    continue;
                }

                values.add(item);
            }
        }

        return values;
    }

}
