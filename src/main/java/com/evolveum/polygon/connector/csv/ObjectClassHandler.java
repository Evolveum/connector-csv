package com.evolveum.polygon.connector.csv;

import com.evolveum.polygon.connector.csv.util.Column;
import com.evolveum.polygon.connector.csv.util.Util;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.exceptions.ConnectorIOException;
import org.identityconnectors.framework.common.objects.*;
import org.identityconnectors.framework.common.objects.filter.FilterTranslator;
import org.identityconnectors.framework.spi.operations.*;

import java.io.IOException;
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

    private Map<String, Column> header;

    public ObjectClassHandler(ObjectClassHandlerConfiguration configuration) {
        this.configuration = configuration;

        init();
    }

    private void init() {
        CSVFormat csv = Util.createCsvFormat(configuration);
        try (Reader reader = Util.createReader(configuration)) {
            CSVParser parser = csv.parse(reader);
            Iterator<CSVRecord> iterator = parser.iterator();

            CSVRecord record = null;
            while (iterator.hasNext()) {
                record = iterator.next();
                if (!isRecordEmpty(record)) {
                    break;
                }
            }

            if (record == null) {
                throw new ConfigurationException("Couldn't initialize headers, nothing in csv file for object class "
                        + configuration.getObjectClass());
            }

            System.out.println(record);

            header = createHeader(record);

        } catch (IOException ex) {
            throw new ConnectorIOException("Couldn't initialize connector for object class "
                    + configuration.getObjectClass(), ex);
        }
    }

    private String getAvailableAttributeName(Map<String, Column> header, String realName) {
        String availableName = realName;
        for (int i = 1; i <= header.size(); i++) {
            if (!header.containsKey(availableName)) {
                break;
            }

            availableName = realName + i;
        }

        return availableName;
    }

    private Map<String, Column> createHeader(CSVRecord record) {
        Map<String, Column> header = new HashMap<>();

        if (configuration.isHeaderExists()) {
            for (int i = 0; i < record.size(); i++) {
                String name = record.get(i);

                if (StringUtil.isEmpty(name)) {
                    name = Util.DEFAULT_COLUMN_NAME + 0;
                }

                String availableName = getAvailableAttributeName(header, name);
                header.put(availableName, new Column(name, i));
            }
        } else {
            // header doesn't exist, we just create col0...colN
            for (int i = 0; i < record.size(); i++) {
                header.put(Util.DEFAULT_COLUMN_NAME + i, new Column(null, i));
            }
        }

        LOG.info("Created header {0}", header);

        testHeader(header);

        return header;
    }

    private void testHeader(Map<String, Column> headers) {
        boolean uniqueFound = false;
        boolean passwordFound = false;

        for (String header : headers.keySet()) {
            if (header.equals(configuration.getUniqueAttribute())) {
                uniqueFound = true;
                continue;
            }

            if (header.equals(configuration.getPasswordAttribute())) {
                passwordFound = true;
                continue;
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

    public ObjectClass getObjectClass() {
        return configuration.getObjectClass();
    }

    public void schema(SchemaBuilder schema) {
        try {

            ObjectClassInfoBuilder objClassBuilder = new ObjectClassInfoBuilder();
            objClassBuilder.addAllAttributeInfo(createAttributeInfo(header));

            schema.defineObjectClass(objClassBuilder.build());
        } catch (Exception ex) {
            handleGenericException(ex, "Couldn't initialize connector");
        }
    }

    private Set<AttributeInfo.Flags> createFlags(AttributeInfo.Flags... flags) {
        Set<AttributeInfo.Flags> set = new HashSet<>();
        set.addAll(Arrays.asList(flags));

        return set;
    }

    private List<AttributeInfo> createAttributeInfo(Map<String, Column> columns) {
        List<AttributeInfo> infos = new ArrayList<>();
        for (String name : columns.keySet()) {
            if (name == null || name.isEmpty()) {
                continue;
            }

            if (name.equals(configuration.getUniqueAttribute())) {
                // unique column
                AttributeInfoBuilder builder = new AttributeInfoBuilder(name);
                builder.setFlags(createFlags(
                        AttributeInfo.Flags.REQUIRED,
                        AttributeInfo.Flags.NOT_RETURNED_BY_DEFAULT,
                        AttributeInfo.Flags.NOT_READABLE));
                builder.setType(String.class);
                builder.setNativeName(name);

                infos.add(builder.build());

                continue;
            }

            if (name.equals(configuration.getNameAttribute())) {
                continue;
            }

            if (name.equals(configuration.getPasswordAttribute())) {
                AttributeInfoBuilder builder = new AttributeInfoBuilder(OperationalAttributes.PASSWORD_NAME);
                builder.setFlags(createFlags(
                        AttributeInfo.Flags.NOT_READABLE,
                        AttributeInfo.Flags.NOT_RETURNED_BY_DEFAULT
                ));
                builder.setType(GuardedString.class);
                builder.setNativeName(name);

                infos.add(builder.build());

                continue;
            }

            AttributeInfoBuilder builder = new AttributeInfoBuilder(name);
            if (name.equals(configuration.getPasswordAttribute())) {
                builder.setType(GuardedString.class);
            } else {
                builder.setType(String.class);
            }
            builder.setNativeName(name);

            infos.add(builder.build());
        }

        return infos;
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
                if (configuration.isHeaderExists() && record.getRecordNumber() == 1) {
                    continue;
                }

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

        for (int i = 0; i < record.size(); i++) {
            String value = record.get(i);
            if (StringUtil.isNotBlank(value)) {
                return false;
            }
        }

        return true;
    }

    private ConnectorObject createConnectorObject(CSVRecord record) {
        ConnectorObjectBuilder builder = new ConnectorObjectBuilder();

        Map<Integer, String> header = new HashMap<>();
        this.header.forEach((key, value) -> {

            header.put(value.getIndex(), key);
        });

        for (int i = 0; i < record.size(); i++) {
            String name = header.get(i);
            String value = record.get(i);

            if (StringUtil.isEmpty(value)) {
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
