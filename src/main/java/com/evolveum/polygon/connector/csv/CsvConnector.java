package com.evolveum.polygon.connector.csv;

import org.apache.commons.csv.*;
import org.identityconnectors.common.IOUtil;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.exceptions.*;
import org.identityconnectors.framework.common.objects.*;
import org.identityconnectors.framework.common.objects.filter.FilterTranslator;
import org.identityconnectors.framework.spi.Configuration;
import org.identityconnectors.framework.spi.Connector;
import org.identityconnectors.framework.spi.ConnectorClass;
import org.identityconnectors.framework.spi.PoolableConnector;
import org.identityconnectors.framework.spi.operations.*;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by Viliam Repan (lazyman).
 */
@ConnectorClass(
        displayNameKey = "UI_CSV_CONNECTOR_NAME",
        configurationClass = CsvConfiguration.class)
public class CsvConnector implements Connector, CreateOp, DeleteOp, TestOp, SchemaOp, SearchOp<String>,
        UpdateAttributeValuesOp, AuthenticateOp, ResolveUsernameOp, SyncOp, PoolableConnector {

    public static final String TMP_EXTENSION = ".tmp";

    private enum Operation {

        DELETE, UPDATE, ADD_ATTR_VALUE, REMOVE_ATTR_VALUE;
    }

    private static final Log LOG = Log.getLog(CsvConnector.class);

    private static final ReentrantReadWriteLock LOCK = new ReentrantReadWriteLock();

    private CsvConfiguration configuration;

    private CsvHeaderDescriptor header;

    @Override
    public void checkAlive() {
    }

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

        try {
            this.header = getHeader();
        } catch (Exception ex) {
            handleGenericException(ex, "Couldn't initialize connector");
        }
    }

    @Override
    public void dispose() {
    }

    @Override
    public Uid create(ObjectClass objectClass, Set<Attribute> attributes, OperationOptions options) {
        LOG.info("Create started");

        Util.assertAccount(objectClass);

        attributes = normalize(attributes);

        String uidValue = findUidValue(attributes);
        Uid uid = new Uid(uidValue);

        LOCK.writeLock().lock();

        File tmp = createTmpFile();

        try (Reader reader = Util.createReader(configuration);
             Writer writer = Util.createWriter(tmp, configuration)) {

            CSVFormat csv = createCsvFormatReader();
            CSVParser parser = csv.parse(reader);

            csv = createCsvFormatWriter();
            CSVPrinter printer = csv.print(writer);

            Iterator<CSVRecord> iterator = parser.iterator();
            while (iterator.hasNext()) {
                CSVRecord record = iterator.next();
                ConnectorObject obj = createConnectorObject(record);

                if (uid.equals(obj.getUid())) {
                    throw new AlreadyExistsException("Account already exists '" + uid.getUidValue() + "'.");
                }

                printer.printRecord(record);
            }

            printer.printRecord(createNewRecord(attributes));

            configuration.getFilePath().delete();
            tmp.renameTo(configuration.getFilePath());
        } catch (Exception ex) {
            handleGenericException(ex, "Error during account '" + uid + "' delete");
        } finally {
            if (!tmp.equals(configuration.getFilePath())) {
                tmp.delete();
            }
            LOCK.writeLock().unlock();
        }

        LOG.info("Create finished");

        return uid;
    }

    @Override
    public void delete(ObjectClass objectClass, Uid uid, OperationOptions options) {
        LOG.info("Delete for {0} with options {1} started", uid, options);
        update(Operation.DELETE, objectClass, uid, null, options);
        LOG.info("Delete for {0} finished", uid);
    }

    @Override
    public Schema schema() {
        LOG.info("Schema started");

        SchemaBuilder builder = new SchemaBuilder(CsvConnector.class);

        try {
            this.header = getHeader();

            ObjectClassInfoBuilder objClassBuilder = new ObjectClassInfoBuilder();
            objClassBuilder.addAllAttributeInfo(createAttributeInfo(header.getColumns()));

            builder.defineObjectClass(objClassBuilder.build());
        } catch (Exception ex) {
            handleGenericException(ex, "Couldn't initialize connector");
        }

        Schema schema = builder.build();
        LOG.info("Schema finished");

        return schema;
    }

    @Override
    public void test() {
        LOG.info("test::begin");

        LOG.info("Validating configuration.");
        configuration.validate();

        LOG.info("Validating header.");
        try {
            getHeader();
        } catch (Exception ex) {
            LOG.error("Test configuration was unsuccessful, reason: {0}.", ex.getMessage());
            handleGenericException(ex, "Test configuration was unsuccessful");
        }

        LOG.info("Test configuration was successful.");

        LOG.info("test::end");
    }

    @Override
    public FilterTranslator<String> createFilterTranslator(ObjectClass objectClass, OperationOptions operationOptions) {
        Util.assertAccount(objectClass);

        return new CsvFilterTranslator();
    }

    @Override
    public void executeQuery(ObjectClass objectClass, String uid, ResultsHandler handler, OperationOptions options) {
        Util.assertAccount(objectClass);

        LOCK.readLock().lock();

        CSVFormat csv = createCsvFormatReader();
        try (Reader reader = Util.createReader(configuration)) {

            CSVParser parser = csv.parse(reader);
            Iterator<CSVRecord> iterator = parser.iterator();
            while (iterator.hasNext()) {
                ConnectorObject obj = createConnectorObject(iterator.next());

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
        } finally {
            LOCK.readLock().unlock();
        }
    }

    @Override
    public Uid authenticate(ObjectClass objectClass, String username, GuardedString password, OperationOptions options) {
        LOG.info("Authenticate for {0} with options {1} started", username, options);
        Uid uid = resolveUsername(objectClass, username, password, options, true);
        LOG.info("Authenticate for {0} finished, uid {1}", username, uid);

        return uid;
    }

    @Override
    public Uid resolveUsername(ObjectClass objectClass, String username, OperationOptions options) {
        LOG.info("Resolve username for {0} with options {1} started", username, options);
        Uid uid = resolveUsername(objectClass, username, null, options, false);
        LOG.info("Resolve username for {0} finished, uid {1}", username, uid);

        return uid;
    }

    @Override
    public void sync(ObjectClass objectClass, SyncToken token, SyncResultsHandler handler, OperationOptions options) {
        LOG.info("Sync started");
        Util.assertAccount(objectClass);

        //todo implement

        LOG.info("Sync finished");
    }

    @Override
    public SyncToken getLatestSyncToken(ObjectClass objectClass) {
        LOG.info("Get latest sync token started");
        Util.assertAccount(objectClass);

        //todo implement

        LOG.info("Get latest sync token finished");

        return null;
    }

    @Override
    public Uid addAttributeValues(ObjectClass objectClass, Uid uid, Set<Attribute> set, OperationOptions options) {
        LOG.info("Add attribute values for {0} with options {1} started", uid, options);
        uid = update(Operation.ADD_ATTR_VALUE, objectClass, uid, set, options);
        LOG.info("Add attribute values for {0} finished", uid);

        return uid;
    }

    @Override
    public Uid removeAttributeValues(ObjectClass objectClass, Uid uid, Set<Attribute> set, OperationOptions options) {
        LOG.info("Remove attribute values for {0} with options {1} started", uid, options);
        uid = update(Operation.REMOVE_ATTR_VALUE, objectClass, uid, set, options);
        LOG.info("Remove attribute values for {0} finished", uid);

        return uid;
    }

    @Override
    public Uid update(ObjectClass objectClass, Uid uid, Set<Attribute> set, OperationOptions options) {
        LOG.info("Update for {0} with options {1} started", uid, options);
        uid = update(Operation.UPDATE, objectClass, uid, set, options);
        LOG.info("Update for {0} finished", uid);

        return uid;
    }

    private Uid resolveUsername(ObjectClass objectClass, String username, GuardedString password,
                                OperationOptions options, boolean authenticate) {
        Util.assertAccount(objectClass);

        if (StringUtil.isEmpty(username)) {
            throw new InvalidCredentialException("Username must not be empty");
        }

        if (authenticate && StringUtil.isEmpty(configuration.getPasswordAttribute())) {
            throw new ConfigurationException("Password attribute not defined in configuration");
        }

        if (authenticate && password == null) {
            throw new InvalidPasswordException("Password is not defined");
        }

        LOCK.readLock().lock();

        CSVFormat csv = createCsvFormatReader();
        try (Reader reader = Util.createReader(configuration)) {

            ConnectorObject object = null;

            CSVParser parser = csv.parse(reader);
            Iterator<CSVRecord> iterator = parser.iterator();
            while (iterator.hasNext()) {
                ConnectorObject obj = createConnectorObject(iterator.next());

                Name name = obj.getName();
                if (name != null && username.equals(AttributeUtil.getStringValue(name))) {
                    object = obj;
                    break;
                }
            }

            if (object == null) {
                String message = authenticate ? "Invalid username and/or password" : "Invalid username";
                throw new InvalidCredentialException(message);
            }

            if (authenticate) {
                GuardedString objPassword = AttributeUtil.getPasswordValue(object.getAttributes());
                if (objPassword == null) {
                    throw new InvalidPasswordException("Password not defined for username '" + username + "'");
                }

                // we don't want to authenticate against empty password
                SimpleAccessor acc = new SimpleAccessor();
                objPassword.access(acc);
                if (StringUtil.isEmpty(acc.getPassword())) {
                    throw new InvalidPasswordException("Password not defined for username '" + username + "'");
                }

                if (!objPassword.equals(password)) {
                    throw new InvalidPasswordException("Invalid username and/or password");
                }
            }

            Uid uid = object.getUid();
            if (uid == null) {
                throw new UnknownUidException("Unique attribute doesn't have value for account '" + username + "'");
            }

            return uid;
        } catch (Exception ex) {
            handleGenericException(ex, "Error during authentication");
        } finally {
            LOCK.readLock().unlock();
        }

        return null;
    }

    private Uid update(Operation operation, ObjectClass objectClass, Uid uid, Set<Attribute> attributes,
                       OperationOptions oo) {

        Util.assertAccount(objectClass);
        Util.notNull(uid, "Uid must not be null");

        if ((Operation.ADD_ATTR_VALUE.equals(operation) || Operation.REMOVE_ATTR_VALUE.equals(operation))
                && attributes.isEmpty()) {
            return uid;
        }

        attributes = normalize(attributes);

        LOCK.writeLock().lock();

        File tmp = createTmpFile();

        try (Reader reader = Util.createReader(configuration);
             Writer writer = Util.createWriter(tmp, configuration)) {

            boolean found = false;

            CSVFormat csv = createCsvFormatReader();
            CSVParser parser = csv.parse(reader);

            csv = createCsvFormatWriter();
            CSVPrinter printer = csv.print(writer);

            Iterator<CSVRecord> iterator = parser.iterator();
            while (iterator.hasNext()) {
                CSVRecord record = iterator.next();
                Map<String, String> data = record.toMap();

                String uidValue = data.get(configuration.getUniqueAttribute());
                if (StringUtil.isEmpty(uidValue)) {
                    continue;
                }

                Uid recordUid = new Uid(uidValue);
                if (!uid.equals(recordUid)) {
                    printer.printRecord(record);
                    continue;
                }

                found = true;

                if (!Operation.DELETE.equals(operation)) {
                    List<Object> updated = updateObject(operation, data, attributes);

                    int uidIndex = header.getColumns().get(configuration.getUniqueAttribute());
                    Object newUidValue = updated.get(uidIndex);
                    uid = new Uid(newUidValue.toString());

                    printer.printRecord(updated);
                }
            }

            configuration.getFilePath().delete();
            tmp.renameTo(configuration.getFilePath());

            if (!found) {
                throw new UnknownUidException("Account '" + uid + "' not found");
            }
        } catch (Exception ex) {
            handleGenericException(ex, "Error during account '" + uid + "' " + operation.name());
        } finally {
            if (!tmp.equals(configuration.getFilePath())) {
                tmp.delete();
            }
            LOCK.writeLock().unlock();
        }

        return uid;
    }

    private List<Object> updateObject(Operation operation, Map<String, String> data, Set<Attribute> attributes) {
        Object[] result = new Object[header.getColumnSet().size()];

        // prefill actual data
        Map<String, Integer> columns = header.getColumns();
        for (String column : columns.keySet()) {
            result[columns.get(column)] = data.get(column);
        }

        // update data based on attributes parameter
        switch (operation) {
            case UPDATE:
                for (Attribute attribute : attributes) {
                    //todo handle multivalue attributes
                    String name = attribute.getName();
                    if (name.equals(Uid.NAME)) {
                        result[columns.get(configuration.getUniqueAttribute())] = AttributeUtil.getSingleValue(attribute);
                    } else if (name.equals(Name.NAME)) {
                        result[columns.get(configuration.getNameAttribute())] = AttributeUtil.getSingleValue(attribute);
                    } else if (name.equals(OperationalAttributes.PASSWORD_NAME)) {
                        GuardedString gs = AttributeUtil.getGuardedStringValue(attribute);
                        if (gs == null) {
                            result[columns.get(configuration.getPasswordAttribute())] = null;
                        } else {
                            SimpleAccessor sa = new SimpleAccessor();
                            gs.access(sa);
                            result[columns.get(configuration.getPasswordAttribute())] = sa.getPassword();
                        }
                    } else {
                        result[columns.get(name)] = AttributeUtil.getSingleValue(attribute);
                    }
                }
                break;
            case ADD_ATTR_VALUE:

                break;
            case REMOVE_ATTR_VALUE:

                break;
        }

        return Arrays.asList(result);
    }

    private Set<Attribute> updateObject(Operation operation, ConnectorObject obj, Set<Attribute> attributes) {
        Set<Attribute> result = new HashSet<>();
        switch (operation) {
            case UPDATE:
                result.addAll(obj.getAttributes());
                for (Attribute attr : attributes) {
                    Attribute resultAttr = AttributeUtil.find(attr.getName(), result);
                    if (resultAttr == null){
                        if (attr.getValue() == null || attr.getValue().isEmpty()) {
                            continue;
                        } else {
                            result.add(attr);
                        }
                    } else {
                        result.remove(resultAttr);
                        if (attr.getValue() != null && !attr.getValue().isEmpty()) {
                            result.add(attr);
                        }
                    }
                }
                break;
            case ADD_ATTR_VALUE:
                result.addAll(obj.getAttributes());

                //todo probably handle UID and NAME, PASSWORD differently
                for (Attribute attr : attributes) {
                    Attribute resultAttr = AttributeUtil.find(attr.getName(), result);
                    if (resultAttr == null) {
                        result.add(attr);
                    } else {
                        List<Object> values = resultAttr.getValue();
                        for (Object newValue : attr.getValue()) {
                            if (Operation.ADD_ATTR_VALUE.equals(operation) && !values.contains(newValue)) {
                                values.add(newValue);
                            }
                        }
                    }
                }
                break;
            case REMOVE_ATTR_VALUE:
                result.addAll(obj.getAttributes());

                //todo probably handle UID and NAME, PASSWORD differently
                for (Attribute attr : attributes) {
                    Attribute resultAttr = AttributeUtil.find(attr.getName(), result);
                    if (resultAttr != null) {
                        List<Object> values = resultAttr.getValue();
                        for (Object newValue : attr.getValue()) {
                            if (Operation.REMOVE_ATTR_VALUE.equals(operation) && values.contains(newValue)) {
                                values.remove(newValue);
                            }
                        }
                    }
                }
                break;
        }

        return result;
    }

    private void testHeader(Map<CsvHeader, Integer> headers) {
        boolean uniqueFound = false;
        boolean passwordFound = false;

        for (CsvHeader header : headers.keySet()) {
            if (header.getColumn().equals(configuration.getUniqueAttribute())) {
                uniqueFound = true;
                continue;
            }

            if (header.getColumn().equals(configuration.getPasswordAttribute())) {
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

    private CSVFormat createCsvFormatReader() {
        return Util.createCsvFormat(configuration).withFirstRecordAsHeader();
    }

    private CSVFormat createCsvFormatWriter() {
        Map<String, Integer> columns = header.getColumns();

        String[] names = new String[columns.size()];
        for (String column : columns.keySet()) {
            names[columns.get(column)] = column;
        }

        return Util.createCsvFormat(configuration).withHeader(names);
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

    private boolean isUid(String column) {
        return configuration.getUniqueAttribute().equals(column);
    }

    private boolean isName(String column) {
        return configuration.getNameAttribute().equals(column);
    }

    private boolean isPassword(String column) {
        return StringUtil.isNotEmpty(configuration.getPasswordAttribute())
                && configuration.getPasswordAttribute().equals(column);
    }

    private Set<Attribute> normalize(Set<Attribute> attributes) {
        if (attributes == null) {
            return null;
        }

        Set<Attribute> result = new HashSet<>();
        result.addAll(attributes);

        Attribute nameAttr = AttributeUtil.getNameFromAttributes(result);
        Object name = nameAttr != null ? AttributeUtil.getSingleValue(nameAttr) : null;

        Attribute uniqueAttr = AttributeUtil.find(configuration.getUniqueAttribute(), result);
        Object uid = uniqueAttr != null ? AttributeUtil.getSingleValue(uniqueAttr) : null;

        if (isUniqueAndNameAttributeEqual()) {
            if (name == null && uid != null) {
                if (nameAttr == null) {
                    nameAttr = AttributeBuilder.build(Name.NAME, uid);
                    result.add(nameAttr);
                }
            } else if (uid == null && name != null) {
                if (uniqueAttr == null) {
                    uniqueAttr = AttributeBuilder.build(configuration.getUniqueAttribute(), name);
                    result.add(uniqueAttr);
                }
            } else if (uid != null && name != null) {
                if (!name.equals(uid)) {
                    throw new InvalidAttributeValueException("Unique attribute value doesn't match name attribute value");
                }
            }
        }

        Set<String> columns = header.getColumnSet();
        for (Attribute attribute: result) {
            String attrName = attribute.getName();
            if (Uid.NAME.equals(attrName) || Name.NAME.equals(attrName)
                    || OperationalAttributes.PASSWORD_NAME.equals(attrName)) {
                continue;
            }

            if (!columns.contains(attrName)) {
                throw new ConnectorException("Unknown attribute " + attrName);
            }

            if (!isUniqueAndNameAttributeEqual() && isName(attrName)) {
                throw new ConnectorException("Column used as " + Name.NAME + " attribute");
            }
        }

        return result;
    }

    private String findUidValue(Set<Attribute> attributes) {
        Attribute uniqueAttr = AttributeUtil.find(configuration.getUniqueAttribute(), attributes);
        Object uid = uniqueAttr != null ? AttributeUtil.getSingleValue(uniqueAttr) : null;

        if (uid == null) {
            throw new InvalidAttributeValueException("Unique attribute value not defined");
        }

        return uid.toString();
    }

    private List<Object> createNewRecord(Set<Attribute> attributes) {
        final Object[] record = new Object[header.getColumnSet().size()];

        Attribute nameAttr = AttributeUtil.getNameFromAttributes(attributes);
        Object name = nameAttr != null ? AttributeUtil.getSingleValue(nameAttr) : null;

        Attribute uniqueAttr = AttributeUtil.find(configuration.getUniqueAttribute(), attributes);
        Object uid = uniqueAttr != null ? AttributeUtil.getSingleValue(uniqueAttr) : null;

        Map<String, Integer> columns = header.getColumns();
        for (String column : columns.keySet()) {
            Object value;
            if (isPassword(column)) {
                Attribute attr = AttributeUtil.find(OperationalAttributes.PASSWORD_NAME, attributes);
                if (attr == null) {
                    continue;
                }

                GuardedString gs = AttributeUtil.getGuardedStringValue(attr);
                SimpleAccessor accessor = new SimpleAccessor();
                gs.access(accessor);

                value = accessor.getPassword();
            } else if (isName(column)) {
                value = name;
            } else if (isUid(column)) {
                value = uid;
            } else {
                Attribute attr = AttributeUtil.find(column, attributes);
                if (attr == null) {
                    continue;
                }

                value = AttributeUtil.getSingleValue(attr);
            }

            record[columns.get(column)] = value;
        }

        return Arrays.asList(record);
    }

    private List<Object> createRecord(Set<Attribute> attributes) {
        final Object[] record = new Object[header.getColumnSet().size()];

        Set<String> supportedAttributes = header.getAttributeSet();
        for (Attribute attr : attributes) {
            if (!supportedAttributes.contains(attr.getName())) {
                throw new ConnectorException("Unsupported attribute '" + attr.getName() + "'");
            }
        }

        Map<String, Integer> attrMap = header.getAttributes();
        for (String name : attrMap.keySet()) {
            final int index = attrMap.get(name);

            Attribute attribute = getAttribute(name, attributes);
            if (attribute == null || attribute.getValue() == null || attribute.getValue().isEmpty()) {
                record[index] = null;
            } else {
                List values = attribute.getValue();
                if (values.size() > 1) {
                    throw new ConnectorException("Multiple values not supported");
                } else {
                    Object object = values.get(0);
                    if (object instanceof GuardedString) {
                        GuardedString pwd = (GuardedString) object;
                        pwd.access(new GuardedString.Accessor() {

                            @Override
                            public void access(char[] chars) {
                                record[index] = new String(chars);
                            }
                        });
                    } else {
                        record[index] = object;
                    }
                }
            }
        }

        return Arrays.asList(record);
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

    private Set<AttributeInfo.Flags> createFlags(AttributeInfo.Flags... flags) {
        Set<AttributeInfo.Flags> set = new HashSet<>();
        set.addAll(Arrays.asList(flags));

        return set;
    }

    private List<AttributeInfo> createAttributeInfo(Map<String, Integer> columns) {
        List<AttributeInfo> infos = new ArrayList<>();
        for (String name : columns.keySet()) {
            if (name.equals(configuration.getUniqueAttribute())) {
                // unique column
                AttributeInfoBuilder builder = new AttributeInfoBuilder(name);
                builder.setFlags(createFlags(
                        AttributeInfo.Flags.REQUIRED,
                        AttributeInfo.Flags.NOT_RETURNED_BY_DEFAULT));
                builder.setType(String.class);

                infos.add(builder.build());

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

    private File createTmpFile() {
        return new File(configuration.getFilePath().getPath() + "." + System.currentTimeMillis() + TMP_EXTENSION);
    }

    private Attribute getAttribute(String name, Set<Attribute> attributes) {
        if (name.equals(configuration.getPasswordAttribute())) {
            name = OperationalAttributes.PASSWORD_NAME;
        }
        if (name.equals(configuration.getNameAttribute())) {
            name = Name.NAME;
        }

        for (Attribute attribute : attributes) {
            if (attribute.getName().equals(name)) {
                return attribute;
            }
        }

        return null;
    }

    private CsvHeaderDescriptor getHeader() throws IOException {
        LOCK.readLock().lock();

        try (Reader reader = Util.createReader(configuration)) {

            CSVFormat csv = createCsvFormatReader();
            CSVParser parser = csv.parse(reader);

            Map<String, Integer> headers = parser.getHeaderMap();
            if (headers.isEmpty()) {
                throw new ConfigurationException("Csv file doesn't contain header");
            }

            Map<CsvHeader, Integer> result = new HashMap<>();
            for (String name : headers.keySet()) {
                String attribute = name;
                if (name.equals(configuration.getPasswordAttribute())) {
                    attribute = OperationalAttributes.PASSWORD_NAME;
                } else if (name.equals(configuration.getNameAttribute())) {
                    attribute = Name.NAME;

                    if (isUniqueAndNameAttributeEqual()) {
                        result.put(new CsvHeader(name, Uid.NAME), headers.get(name));
                        result.put(new CsvHeader(name, name), headers.get(name));
                    }
                } else if (name.equals(configuration.getUniqueAttribute())) {
                    attribute = Uid.NAME;

                    if (isUniqueAndNameAttributeEqual()) {
                        result.put(new CsvHeader(name, Name.NAME), headers.get(name));
                        result.put(new CsvHeader(name, name), headers.get(name));
                    }
                }

                result.put(new CsvHeader(name, attribute), headers.get(name));
            }

            testHeader(result);

            return new CsvHeaderDescriptor(result);
        } catch (IllegalArgumentException ex) {
            throw new ConfigurationException(ex);
        } finally {
            LOCK.readLock().unlock();
        }
    }
}
