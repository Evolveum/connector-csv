package com.evolveum.polygon.connector.csv;

import com.evolveum.polygon.connector.csv.util.Column;
import com.evolveum.polygon.connector.csv.util.StringAccessor;
import com.evolveum.polygon.connector.csv.util.Util;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.exceptions.*;
import org.identityconnectors.framework.common.objects.*;
import org.identityconnectors.framework.common.objects.filter.FilterTranslator;
import org.identityconnectors.framework.spi.operations.*;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.FileLock;
import java.nio.file.*;
import java.util.*;

import static com.evolveum.polygon.connector.csv.util.Util.handleGenericException;

/**
 * todo check new FileSystem().newWatchService() to create exclusive tmp file https://docs.oracle.com/javase/tutorial/essential/io/notification.html
 *
 * Created by lazyman on 27/01/2017.
 */
public class ObjectClassHandler implements CreateOp, DeleteOp, TestOp, SearchOp<String>,
        UpdateAttributeValuesOp, AuthenticateOp, ResolveUsernameOp, SyncOp {

    private enum Operation {

        DELETE, UPDATE, ADD_ATTR_VALUE, REMOVE_ATTR_VALUE;
    }

    private static final Log LOG = Log.getLog(ObjectClassHandler.class);

    private ObjectClassHandlerConfiguration configuration;

    private Map<String, Column> header;

    public ObjectClassHandler(ObjectClassHandlerConfiguration configuration) {
        this.configuration = configuration;

        header = initHeader();
    }

    private Map<String, Column> initHeader() {
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

            return createHeader(record);
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
        return resolveUsername(username, password, oo, true);
    }

    @Override
    public Uid create(ObjectClass oc, Set<Attribute> set, OperationOptions oo) {
        Set<Attribute> attributes = normalize(set);

        String uidValue = findUidValue(attributes);
        Uid uid = new Uid(uidValue);

        FileLock lock = Util.obtainTmpFileLock(configuration);
        Reader reader = null;
        Writer writer = null;
        try {
            reader = Util.createReader(configuration);
            writer = new BufferedWriter(Channels.newWriter(lock.channel(), configuration.getEncoding()));

            CSVFormat csv = Util.createCsvFormat(configuration);
            CSVParser parser = csv.parse(reader);

            csv = Util.createCsvFormat(configuration);
            CSVPrinter printer = csv.print(writer);

            Iterator<CSVRecord> iterator = parser.iterator();
            // we don't want to skip header in any case, but if it's there just
            // write it to tmp file as "standard" record. We can't handle first row
            // as header in case there are more columns with the same name.
            if (configuration.isHeaderExists() && iterator.hasNext()) {
                CSVRecord record = iterator.next();
                printer.printRecord(record);
            }

            // handling real records
            while (iterator.hasNext()) {
                CSVRecord record = iterator.next();
                ConnectorObject obj = createConnectorObject(record);

                if (uid.equals(obj.getUid())) {
                    throw new AlreadyExistsException("Account already exists '" + uid.getUidValue() + "'.");
                }

                printer.printRecord(record);
            }

            printer.printRecord(createNewRecord(attributes));

            writer.close();
            reader.close();

            moveTmpToOrig();
        } catch (Exception ex) {
            handleGenericException(ex, "Error during account '" + uid + "' create");
        } finally {
            Util.cleanupResources(writer, reader, lock, configuration);
        }

        return uid;
    }

    private void moveTmpToOrig() throws IOException {
        // moving existing file
        String path = configuration.getFilePath().getPath();
        File orig = new File(path);

        File tmp = Util.createTmpPath(configuration);

        Files.move(tmp.toPath(), orig.toPath(), StandardCopyOption.REPLACE_EXISTING);
    }

    private boolean isPassword(String column) {
        return StringUtil.isNotEmpty(configuration.getPasswordAttribute())
                && configuration.getPasswordAttribute().equals(column);
    }

    private boolean isUid(String column) {
        return configuration.getUniqueAttribute().equals(column);
    }

    private List<Object> createNewRecord(Set<Attribute> attributes) {
        final Object[] record = new Object[header.size()];

        Attribute nameAttr = AttributeUtil.getNameFromAttributes(attributes);
        Object name = nameAttr != null ? AttributeUtil.getSingleValue(nameAttr) : null;

        Attribute uniqueAttr = AttributeUtil.find(configuration.getUniqueAttribute(), attributes);
        Object uid = uniqueAttr != null ? AttributeUtil.getSingleValue(uniqueAttr) : null;

        for (String column : header.keySet()) {
            Object value;
            if (isPassword(column)) {
                Attribute attr = AttributeUtil.find(OperationalAttributes.PASSWORD_NAME, attributes);
                if (attr == null) {
                    continue;
                }

                value = Util.createRawValue(attr, configuration);
            } else if (isName(column)) {
                value = name;
            } else if (isUid(column)) {
                value = uid;
            } else {
                Attribute attr = AttributeUtil.find(column, attributes);
                if (attr == null) {
                    continue;
                }

                value = Util.createRawValue(attr, configuration);
            }

            record[header.get(column).getIndex()] = value;
        }

        return Arrays.asList(record);
    }

    private String findUidValue(Set<Attribute> attributes) {
        Attribute uniqueAttr = AttributeUtil.find(configuration.getUniqueAttribute(), attributes);
        Object uid = uniqueAttr != null ? AttributeUtil.getSingleValue(uniqueAttr) : null;

        if (uid == null) {
            throw new InvalidAttributeValueException("Unique attribute value not defined");
        }

        return uid.toString();
    }

    @Override
    public void delete(ObjectClass oc, Uid uid, OperationOptions oo) {
        update(Operation.DELETE, oc, uid, null, oo);
    }

    @Override
    public Uid resolveUsername(ObjectClass oc, String username, OperationOptions oo) {
        return resolveUsername(username, null, oo, false);
    }

    @Override
    public FilterTranslator<String> createFilterTranslator(ObjectClass oc, OperationOptions oo) {
        return new CsvFilterTranslator();
    }

    private boolean skipRecord(CSVRecord record) {
        if (configuration.isHeaderExists() && record.getRecordNumber() == 1) {
            return true;
        }

        if (isRecordEmpty(record)) {
            return true;
        }

        return false;
    }

    @Override
    public void executeQuery(ObjectClass oc, String uid, ResultsHandler handler, OperationOptions oo) {
        CSVFormat csv = Util.createCsvFormatReader(configuration);
        try (Reader reader = Util.createReader(configuration)) {

            CSVParser parser = csv.parse(reader);
            Iterator<CSVRecord> iterator = parser.iterator();
            while (iterator.hasNext()) {
                CSVRecord record = iterator.next();
                if (skipRecord(record)) {
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

    private void validateAuthenticationInputs(String username, GuardedString password, boolean authenticate) {
        if (StringUtil.isEmpty(username)) {
            throw new InvalidCredentialException("Username must not be empty");
        }

        if (authenticate && StringUtil.isEmpty(configuration.getPasswordAttribute())) {
            throw new ConfigurationException("Password attribute not defined in configuration");
        }

        if (authenticate && password == null) {
            throw new InvalidPasswordException("Password is not defined");
        }
    }

    private Uid resolveUsername(String username, GuardedString password, OperationOptions oo, boolean authenticate) {
        validateAuthenticationInputs(username, password, authenticate);

        CSVFormat csv = Util.createCsvFormatReader(configuration);
        try (Reader reader = Util.createReader(configuration)) {

            ConnectorObject object = null;

            CSVParser parser = csv.parse(reader);
            Iterator<CSVRecord> iterator = parser.iterator();
            while (iterator.hasNext()) {
                CSVRecord record = iterator.next();
                if (skipRecord(record)) {
                    continue;
                }

                ConnectorObject obj = createConnectorObject(record);

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
                authenticate(username, password, object);
            }

            Uid uid = object.getUid();
            if (uid == null) {
                throw new UnknownUidException("Unique attribute doesn't have value for account '" + username + "'");
            }

            return uid;
        } catch (Exception ex) {
            handleGenericException(ex, "Error during authentication");
        }

        return null;
    }

    private void authenticate(String username, GuardedString password, ConnectorObject foundObject) {
        GuardedString objPassword = AttributeUtil.getPasswordValue(foundObject.getAttributes());
        if (objPassword == null) {
            throw new InvalidPasswordException("Password not defined for username '" + username + "'");
        }

        // we don't want to authenticate against empty password
        StringAccessor acc = new StringAccessor();
        objPassword.access(acc);
        if (StringUtil.isEmpty(acc.getValue())) {
            throw new InvalidPasswordException("Password not defined for username '" + username + "'");
        }

        if (!objPassword.equals(password)) {
            throw new InvalidPasswordException("Invalid username and/or password");
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
        configuration.validate();

        initHeader();
    }

    @Override
    public Uid addAttributeValues(ObjectClass oc, Uid uid, Set<Attribute> set, OperationOptions oo) {
        return update(Operation.ADD_ATTR_VALUE, oc, uid, set, oo);
    }

    @Override
    public Uid removeAttributeValues(ObjectClass oc, Uid uid, Set<Attribute> set, OperationOptions oo) {
        return update(Operation.REMOVE_ATTR_VALUE, oc, uid, set, oo);
    }

    @Override
    public Uid update(ObjectClass oc, Uid uid, Set<Attribute> set, OperationOptions oo) {
        return update(Operation.UPDATE, oc, uid, set, oo);
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

    private Uid update(Operation operation, ObjectClass objectClass, Uid uid, Set<Attribute> attributes,
                       OperationOptions oo) {

        Util.notNull(uid, "Uid must not be null");

        if ((Operation.ADD_ATTR_VALUE.equals(operation) || Operation.REMOVE_ATTR_VALUE.equals(operation))
                && attributes.isEmpty()) {
            return uid;
        }

        Map<Integer, String> header = new HashMap<>();
        this.header.forEach((key, value) -> {

            header.put(value.getIndex(), key);
        });

        attributes = normalize(attributes);

        FileLock lock = Util.obtainTmpFileLock(configuration);
        Reader reader = null;
        Writer writer = null;
        try {
            reader = Util.createReader(configuration);
            writer = new BufferedWriter(Channels.newWriter(lock.channel(), configuration.getEncoding()));

            boolean found = false;

            CSVFormat csv = Util.createCsvFormat(configuration);
            CSVParser parser = csv.parse(reader);

            csv = Util.createCsvFormat(configuration);
            CSVPrinter printer = csv.print(writer);

            Iterator<CSVRecord> iterator = parser.iterator();
            while (iterator.hasNext()) {
                CSVRecord record = iterator.next();

                Map<String, String> data = new HashMap<>();
                for (int i = 0; i < record.size(); i++) {
                    data.put(header.get(i), record.get(i));
                }

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

                    int uidIndex = this.header.get(configuration.getUniqueAttribute()).getIndex();
                    Object newUidValue = updated.get(uidIndex);
                    uid = new Uid(newUidValue.toString());

                    printer.printRecord(updated);
                }
            }

            writer.close();
            reader.close();

            if (!found) {
                throw new UnknownUidException("Account '" + uid + "' not found");
            }

            moveTmpToOrig();
        } catch (Exception ex) {
            handleGenericException(ex, "Error during account '" + uid + "' " + operation.name());
        } finally {
            Util.cleanupResources(writer, reader, lock, configuration);
        }

        return uid;
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

        Set<String> columns = header.keySet();
        for (Attribute attribute : result) {
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

    private boolean isName(String column) {
        return configuration.getNameAttribute().equals(column);
    }

    private List<Object> updateObject(Operation operation, Map<String, String> data, Set<Attribute> attributes) {
        Object[] result = new Object[header.size()];

        // prefill actual data
        for (String column : header.keySet()) {
            result[header.get(column).getIndex()] = data.get(column);
        }

        // update data based on attributes parameter
        switch (operation) {
            case UPDATE:
                for (Attribute attribute : attributes) {
                    Integer index;

                    String name = attribute.getName();
                    if (name.equals(Uid.NAME)) {
                        index = header.get(configuration.getUniqueAttribute()).getIndex();
                    } else if (name.equals(Name.NAME)) {
                        index = header.get(configuration.getNameAttribute()).getIndex();
                    } else if (name.equals(OperationalAttributes.PASSWORD_NAME)) {
                        index = header.get(configuration.getPasswordAttribute()).getIndex();
                    } else {
                        index = header.get(name).getIndex();
                    }

                    String value = Util.createRawValue(attribute, configuration);
                    result[index] = value;
                }
                break;
            case ADD_ATTR_VALUE:
            case REMOVE_ATTR_VALUE:
                for (Attribute attribute : attributes) {
                    Class type = String.class;
                    Integer index;

                    String name = attribute.getName();
                    if (name.equals(Uid.NAME)) {
                        index = header.get(configuration.getUniqueAttribute()).getIndex();
                    } else if (name.equals(Name.NAME)) {
                        index = header.get(configuration.getNameAttribute()).getIndex();
                    } else if (name.equals(OperationalAttributes.PASSWORD_NAME)) {
                        index = header.get(configuration.getPasswordAttribute()).getIndex();
                        type = GuardedString.class;
                    } else {
                        index = header.get(name).getIndex();
                    }

                    List<Object> current = Util.createAttributeValues((String) result[index], type, configuration);
                    List<Object> updated = Operation.ADD_ATTR_VALUE.equals(operation) ?
                            Util.addValues(current, attribute.getValue()) :
                            Util.removeValues(current, attribute.getValue());

                    if (isUid(name) && updated.size() != 1) {
                        throw new IllegalArgumentException("Unique attribute '" + name + "' must contain single value");
                    }

                    String value = Util.createRawValue(updated, configuration);
                    result[index] = value;
                }
        }

        return Arrays.asList(result);
    }
}
