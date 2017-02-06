package com.evolveum.polygon.connector.csv.util;

import com.evolveum.polygon.connector.csv.CsvConfiguration;
import com.evolveum.polygon.connector.csv.ObjectClassHandlerConfiguration;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;
import org.identityconnectors.common.Base64;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.security.GuardedByteArray;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.exceptions.ConnectorIOException;
import org.identityconnectors.framework.common.objects.Attribute;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by Viliam Repan (lazyman).
 */
public class Util {

    public static final DateFormat DATE_FORMAT = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss Z");

    public static final String TMP_EXTENSION = "tmp";

    public static final String DEFAULT_COLUMN_NAME = "col";

    public static void closeQuietly(Writer writer, FileLock lock) {
        try {
            if (writer != null) {
                writer.close();
            }
            if (lock != null && lock.isValid()) {
                lock.release();
            }
        } catch (IOException ex) {
        }
    }

    public static FileLock obtainTmpFileLock(ObjectClassHandlerConfiguration config) {
        final long MAX_WAIT = 5 * 1000; // 5 seconds
        File tmp = new File(config.getFilePath().getPath() + "." + Util.TMP_EXTENSION);
        Path path = tmp.toPath();

        long start = System.currentTimeMillis();
        FileChannel channel;
        while (true) {
            try {
                channel = FileChannel.open(path,
                        new HashSet(Arrays.asList(StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)));

                break;
            } catch (IOException ex) {
                if (System.currentTimeMillis() > (start + MAX_WAIT)) {
                    throw new ConnectorIOException("Timeout, couldn't create tmp file '" + tmp.getPath()
                            + "', reason: " + ex.getMessage(), ex);
                }

                try {
                    Thread.sleep((long) (10 + (Math.random() * 50)));
                } catch (InterruptedException ie) {
                    throw new ConnectorException(ie);
                }
            }
        }

        FileLock lock;
        try {
            lock = channel.lock();
        } catch (IOException ex) {
            try {
                channel.close();
            } catch (IOException io) {
            }

            tmp.delete();

            throw new ConnectorIOException("Couldn't obtain lock for temp file '" + tmp.getPath()
                    + "', reason: " + ex.getMessage(), ex);
        }

        return lock;
    }

    public static <T> T getSafeValue(Map<String, Object> map, String key, T defValue, Class<T> type) {
        if (map == null) {
            return defValue;
        }

        Object value = map.get(key);
        if (value == null) {
            return defValue;
        }

        String strValue = value.toString();
        if (String.class.equals(type)) {
            return (T) strValue;
        } else if (Integer.class.equals(type)) {
            return (T) Integer.valueOf(strValue);
        } else if (Boolean.class.equals(type)) {
            return (T) Boolean.valueOf(strValue);
        } else if (File.class.equals(type)) {
            return (T) new File(strValue);
        }

        return defValue;
    }

    public static BufferedReader createReader(ObjectClassHandlerConfiguration configuration) throws IOException {
        return createReader(configuration.getFilePath(), configuration);
    }

    public static BufferedReader createReader(File path, ObjectClassHandlerConfiguration configuration) throws IOException {
        FileInputStream fis = new FileInputStream(path);
        InputStreamReader in = new InputStreamReader(fis, configuration.getEncoding());
        return new BufferedReader(in);
    }

    public static BufferedWriter createWriter(File path, ObjectClassHandlerConfiguration configuration)
            throws IOException {

        FileOutputStream fos = new FileOutputStream(path);
        OutputStreamWriter out = new OutputStreamWriter(fos, configuration.getEncoding());
        return new BufferedWriter(out);
    }

    public static void checkCanReadFile(File file) {
        if (file == null) {
            throw new ConfigurationException("File path is not defined");
        }

        if (!file.exists()) {
            throw new ConfigurationException("File '" + file + "' doesn't exists. At least file with CSV header must exist");
        }
        if (file.isDirectory()) {
            throw new ConfigurationException("File path '" + file + "' is a directory, must be a CSV file");
        }
        if (!file.canRead()) {
            throw new ConfigurationException("File '" + file + "' can't be read");
        }
    }

    public static void handleGenericException(Exception ex, String message) {
        if (ex instanceof IllegalArgumentException) {
            throw (IllegalArgumentException) ex;
        }

        if (ex instanceof ConnectorException) {
            throw (ConnectorException) ex;
        }

        if (ex instanceof IOException) {
            throw new ConnectorIOException(message + ", IO exception occurred, reason: " + ex.getMessage(), ex);
        }

        throw new ConnectorException(message + ", reason: " + ex.getMessage(), ex);
    }

    public static void notNull(Object object, String message) {
        if (object == null) {
            throw new IllegalArgumentException(message);
        }
    }

    public static BufferedReader createReader(CsvConfiguration configuration) throws IOException {
        return createReader(configuration.getFilePath(), configuration);
    }

    public static BufferedReader createReader(File path, CsvConfiguration configuration) throws IOException {
        FileInputStream fis = new FileInputStream(path);
        InputStreamReader in = new InputStreamReader(fis, configuration.getEncoding());
        return new BufferedReader(in);
    }

    public static BufferedWriter createWriter(File path, CsvConfiguration configuration)
            throws IOException {

        FileOutputStream fos = new FileOutputStream(path);
        OutputStreamWriter out = new OutputStreamWriter(fos, configuration.getEncoding());
        return new BufferedWriter(out);
    }

    public static Character toCharacter(String value) {
        if (value == null) {
            return null;
        }

        if (value.length() != 1) {
            throw new ConfigurationException("Can't cast to character, illegal string size: "
                    + value.length() + ", should be 1");
        }

        return value.charAt(0);
    }

    public static void notEmpty(String str, String message) {
        if (StringUtil.isEmpty(str)) {
            throw new ConfigurationException(message);
        }
    }

    public static CSVFormat createCsvFormatWriter(ObjectClassHandlerConfiguration config, Map<String, Column> header) {
        CSVFormat format = createCsvFormat(config);
return null; //todo implement
    }

    public static CSVFormat createCsvFormatReader(ObjectClassHandlerConfiguration configuration) {
        CSVFormat format = createCsvFormat(configuration);
        format = format.withSkipHeaderRecord(configuration.isHeaderExists());

        return format;
    }

    public static CSVFormat createCsvFormat(ObjectClassHandlerConfiguration configuration) {
        notNull(configuration, "CsvConfiguration must not be null");

        return CSVFormat.newFormat(toCharacter(configuration.getFieldDelimiter()))
                .withAllowMissingColumnNames(false)
                .withEscape(toCharacter(configuration.getEscape()))
                .withCommentMarker(toCharacter(configuration.getCommentMarker()))
                .withIgnoreEmptyLines(configuration.isIgnoreEmptyLines())
                .withIgnoreHeaderCase(false)
                .withIgnoreSurroundingSpaces(configuration.isIgnoreSurroundingSpaces())
                .withQuote(toCharacter(configuration.getQuote()))
                .withQuoteMode(QuoteMode.valueOf(configuration.getQuoteMode()))
                .withRecordSeparator(configuration.getRecordSeparator())
                .withTrailingDelimiter(configuration.isTrailingDelimiter())
                .withTrim(configuration.isTrim());
    }

    public static String createRawValue(Attribute attribute, CsvConfiguration configuration) {
        if (attribute == null) {
            return null;
        }

        return createRawValue(attribute.getValue(), configuration);
    }

    public static String createRawValue(List<Object> values, CsvConfiguration configuration) {
        if (values == null || values.isEmpty()) {
            return null;
        }

        if (values.size() > 1 && StringUtil.isEmpty(configuration.getMultivalueDelimiter())) {
            throw new ConnectorException("Multivalue delimiter not defined in connector configuration");
        }

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < values.size(); i++) {
            Object obj = values.get(i);
            if (obj instanceof GuardedString) {
                GuardedString gs = (GuardedString) obj;
                StringAccessor sa = new StringAccessor();
                gs.access(sa);

                sb.append(sa.getValue());
            } else if (obj instanceof GuardedByteArray) {
                GuardedByteArray ga = (GuardedByteArray) obj;
                ByteArrayAccessor ba = new ByteArrayAccessor();
                ga.access(ba);

                String value = org.identityconnectors.common.Base64.encode(ba.getValue());
                sb.append(value);
            } else {
                sb.append(obj);
            }

            if (i + 1 < values.size()) {
                sb.append(configuration.getMultivalueDelimiter());
            }
        }

        return sb.toString();
    }

    public static <T extends Object> List<T> createAttributeValues(String raw, Class<T> type,
                                                                   CsvConfiguration configuration) {
        if (StringUtil.isEmpty(raw)) {
            return new ArrayList<>();
        }

        List<T> result = new ArrayList<>();

        if (StringUtil.isEmpty(configuration.getMultivalueDelimiter())) {
            Object value = createValue(raw, type);
            if (value != null) {
                result.add((T) value);
            }
        } else {
            String[] array = raw.split(configuration.getMultivalueDelimiter());
            for (String item : array) {
                if (StringUtil.isEmpty(item)) {
                    continue;
                }

                T value = (T) createValue(item, type);
                if (value != null) {
                    result.add(value);
                }
            }
        }

        return result;
    }

    private static <T extends Object> Object createValue(String raw, Class<T> type) {
        if (StringUtil.isEmpty(raw)) {
            return null;
        }

        if (GuardedString.class.equals(type)) {
            return new GuardedString(raw.toCharArray());
        } else if (GuardedByteArray.class.equals(type)) {
            byte[] bytes = Base64.decode(raw);
            return new GuardedByteArray(bytes);
        }

        return raw;
    }

    public static List<Object> addValues(List<Object> base, List<Object> toAdd) {
        List<Object> result = new ArrayList<>();
        if (base != null) {
            result.addAll(base);
        }

        for (Object add : toAdd) {
            if (add == null || result.contains(add)) {
                continue;
            }

            result.add(add);
        }

        return result;
    }

    public static List<Object> removeValues(List<Object> base, List<Object> toRemove) {
        List<Object> result = new ArrayList<>();
        if (base != null) {
            result.addAll(base);
        }

        for (Object remove : toRemove) {
            if (remove == null) {
                continue;
            }

            if (result.contains(remove)) {
                result.remove(remove);
            }
        }

        return result;
    }

    public static void copyAndReplace(File from, File to) throws IOException {
        if (to.exists()) {
            to.delete();
        }

        to.createNewFile();

        try (FileInputStream fis = new FileInputStream(from);
             FileOutputStream fos = new FileOutputStream(to)) {

            FileChannel inChannel = fis.getChannel();
            FileChannel outChannel = fos.getChannel();

            inChannel.transferTo(0, inChannel.size(), outChannel);
        }
    }

    public static String printDate(long millis) {
        return DATE_FORMAT.format(new Date(millis));
    }
}
