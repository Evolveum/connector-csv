package com.evolveum.polygon.connector.csv;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.security.GuardedByteArray;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.ObjectClass;

import java.io.*;
import java.util.List;

/**
 * Created by Viliam Repan (lazyman).
 */
public class Util {

    public static void assertAccount(ObjectClass oc) {
        if (oc == null) {
            throw new IllegalArgumentException("Object class must not be null.");
        }

        if (!ObjectClass.ACCOUNT.is(oc.getObjectClassValue())) {
            throw new ConnectorException("Can't work with resource object different than account.");
        }
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

    public static CSVFormat createCsvFormat(CsvConfiguration configuration) {
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
        if (attribute == null || attribute.getValue() == null || attribute.getValue().isEmpty()) {
            return null;
        }

        List<Object> values = attribute.getValue();
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
}
