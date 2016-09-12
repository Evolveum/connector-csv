package com.evolveum.polygon.connector.csv;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.ObjectClass;

import java.io.*;

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

    public static void setValue(Attribute attr, Object value) {
        notNull(attr, "Attribute must not be null");

        attr.getValue().clear();

        if (value != null) {
            attr.getValue().add(value);
        }
    }
}
