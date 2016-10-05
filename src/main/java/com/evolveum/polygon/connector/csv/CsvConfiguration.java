package com.evolveum.polygon.connector.csv;

import org.apache.commons.csv.QuoteMode;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.identityconnectors.framework.spi.AbstractConfiguration;
import org.identityconnectors.framework.spi.ConfigurationProperty;

import java.io.File;
import java.nio.charset.Charset;

/**
 * Created by Viliam Repan (lazyman).
 */
public class CsvConfiguration extends AbstractConfiguration {

    private static final Log LOG = Log.getLog(CsvConfiguration.class);

    private File filePath = null;

    private String encoding = "utf-8";

    private String fieldDelimiter = ";";
    private String escape = "\\";
    private String commentMarker = "#";
    private boolean ignoreEmptyLines = true;
    private String quote = "\"";
    private String quoteMode = QuoteMode.MINIMAL.name();
    private String recordSeparator = "\r\n";
    private boolean ignoreSurroundingSpaces = false;
    private boolean trailingDelimiter = false;
    private boolean trim = false;

    private String uniqueAttribute = null;
    private String nameAttribute = null;
    private String passwordAttribute = null;

    private String multivalueDelimiter = null;

    private int preserverOldSyncFiles = 10;

    @ConfigurationProperty(
            displayMessageKey = "UI_PRESERVE_OLD_SYNC_FILES",
            helpMessageKey = "UI_PRESERVE_OLD_SYNC_FILES_HELP")
    public int getPreserverOldSyncFiles() {
        return preserverOldSyncFiles;
    }

    @ConfigurationProperty(
            displayMessageKey = "UI_CSV_UNIQUE_ATTRIBUTE",
            helpMessageKey = "UI_CSV_UNIQUE_ATTRIBUTE_HELP", required = true)
    public String getUniqueAttribute() {
        return uniqueAttribute;
    }

    @ConfigurationProperty(
            displayMessageKey = "UI_CSV_NAME_ATTRIBUTE",
            helpMessageKey = "UI_CSV_NAME_ATTRIBUTE_HELP", required = true)
    public String getNameAttribute() {
        return nameAttribute;
    }

    @ConfigurationProperty(
            displayMessageKey = "UI_CSV_FILE_PATH",
            helpMessageKey = "UI_CSV_FILE_PATH_HELP", required = true)
    public File getFilePath() {
        return filePath;
    }

    @ConfigurationProperty(
            displayMessageKey = "UI_CSV_PASSWORD_ATTRIBUTE",
            helpMessageKey = "UI_CSV_PASSWORD_ATTRIBUTE_HELP")
    public String getPasswordAttribute() {
        return passwordAttribute;
    }

    @ConfigurationProperty(
            displayMessageKey = "UI_CSV_ENCODING",
            helpMessageKey = "UI_CSV_ENCODING_HELP")
    public String getEncoding() {
        return encoding;
    }

    @ConfigurationProperty(
            displayMessageKey = "UI_CSV_FIELD_DELIMITER",
            helpMessageKey = "UI_CSV_FIELD_DELIMITER_HELP")
    public String getFieldDelimiter() {
        return fieldDelimiter;
    }

    @ConfigurationProperty(
            displayMessageKey = "UI_CSV_ESCAPE",
            helpMessageKey = "UI_CSV_ESCAPE_HELP")
    public String getEscape() {
        return escape;
    }

    @ConfigurationProperty(
            displayMessageKey = "UI_CSV_COMMENT_MARKER",
            helpMessageKey = "UI_CSV_COMMENT_MARKER_HELP")
    public String getCommentMarker() {
        return commentMarker;
    }

    @ConfigurationProperty(
            displayMessageKey = "UI_CSV_IGNORE_EMPTY_LINES",
            helpMessageKey = "UI_CSV_IGNORE_EMPTY_LINES_HELP")
    public boolean isIgnoreEmptyLines() {
        return ignoreEmptyLines;
    }

    @ConfigurationProperty(
            displayMessageKey = "UI_CSV_QUOTE",
            helpMessageKey = "UI_CSV_QUOTE_HELP")
    public String getQuote() {
        return quote;
    }

    @ConfigurationProperty(
            displayMessageKey = "UI_CSV_QUOTE_MODE",
            helpMessageKey = "UI_CSV_QUOTE_MODE_HELP")
    public String getQuoteMode() {
        return quoteMode;
    }

    @ConfigurationProperty(
            displayMessageKey = "UI_CSV_RECORD_SEPARATOR",
            helpMessageKey = "UI_CSV_RECORD_SEPARATOR_HELP")
    public String getRecordSeparator() {
        return recordSeparator;
    }

    @ConfigurationProperty(
            displayMessageKey = "UI_CSV_IGNORE_SURROUNDING_SPACES",
            helpMessageKey = "UI_CSV_IGNORE_SURROUDING_SPACES_HELP")
    public boolean isIgnoreSurroundingSpaces() {
        return ignoreSurroundingSpaces;
    }

    @ConfigurationProperty(
            displayMessageKey = "UI_CSV_TRAILING_DELIMITER",
            helpMessageKey = "UI_CSV_TRAILING_DELIMITER_HELP")
    public boolean isTrailingDelimiter() {
        return trailingDelimiter;
    }

    @ConfigurationProperty(
            displayMessageKey = "UI_CSV_TRIM",
            helpMessageKey = "UI_CSV_TRIM_HELP")
    public boolean isTrim() {
        return trim;
    }

    @ConfigurationProperty(
            displayMessageKey = "UI_CSV_MULTI_VALUE_DELIMITER",
            helpMessageKey = "UI_CSV_MULTI_VALUE_DELIMITER_HELP")
    public String getMultivalueDelimiter() {
        return multivalueDelimiter;
    }

    public void setMultivalueDelimiter(String multivalueDelimiter) {
        this.multivalueDelimiter = multivalueDelimiter;
    }

    public void setFilePath(File filePath) {
        this.filePath = filePath;
    }

    public void setPasswordAttribute(String passwordAttribute) {
        this.passwordAttribute = passwordAttribute;
    }

    public void setPreserverOldSyncFiles(int preserverOldSyncFiles) {
        this.preserverOldSyncFiles = preserverOldSyncFiles;
    }

    public void setUniqueAttribute(String uniqueAttribute) {
        this.uniqueAttribute = uniqueAttribute;
        if (StringUtil.isEmpty(nameAttribute)) {
            nameAttribute = uniqueAttribute;
        }
    }

    public void setNameAttribute(String nameAttribute) {
        this.nameAttribute = StringUtil.isEmpty(nameAttribute) ? this.uniqueAttribute : nameAttribute;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public void setFieldDelimiter(String fieldDelimiter) {
        this.fieldDelimiter = fieldDelimiter;
    }

    public void setEscape(String escape) {
        this.escape = escape;
    }

    public void setCommentMarker(String commentMarker) {
        this.commentMarker = commentMarker;
    }

    public void setIgnoreEmptyLines(boolean ignoreEmptyLines) {
        this.ignoreEmptyLines = ignoreEmptyLines;
    }

    public void setQuote(String quote) {
        this.quote = quote;
    }

    public void setQuoteMode(String quoteMode) {
        this.quoteMode = quoteMode;
    }

    public void setRecordSeparator(String recordSeparator) {
        this.recordSeparator = recordSeparator;
    }

    public void setIgnoreSurroundingSpaces(boolean ignoreSurroundingSpaces) {
        this.ignoreSurroundingSpaces = ignoreSurroundingSpaces;
    }

    public void setTrailingDelimiter(boolean trailingDelimiter) {
        this.trailingDelimiter = trailingDelimiter;
    }

    public void setTrim(boolean trim) {
        this.trim = trim;
    }

    @Override
    public void validate() {
        LOG.info("Csv configuration validation started");

        if (filePath == null) {
            throw new ConfigurationException("File path is not defined");
        }

        if (!filePath.exists()) {
            throw new ConfigurationException("File '" + filePath + "' doesn't exists. At least file with CSV header must exist");
        }
        if (filePath.isDirectory()) {
            throw new ConfigurationException("File path '" + filePath + "' is a directory, must be a CSV file");
        }
        if (!filePath.canRead()) {
            throw new ConfigurationException("File '" + filePath + "' can't be read");
        }
        if (!filePath.canWrite()) {
            throw new ConfigurationException("Can't write to file '" + filePath.getAbsolutePath() + "'");
        }

        Util.notEmpty(encoding, "Encoding is not defined.");

        if (!Charset.isSupported(encoding)) {
            throw new ConfigurationException("Encoding '" + encoding + "' is not supported");
        }

        Util.notEmpty(fieldDelimiter, "Field delimiter can't be null or empty");
        Util.notEmpty(escape, "Escape character is not defined");
        Util.notEmpty(commentMarker, "Comment marker character is not defined");
        Util.notEmpty(quote, "Quote character is not defined");

        Util.notEmpty(quoteMode, "Quote mode is not defined");
        boolean found = false;
        for (QuoteMode qm : QuoteMode.values()) {
            if (qm.name().equalsIgnoreCase(quoteMode)) {
                found = true;
                break;
            }
        }
        if (!found) {
            StringBuilder sb = new StringBuilder();
            for (QuoteMode qm : QuoteMode.values()) {
                sb.append(qm.name()).append(",");
            }
            sb.deleteCharAt(sb.length() - 1);

            throw new ConfigurationException("Quote mode '" + quoteMode + "' is not supported, supported values: ["
                    + sb.toString() + "]");
        }

        Util.notEmpty(recordSeparator, "Record separator is not defined");

        if (StringUtil.isEmpty(uniqueAttribute)) {
            throw new ConfigurationException("Unique attribute is not defined.");
        }

        if (StringUtil.isEmpty(nameAttribute)) {
            LOG.warn("Name attribute not defined, value from unique attribute will be used (" + uniqueAttribute + ").");
            nameAttribute = uniqueAttribute;
        }

        if (StringUtil.isEmpty(passwordAttribute)) {
            LOG.warn("Password attribute is not defined.");
        }

        LOG.info("Csv configuration validation finished");
    }
}
