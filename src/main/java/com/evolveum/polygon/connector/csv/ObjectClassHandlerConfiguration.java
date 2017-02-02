package com.evolveum.polygon.connector.csv;

import com.evolveum.polygon.connector.csv.util.Util;
import org.apache.commons.csv.QuoteMode;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.identityconnectors.framework.common.objects.ObjectClass;

import java.io.File;
import java.nio.charset.Charset;

/**
 * Created by lazyman on 29/01/2017.
 */
public class ObjectClassHandlerConfiguration {

    private static final Log LOG = Log.getLog(ObjectClassHandlerConfiguration.class);

    private ObjectClass objectClass = ObjectClass.ACCOUNT;

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

    private int preserveOldSyncFiles = 10;

    public ObjectClass getObjectClass() {
        return objectClass;
    }

    public void setObjectClass(ObjectClass objectClass) {
        this.objectClass = objectClass;
    }

    public File getFilePath() {
        return filePath;
    }

    public void setFilePath(File filePath) {
        this.filePath = filePath;
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public String getFieldDelimiter() {
        return fieldDelimiter;
    }

    public void setFieldDelimiter(String fieldDelimiter) {
        this.fieldDelimiter = fieldDelimiter;
    }

    public String getEscape() {
        return escape;
    }

    public void setEscape(String escape) {
        this.escape = escape;
    }

    public String getCommentMarker() {
        return commentMarker;
    }

    public void setCommentMarker(String commentMarker) {
        this.commentMarker = commentMarker;
    }

    public boolean isIgnoreEmptyLines() {
        return ignoreEmptyLines;
    }

    public void setIgnoreEmptyLines(boolean ignoreEmptyLines) {
        this.ignoreEmptyLines = ignoreEmptyLines;
    }

    public String getQuote() {
        return quote;
    }

    public void setQuote(String quote) {
        this.quote = quote;
    }

    public String getQuoteMode() {
        return quoteMode;
    }

    public void setQuoteMode(String quoteMode) {
        this.quoteMode = quoteMode;
    }

    public String getRecordSeparator() {
        return recordSeparator;
    }

    public void setRecordSeparator(String recordSeparator) {
        this.recordSeparator = recordSeparator;
    }

    public boolean isIgnoreSurroundingSpaces() {
        return ignoreSurroundingSpaces;
    }

    public void setIgnoreSurroundingSpaces(boolean ignoreSurroundingSpaces) {
        this.ignoreSurroundingSpaces = ignoreSurroundingSpaces;
    }

    public boolean isTrailingDelimiter() {
        return trailingDelimiter;
    }

    public void setTrailingDelimiter(boolean trailingDelimiter) {
        this.trailingDelimiter = trailingDelimiter;
    }

    public boolean isTrim() {
        return trim;
    }

    public void setTrim(boolean trim) {
        this.trim = trim;
    }

    public String getUniqueAttribute() {
        return uniqueAttribute;
    }

    public void setUniqueAttribute(String uniqueAttribute) {
        this.uniqueAttribute = uniqueAttribute;
        if (StringUtil.isEmpty(nameAttribute)) {
            nameAttribute = uniqueAttribute;
        }
    }

    public String getNameAttribute() {
        return nameAttribute;
    }

    public void setNameAttribute(String nameAttribute) {
        this.nameAttribute = StringUtil.isEmpty(nameAttribute) ? this.uniqueAttribute : nameAttribute;
    }

    public String getPasswordAttribute() {
        return passwordAttribute;
    }

    public void setPasswordAttribute(String passwordAttribute) {
        this.passwordAttribute = passwordAttribute;
    }

    public String getMultivalueDelimiter() {
        return multivalueDelimiter;
    }

    public void setMultivalueDelimiter(String multivalueDelimiter) {
        this.multivalueDelimiter = multivalueDelimiter;
    }

    public int getPreserveOldSyncFiles() {
        return preserveOldSyncFiles;
    }

    public void setPreserveOldSyncFiles(int preserveOldSyncFiles) {
        this.preserveOldSyncFiles = preserveOldSyncFiles;
    }

    public void validate() {
        LOG.ok("Validating configuration for {0}", objectClass);

        Util.checkCanReadFile(filePath);

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
    }
}
