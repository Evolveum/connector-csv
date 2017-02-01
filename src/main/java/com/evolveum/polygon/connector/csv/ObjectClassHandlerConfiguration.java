package com.evolveum.polygon.connector.csv;

import org.apache.commons.csv.QuoteMode;
import org.identityconnectors.framework.common.objects.ObjectClass;

import java.io.File;

/**
 * Created by lazyman on 29/01/2017.
 */
public class ObjectClassHandlerConfiguration {

    private ObjectClass objectClass;

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
    }

    public String getNameAttribute() {
        return nameAttribute;
    }

    public void setNameAttribute(String nameAttribute) {
        this.nameAttribute = nameAttribute;
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

    public int getPreserverOldSyncFiles() {
        return preserverOldSyncFiles;
    }

    public void setPreserverOldSyncFiles(int preserverOldSyncFiles) {
        this.preserverOldSyncFiles = preserverOldSyncFiles;
    }
}
