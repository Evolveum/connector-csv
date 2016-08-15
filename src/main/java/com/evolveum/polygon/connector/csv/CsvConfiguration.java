package com.evolveum.polygon.connector.csv;

import org.identityconnectors.common.Assertions;
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
    // Exposed configuration properties.
    private File filePath = null;
    private String encoding = "utf-8";
    private String valueQualifier = "\"";
    private String fieldDelimiter = ",";
    private String multivalueDelimiter = ";";
    private boolean usingMultivalue = false;
    private String uniqueAttribute = null;
    private String nameAttribute = null;
    private String passwordAttribute = null;
    private boolean alwaysQualify = true;
    private int preserveLastTokens = 10;

    @ConfigurationProperty(displayMessageKey = "UI_PRESERVE_LAST_TOKENS",
            helpMessageKey = "UI_PRESERVE_LAST_TOKENS_HELP")
    public int getPreserveLastTokens() {
        return preserveLastTokens;
    }

    public void setPreserveLastTokens(int preserveLastTokens) {
        this.preserveLastTokens = preserveLastTokens;
    }

    @ConfigurationProperty(displayMessageKey = "UI_FLAT_ENCODING",
            helpMessageKey = "UI_FLAT_ENCODING_HELP")
    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    @ConfigurationProperty(displayMessageKey = "UI_FLAT_FIELD_DELIMITER",
            helpMessageKey = "UI_FLAT_FIELD_DELIMITER_HELP")
    public String getFieldDelimiter() {
        return fieldDelimiter;
    }

    public void setFieldDelimiter(String fieldDelimiter) {
        this.fieldDelimiter = fieldDelimiter;
    }

    @ConfigurationProperty(displayMessageKey = "UI_FLAT_FILE_PATH",
            helpMessageKey = "UI_FLAT_FILE_PATH_HELP", required = true)
    public File getFilePath() {
        return filePath;
    }

    public void setFilePath(File filePath) {
        this.filePath = filePath;
    }

    @ConfigurationProperty(displayMessageKey = "UI_FLAT_MULTIVALUE_DELIMITER",
            helpMessageKey = "UI_FLAT_MULTIVALUE_DELIMITER_HELP")
    public String getMultivalueDelimiter() {
        return multivalueDelimiter;
    }

    public void setMultivalueDelimiter(String multivalueDelimiter) {
        this.multivalueDelimiter = multivalueDelimiter;
    }

    @ConfigurationProperty(displayMessageKey = "UI_FLAT_PASSWORD_ATTRIBUTE",
            helpMessageKey = "UI_FLAT_PASSWORD_ATTRIBUTE_HELP")
    public String getPasswordAttribute() {
        return passwordAttribute;
    }

    public void setPasswordAttribute(String passwordAttribute) {
        this.passwordAttribute = passwordAttribute;
    }

    @ConfigurationProperty(displayMessageKey = "UI_FLAT_UNIQUE_ATTRIBUTE",
            helpMessageKey = "UI_FLAT_UNIQUE_ATTRIBUTE_HELP", required = true)
    public String getUniqueAttribute() {
        return uniqueAttribute;
    }

    public void setUniqueAttribute(String uniqueAttribute) {
        this.uniqueAttribute = uniqueAttribute;
        if (StringUtil.isEmpty(nameAttribute)) {
            nameAttribute = uniqueAttribute;
        }
    }

    @ConfigurationProperty(displayMessageKey = "UI_FLAT_VALUE_QUALIFIER",
            helpMessageKey = "UI_FLAT_VALUE_QUALIFIER_HELP")
    public String getValueQualifier() {
        return valueQualifier;
    }

    public void setValueQualifier(String valueQualifier) {
        this.valueQualifier = valueQualifier;
    }

    @ConfigurationProperty(displayMessageKey = "UI_FLAT_USING_MULTIVALUE",
            helpMessageKey = "UI_FLAT_USING_MULTIVALUE_HELP")
    public boolean isUsingMultivalue() {
        return usingMultivalue;
    }

    public void setUsingMultivalue(boolean usingMultivalue) {
        this.usingMultivalue = usingMultivalue;
    }

    @ConfigurationProperty(displayMessageKey = "UI_FLAT_NAME_ATTRIBUTE",
            helpMessageKey = "UI_FLAT_NAME_ATTRIBUTE_HELP", required = true)
    public String getNameAttribute() {
        return nameAttribute;
    }

    public void setNameAttribute(String nameAttribute) {
        this.nameAttribute = nameAttribute;
    }

    @ConfigurationProperty(displayMessageKey = "UI_FLAT_FILE_ALWAYS_QUALIFY",
            helpMessageKey = "UI_FLAT_FILE_ALWAYS_QUALIFY_HELP", required = true)
    public boolean getAlwaysQualify() {
        return alwaysQualify;
    }

    public void setAlwaysQualify(boolean alwaysQualify) {
        this.alwaysQualify = alwaysQualify;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void validate() {
        LOG.info("begin");

        if (StringUtil.isEmpty(encoding)) {
            throw new ConfigurationException("Encoding is not defined.");
        }
        if (!Charset.isSupported(encoding)) {
            throw new ConfigurationException("Encoding '" + encoding + "' is not supported.");
        }

        if (StringUtil.isEmpty(fieldDelimiter)) {
            throw new ConfigurationException("Field delimiter can't be null or empty.");
        }

        if (StringUtil.isEmpty(valueQualifier)) {
            throw new ConfigurationException("Value qualifier can't be null or empty.");
        }

        if (StringUtil.isEmpty(multivalueDelimiter)) {
            throw new ConfigurationException("Multivalue delimiter delimiter can't be null or empty.");
        }

        if (fieldDelimiter.equals(valueQualifier)) {
            throw new ConfigurationException("Field delimiter '" + fieldDelimiter + "' can't be equal to value qualifier.");
        }

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

        Assertions.nullCheck(filePath, "File path");

        if (!filePath.exists()) {
            throw new ConfigurationException("File '" + filePath + "' doesn't exists. At least file with csv header must exist.");
        }
        if (filePath.isDirectory()) {
            throw new ConfigurationException("File path '" + filePath + "' points to a directory.");
        }
        if (!filePath.canRead()) {
            throw new ConfigurationException("File '" + filePath + "' can't be read.");
        }
        if (!filePath.canWrite()) {
            throw new ConfigurationException("Can't write to file '" + filePath.getAbsolutePath() + "'.");
        }

        LOG.info("end");
    }

    boolean isUniqueAndNameAttributeEqual() {
        return uniqueAttribute == null ? nameAttribute == null : uniqueAttribute.equals(nameAttribute);
    }
}
