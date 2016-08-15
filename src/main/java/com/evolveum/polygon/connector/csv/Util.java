package com.evolveum.polygon.connector.csv;

import org.identityconnectors.common.StringUtil;
import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
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

    public static BufferedReader createReader(CsvConfiguration configuration) throws IOException {
        return createReader(configuration.getFilePath(), configuration);
    }

    public static BufferedReader createReader(File path, CsvConfiguration configuration) throws IOException {
        FileInputStream fis = new FileInputStream(path);
        InputStreamReader in = new InputStreamReader(fis, configuration.getEncoding());
        return new BufferedReader(in);
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

    public static void validateNotEmpty(String str, String message) {
        if (StringUtil.isEmpty(str)) {
            throw new ConfigurationException(message);
        }
    }
}
