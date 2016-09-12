package com.evolveum.polygon.connector.csv.util;

import com.evolveum.polygon.connector.csv.BaseTest;
import com.evolveum.polygon.connector.csv.CsvConfiguration;
import com.evolveum.polygon.connector.csv.Util;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

/**
 * Created by Viliam Repan (lazyman).
 */
public class CsvTestUtil {

    public static Map<String, String> findRecord(CsvConfiguration config, String value) throws IOException {
        return findRecord(config, BaseTest.ATTR_UID, value);
    }

    public static Map<String, String> findRecord(CsvConfiguration config, String uniqueColumn, String value)
            throws IOException {
        Util.notNull(config, "CsvConfiguration must not be null");
        Util.notEmpty(uniqueColumn, "Unique column name must not be empty");

        CSVFormat csv = Util.createCsvFormat(config).withFirstRecordAsHeader();

        try (Reader reader = Util.createReader(config)) {

            CSVParser parser = csv.parse(reader);
            Iterator<CSVRecord> iterator = parser.iterator();
            while (iterator.hasNext()) {
                CSVRecord record = iterator.next();
                Map<String, String> map = record.toMap();

                String unique = map.get(uniqueColumn);
                if (Objects.equals(value, unique)) {
                    return map;
                }
            }
        }

        return null;
    }
}
