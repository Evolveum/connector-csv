package com.evolveum.polygon.connector.csv;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by Viliam Repan (lazyman).
 */
public class CsvHeaderDescriptor {

    private Map<CsvHeader, Integer> headers = new HashMap<>();

    private Map<String, Integer> attributes = new HashMap<>();
    private Map<String, Integer> columns = new HashMap<>();

    public CsvHeaderDescriptor(Map<CsvHeader, Integer> headers) {
        if (headers != null) {
            this.headers = headers;
        }

        for (CsvHeader header : headers.keySet()) {
            attributes.put(header.getAttribute(), headers.get(header));
            columns.put(header.getColumn(), headers.get(header));
        }
    }

    public Map<CsvHeader, Integer> getHeaders() {
        return Collections.unmodifiableMap(headers);
    }

    public Map<String, Integer> getAttributes() {
        return Collections.unmodifiableMap(attributes);
    }

    public Map<String, Integer> getColumns() {
        return Collections.unmodifiableMap(columns);
    }

    public Set<String> getColumnSet() {
        return Collections.unmodifiableSet(columns.keySet());
    }

    public Set<String> getAttributeSet() {
        return Collections.unmodifiableSet(attributes.keySet());
    }
}
