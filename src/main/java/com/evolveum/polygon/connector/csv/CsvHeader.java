package com.evolveum.polygon.connector.csv;

import java.util.Arrays;

/**
 * Created by Viliam Repan (lazyman).
 */
public class CsvHeader {

    private String column;
    private String attribute;

    public CsvHeader(String column, String attribute) {
        this.column = column;
        this.attribute = attribute;
    }

    public String getColumn() {
        return column;
    }

    public String getAttribute() {
        return attribute;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CsvHeader that = (CsvHeader) o;

        if (column != null ? !column.equals(that.column) : that.column != null) return false;
        return attribute != null ? attribute.equals(that.attribute) : that.attribute == null;

    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(new Object[]{column, attribute});
    }

    @Override
    public String toString() {
        return "Header{c=" + column + ",a=" + attribute + "}";
    }
}
