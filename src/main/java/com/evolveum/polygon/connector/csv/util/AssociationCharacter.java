package com.evolveum.polygon.connector.csv.util;

public enum AssociationCharacter {
//    CONF_ASSOC_DELIMITER_SUBJECT_ID("--#"),
    REFERS_TO("#-"),
    OBTAINS("-#");

    public final String value;

    AssociationCharacter(String value) {

        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }
}
