package com.evolveum.polygon.connector.csv.util;

import java.util.Objects;

public class AssociationHolder {

    private String associationAttributeName;
    private String valueAttributeName;
    private String subjectObjectClassName;
    private String objectObjectClassName;
    private boolean isAccess = false;
    private boolean omitFromSchema = false;
    private AssociationCharacter character;

    public String getAssociationAttributeName() {
        return associationAttributeName;
    }

    public void setAssociationAttributeName(String associationAttributeName) {
        this.associationAttributeName = associationAttributeName;
    }

    public String getValueAttributeName() {
        return valueAttributeName;
    }

    public void setValueAttributeName(String valueAttributeName) {
        this.valueAttributeName = valueAttributeName;
    }

    public String getSubjectObjectClassName() {
        return subjectObjectClassName;
    }

    public void setSubjectObjectClassName(String subjectObjectClassName) {
        this.subjectObjectClassName = subjectObjectClassName;
    }

    public String getObjectObjectClassName() {
        return objectObjectClassName;
    }

    public void setObjectObjectClassName(String objectObjectClassName) {
        this.objectObjectClassName = objectObjectClassName;
    }

    public AssociationCharacter getCharacter() {
        return character;
    }

    public void setCharacter(AssociationCharacter character) {
        this.character = character;
    }

    public void setAccess(boolean isAccess) {
        this.isAccess = isAccess;
    }

    public boolean isAccess() {
        return isAccess;
    }

    public boolean isOmitFromSchema() {
        return omitFromSchema;
    }

    public void setOmitFromSchema(boolean omitFromSchema) {
        this.omitFromSchema = omitFromSchema;
    }

}
