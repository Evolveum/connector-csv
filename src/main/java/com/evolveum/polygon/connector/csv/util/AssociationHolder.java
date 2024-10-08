package com.evolveum.polygon.connector.csv.util;

import java.util.Objects;

public class AssociationHolder {

    private String associationAttributeName;
    private String valueAttributeName;
    private String subjectObjectClassName;
    private String objectObjectClassName;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AssociationHolder holder = (AssociationHolder) o;
        return Objects.equals(getAssociationAttributeName(), holder.getAssociationAttributeName()) && Objects.equals(getValueAttributeName(), holder.getValueAttributeName()) && Objects.equals(getSubjectObjectClassName(), holder.getSubjectObjectClassName()) && Objects.equals(getObjectObjectClassName(), holder.getObjectObjectClassName()) && getCharacter() == holder.getCharacter();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getAssociationAttributeName(), getValueAttributeName(), getSubjectObjectClassName(), getObjectObjectClassName(), getCharacter());
    }
}
