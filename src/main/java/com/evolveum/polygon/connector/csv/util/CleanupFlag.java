package com.evolveum.polygon.connector.csv.util;

public class CleanupFlag {

    boolean remove = false;
    boolean isSubjectForCleanup = false;


    public CleanupFlag(boolean isSubjectForCleanup) {
        this.isSubjectForCleanup = isSubjectForCleanup;
    }

    public boolean isSubjectForCleanup() {
        return isSubjectForCleanup;
    }

    public boolean isRemove() {
        return remove;
    }

    public void setRemove(boolean remove) {
        this.remove = remove;
    }
}
