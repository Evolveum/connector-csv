package com.evolveum.polygon.connector.csv;

import org.identityconnectors.common.security.GuardedString;

/**
 * Created by Viliam Repan (lazyman).
 */
public class SimpleAccessor implements GuardedString.Accessor {

    private String password;

    @Override
    public void access(char[] chars) {
        password = chars == null ? null : String.valueOf(chars);
    }

    public String getPassword() {
        return password;
    }
}
