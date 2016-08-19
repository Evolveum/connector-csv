package com.evolveum.polygon.connector.csv;

import org.identityconnectors.framework.common.objects.ConnectorObject;
import org.identityconnectors.framework.common.objects.ResultsHandler;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Viliam Repan (lazyman).
 */
public class ListResultHandler implements ResultsHandler {

    private List<ConnectorObject> objects = new ArrayList<>();

    @Override
    public boolean handle(ConnectorObject object) {
        objects.add(object);

        return true;
    }
}
