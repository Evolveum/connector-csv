package com.evolveum.polygon.connector.csv;

import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.objects.*;
import org.identityconnectors.framework.common.objects.filter.FilterTranslator;
import org.identityconnectors.framework.spi.operations.*;

import java.util.Set;

/**
 * Created by lazyman on 27/01/2017.
 */
public class ObjectClassHandler implements CreateOp, DeleteOp, TestOp, SchemaOp, SearchOp<String>,
        UpdateAttributeValuesOp, AuthenticateOp, ResolveUsernameOp, SyncOp {

    public static final String TMP_EXTENSION = ".tmp";

    private enum Operation {

        DELETE, UPDATE, ADD_ATTR_VALUE, REMOVE_ATTR_VALUE;
    }

    private static final Log LOG = Log.getLog(ObjectClassHandler.class);

    private ObjectClassHandlerConfiguration configuration;

    public ObjectClassHandler(ObjectClassHandlerConfiguration configuration) {
        this.configuration = configuration;
    }

    public ObjectClass getObjectClass() {
        return configuration.getObjectClass();
    }

    public void schema(SchemaBuilder schema) {
        //todo implement
    }

    @Override
    public Uid authenticate(ObjectClass oc, String username, GuardedString password, OperationOptions oo) {
        return null; //todo implement
    }

    @Override
    public Uid create(ObjectClass oc, Set<Attribute> set, OperationOptions oo) {
        return null; //todo implement
    }

    @Override
    public void delete(ObjectClass oc, Uid uid, OperationOptions oo) {

    }

    @Override
    public Uid resolveUsername(ObjectClass oc, String username, OperationOptions oo) {
        return null; //todo implement
    }

    @Override
    public Schema schema() {
        return null; //todo implement
    }

    @Override
    public FilterTranslator<String> createFilterTranslator(ObjectClass oc, OperationOptions oo) {
        return new CsvFilterTranslator();
    }

    @Override
    public void executeQuery(ObjectClass oc, String s, ResultsHandler handler, OperationOptions oo) {
        // todo implement
    }

    @Override
    public void sync(ObjectClass oc, SyncToken token, SyncResultsHandler handler, OperationOptions oo) {
        // todo implement
    }

    @Override
    public SyncToken getLatestSyncToken(ObjectClass oc) {
        return null; //todo implement
    }

    @Override
    public void test() {
        // todo implement
    }

    @Override
    public Uid addAttributeValues(ObjectClass oc, Uid uid, Set<Attribute> set, OperationOptions oo) {
        return null; //todo implement
    }

    @Override
    public Uid removeAttributeValues(ObjectClass oc, Uid uid, Set<Attribute> set, OperationOptions oo) {
        return null; //todo implement
    }

    @Override
    public Uid update(ObjectClass oc, Uid uid, Set<Attribute> set, OperationOptions oo) {
        return null; //todo implement
    }
}
