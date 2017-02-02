package com.evolveum.polygon.connector.csv;

import com.evolveum.polygon.connector.csv.util.Util;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.*;
import org.identityconnectors.framework.common.objects.filter.FilterTranslator;
import org.identityconnectors.framework.spi.Configuration;
import org.identityconnectors.framework.spi.Connector;
import org.identityconnectors.framework.spi.ConnectorClass;
import org.identityconnectors.framework.spi.operations.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by Viliam Repan (lazyman).
 */
@ConnectorClass(
        displayNameKey = "UI_CSV_CONNECTOR_NAME",
        configurationClass = CsvConfiguration.class)
public class CsvConnector implements Connector, TestOp, SchemaOp, SearchOp<String>, AuthenticateOp,
        ResolveUsernameOp, SyncOp, CreateOp, UpdateOp, UpdateAttributeValuesOp, DeleteOp {

    private static final Log LOG = Log.getLog(CsvConnector.class);

    private CsvConfiguration configuration;

    private Map<ObjectClass, ObjectClassHandler> handlers = new HashMap<>();

    @Override
    public Configuration getConfiguration() {
        return configuration;
    }

    @Override
    public void init(Configuration configuration) {
        if (!(configuration instanceof CsvConfiguration)) {
            throw new ConfigurationException("Configuration is not instance of " + CsvConfiguration.class.getName());
        }

        configuration.validate();

        this.configuration = (CsvConfiguration) configuration;

        try {
            List<ObjectClassHandlerConfiguration> configs = this.configuration.getAllConfigs();
            configs.forEach(config -> handlers.put(config.getObjectClass(), new ObjectClassHandler(config)));
        } catch (Exception ex) {
            Util.handleGenericException(ex, "Couldn't initialize connector");
        }
    }

    @Override
    public void dispose() {
        configuration = null;
        handlers = null;
    }

    private ObjectClassHandler getHandler(ObjectClass oc) {
        ObjectClassHandler handler = handlers.get(oc);
        if (handler == null) {
            throw new ConnectorException("Unknown object class " + oc);
        }

        return handler;
    }

    @Override
    public Uid authenticate(ObjectClass oc, String username, GuardedString password, OperationOptions oo) {
        LOG.info("authenticate started {0} {1} {2} {3}", oc, username, password != null ? "password" : "null", oo);

        Uid uid = getHandler(oc).authenticate(oc, username, password, oo);

        LOG.info("authenticate finished");

        return uid;
    }

    @Override
    public Uid resolveUsername(ObjectClass oc, String username, OperationOptions oo) {
        LOG.info("resolveUsername started {0} {1} {2}", oc, username, oo);

        Uid uid = getHandler(oc).resolveUsername(oc, username, oo);

        LOG.info("authenticate finished");

        return uid;
    }

    @Override
    public Schema schema() {
        LOG.info("schema started");

        SchemaBuilder builder = new SchemaBuilder(CsvConnector.class);
        handlers.values().forEach(handler -> {

            LOG.info("schema started for {0}", handler.getObjectClass());

            handler.schema(builder);

            LOG.info("schema finished for {0}", handler.getObjectClass());
        });

        Schema schema = builder.build();
        LOG.info("schema finished");

        return schema;
    }

    @Override
    public FilterTranslator<String> createFilterTranslator(ObjectClass oc, OperationOptions oo) {
        LOG.info("createFilterTranslator {0} {1}", oc, oo);

        FilterTranslator<String> translator = getHandler(oc).createFilterTranslator(oc, oo);

        LOG.info("createFilterTranslator finished");

        return translator;
    }

    @Override
    public void executeQuery(ObjectClass oc, String uid, ResultsHandler handler, OperationOptions oo) {
        LOG.info("executeQuery {0} {1} {2} {3}", oc, uid, handler, oo);

        getHandler(oc).executeQuery(oc, uid, handler, oo);

        LOG.info("executeQuery finished");
    }

    @Override
    public void sync(ObjectClass oc, SyncToken token, SyncResultsHandler handler, OperationOptions oo) {
        LOG.info("sync {0} {1} {2} {3}", oc, token, handler, oo);

        getHandler(oc).sync(oc, token, handler, oo);

        LOG.info("sync finished");
    }

    @Override
    public SyncToken getLatestSyncToken(ObjectClass oc) {
        LOG.info("getLatestSyncToken {0}", oc);

        SyncToken token = getHandler(oc).getLatestSyncToken(oc);

        LOG.info("getLatestSyncToken finished");

        return token;
    }

    @Override
    public void test() {
        LOG.info("test started");

        handlers.values().forEach(handler -> {

            LOG.info("test started for {0}", handler.getObjectClass());

            handler.test();

            LOG.info("test finished for {0}", handler.getObjectClass());

        });

        LOG.info("test finished");
    }

    @Override
    public Uid create(ObjectClass oc, Set<Attribute> set, OperationOptions oo) {
        LOG.info("create {0} {1}", oc, oo);

        Uid u = getHandler(oc).create(oc, set, oo);

        LOG.info("create finished");

        return u;
    }

    @Override
    public void delete(ObjectClass oc, Uid uid, OperationOptions oo) {
        LOG.info("delete {0} {1} {2}", oc, uid, oo);

        getHandler(oc).delete(oc, uid, oo);

        LOG.info("delete finished");
    }

    @Override
    public Uid addAttributeValues(ObjectClass oc, Uid uid, Set<Attribute> set, OperationOptions oo) {
        LOG.info("addAttributeValues {0} {1} {2} {3}", oc, uid, set, oo);

        Uid u = getHandler(oc).addAttributeValues(oc, uid, set, oo);

        LOG.info("addAttributeValues finished");

        return u;
    }

    @Override
    public Uid removeAttributeValues(ObjectClass oc, Uid uid, Set<Attribute> set, OperationOptions oo) {
        LOG.info("removeAttributeValues {0} {1} {2} {3}", oc, uid, set, oo);

        Uid u = getHandler(oc).removeAttributeValues(oc, uid, set, oo);

        LOG.info("removeAttributeValues finished");

        return u;
    }

    @Override
    public Uid update(ObjectClass oc, Uid uid, Set<Attribute> set, OperationOptions oo) {
        LOG.info("update {0} {1} {2}", oc, set, oo);

        Uid u = getHandler(oc).update(oc, uid, set, oo);

        LOG.info("update finished");

        return u;
    }
}
