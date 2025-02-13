package com.evolveum.polygon.connector.csv;

import com.evolveum.polygon.connector.csv.util.AssociationCharacter;
import com.evolveum.polygon.connector.csv.util.AssociationHolder;
import com.evolveum.polygon.connector.csv.util.Util;
import org.apache.commons.lang3.ArrayUtils;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.exceptions.*;
import org.identityconnectors.framework.common.objects.*;
import org.identityconnectors.framework.common.objects.filter.Filter;
import org.identityconnectors.framework.common.objects.filter.FilterTranslator;
import org.identityconnectors.framework.spi.Configuration;
import org.identityconnectors.framework.spi.Connector;
import org.identityconnectors.framework.spi.ConnectorClass;
import org.identityconnectors.framework.spi.operations.*;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import static com.evolveum.polygon.connector.csv.util.AssociationCharacter.OBTAINS;
import static com.evolveum.polygon.connector.csv.util.AssociationCharacter.REFERS_TO;
import static com.evolveum.polygon.connector.csv.util.Util.ASSOC_ATTR_ACCESS;
import static com.evolveum.polygon.connector.csv.util.Util.ASSOC_ATTR_GROUP;

/**
 * Created by Viliam Repan (lazyman).
 */
@ConnectorClass(
        displayNameKey = "UI_CSV_CONNECTOR_NAME",
        configurationClass = CsvConfiguration.class)
public class CsvConnector implements Connector, TestOp, SchemaOp, SearchOp<Filter>, AuthenticateOp,
        ResolveUsernameOp, SyncOp, CreateOp, UpdateOp, UpdateAttributeValuesOp, DeleteOp, ScriptOnResourceOp,
        ScriptOnConnectorOp, DiscoverConfigurationOp {

	public static final Integer SYNCH_FILE_LOCK = 0;

    private static final Log LOG = Log.getLog(CsvConnector.class);

    private CsvConfiguration configuration;

    private Map<ObjectClass, ObjectClassHandler> handlers = new HashMap<>();

    protected Map<String, HashSet<AssociationHolder>> associationHolders;

    private String CONF_ASSOC_ATTR_DELIMITER ="\"\\+";

    @Override
    public Configuration getConfiguration() {
        return configuration;
    }

    @Override
    public void init(Configuration configuration) {
        LOG.info(">>> Initializing connector");

        if (!(configuration instanceof CsvConfiguration)) {
            throw new ConfigurationException("Configuration is not instance of " + CsvConfiguration.class.getName());
        }

        CsvConfiguration csvConfig = (CsvConfiguration) configuration;
        csvConfig.validate();

        this.configuration = csvConfig;

        try {
            List<ObjectClassHandlerConfiguration> configs = this.configuration.getAllConfigs();
            configs.forEach(config -> handlers.put(config.getObjectClass(), new ObjectClassHandler(config)));

            if(!ArrayUtils.isEmpty(((CsvConfiguration) configuration)
                    .getManagedAssociationPairs())) {
                generateAssociationHolders();
                for (ObjectClass objectClass : handlers.keySet()) {

                    Map<ObjectClass, ObjectClassHandler> handlersNotCurrent = new HashMap<>();

                    handlersNotCurrent.putAll(handlers);
                    handlersNotCurrent.remove(objectClass);
                    handlers.get(objectClass).setHandlers(handlersNotCurrent);
                    handlers.get(objectClass).setAssociationHolders(associationHolders);
                }
            }

        } catch (Exception ex) {
            Util.handleGenericException(ex, "Couldn't initialize connector");
        }

        LOG.info(">>> Connector initialization finished");
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
        handler.validate();

        return handler;
    }

    @Override
    public Uid authenticate(ObjectClass oc, String username, GuardedString password, OperationOptions oo) {
        LOG.info(">>> authenticate started {0} {1} {2} {3}", oc, username, password != null ? "password" : "null", oo);

        Uid uid = getHandler(oc).authenticate(oc, username, password, oo);

        LOG.info(">>> authenticate finished");

        return uid;
    }

    @Override
    public Uid resolveUsername(ObjectClass oc, String username, OperationOptions oo) {
        LOG.info(">>> resolveUsername started {0} {1} {2}", oc, username, oo);

        Uid uid = getHandler(oc).resolveUsername(oc, username, oo);

        LOG.info(">>> authenticate finished");

        return uid;
    }

    @Override
    public Schema schema() {
        LOG.info(">>> schema started");

        SchemaBuilder builder = new SchemaBuilder(CsvConnector.class);
        handlers.values().forEach(handler -> {

            LOG.info("schema started for {0}", handler.getObjectClass());

            handler.schema(builder);

            LOG.info("schema finished for {0}", handler.getObjectClass());
        });

        Schema schema = builder.build();
        LOG.info(">>> schema finished");

        return schema;
    }

    @Override
    public FilterTranslator<Filter> createFilterTranslator(ObjectClass oc, OperationOptions oo) {
        LOG.info(">>> createFilterTranslator {0} {1}", oc, oo);

        // Just return dummy filter translator that does not translate anything. We need better control over the
        // filter translation than what the framework can provide.
        FilterTranslator<Filter> translator = new FilterTranslator<Filter>() {
            @Override
            public List<Filter> translate(Filter filter) {
                List<Filter> list = new ArrayList<>(1);
                list.add(filter);
                return list;
            }
        };

        LOG.info(">>> createFilterTranslator finished");

        return translator;
    }

    @Override
    public void executeQuery(ObjectClass oc, Filter filter, ResultsHandler handler, OperationOptions oo) {
        LOG.info(">>> executeQuery {0} {1} {2} {3}", oc, filter, handler, oo);

        getHandler(oc).executeQuery(oc, filter, handler, oo);

        LOG.info(">>> executeQuery finished");
    }

    @Override
    public void sync(ObjectClass oc, SyncToken token, SyncResultsHandler handler, OperationOptions oo) {
        LOG.info(">>> sync {0} {1} {2} {3}", oc, token, handler, oo);

        if(!ObjectClass.ALL.equals(oc)) {

            getHandler(oc).sync(oc, token, handler, oo);
        } else {

            if(!ArrayUtils.isEmpty(configuration.getManagedAssociationPairs())){
                executeSyncInOrder(oc, token, handler, oo);
            }
        }
        LOG.info(">>> sync finished");
    }

    private void executeSyncInOrder(ObjectClass oc, SyncToken token, SyncResultsHandler handler, OperationOptions oo) {

        LinkedList<ObjectClassHandler> handlersInOrder = new LinkedList<>();
        LinkedList<String> objectObjectClassNames = new LinkedList<>();

        for (String objectClassName : associationHolders.keySet()) {

            Set<AssociationHolder> holders = associationHolders.get(objectClassName);

            for (AssociationHolder holder : holders) {
                String holderObjectObjectClassName = holder.getObjectObjectClassName();
                String holderSubjectObjectClassName = holder.getSubjectObjectClassName();
                AssociationCharacter character = holder.getCharacter();

                if (objectObjectClassNames.contains(holderSubjectObjectClassName)) {

                    int i = objectObjectClassNames.indexOf(holderSubjectObjectClassName);
                    if (i != 0) {
                        if (character.equals(OBTAINS)) {

                            if (!objectObjectClassNames.contains(holderObjectObjectClassName)) {

                                objectObjectClassNames.add(i - 1, holderObjectObjectClassName);
                            } else {

                                objectObjectClassNames.remove(holderObjectObjectClassName);
                                objectObjectClassNames.add(i - 1, holderObjectObjectClassName);
                            }
                        }
                    } else {
                        if (character.equals(OBTAINS)) {
                            if (!objectObjectClassNames.contains(holderObjectObjectClassName)) {

                                objectObjectClassNames.addFirst(holderObjectObjectClassName);
                            } else {

                                objectObjectClassNames.remove(holderObjectObjectClassName);
                                objectObjectClassNames.addFirst(holderObjectObjectClassName);
                            }
                        }
                    }
                } else {
                    if (character.equals(REFERS_TO)) {
                        objectObjectClassNames.add(holder.getSubjectObjectClassName());
                    } else {

                        if (!objectObjectClassNames.contains(holderObjectObjectClassName)) {

                            objectObjectClassNames.add(holderObjectObjectClassName);
                        }
                    }
                }
            }
        }

        for (String ocName : objectObjectClassNames) {
            ObjectClassHandler ocHanler = handlers.get(Util.getObjectClass(ocName));

            if(ocHanler!=null){

                handlersInOrder.add(ocHanler);
            } else {

                LOG.warn("The Handler of the object class '{0}' not found when deducing the order of sync operations.", ocName);
            }
        }

        for(ObjectClassHandler h : handlers.values()){
            if(!handlersInOrder.contains(h)){
                handlersInOrder.add(h);
            }
        }

        Map<String, HashSet<String>> syncHook = new HashMap<>();

        for (ObjectClassHandler ocHandler : handlersInOrder) {

            ocHandler.setSyncHook(syncHook);
            ocHandler.sync(oc, token, handler, oo);
        }
    }

    @Override
    public SyncToken getLatestSyncToken(ObjectClass oc) {
        LOG.info(">>> getLatestSyncToken {0}", oc);

        SyncToken token = getHandler(oc).getLatestSyncToken(oc);

        LOG.info(">>> getLatestSyncToken finished");

        return token;
    }

    @Override
    public void test() {
        LOG.info(">>> test started");

        handlers.values().forEach(handler -> {

            LOG.info("test started for {0}", handler.getObjectClass());

            handler.test();

            LOG.info("test finished for {0}", handler.getObjectClass());

        });

        LOG.info(">>> test finished");
    }

    @Override
    public Uid create(ObjectClass oc, Set<Attribute> set, OperationOptions oo) {
        LOG.info(">>> create {0} {1}", oc, oo);

        Uid u = getHandler(oc).create(oc, set, oo);

        LOG.info(">>> create finished");

        return u;
    }

    @Override
    public void delete(ObjectClass oc, Uid uid, OperationOptions oo) {
        LOG.info(">>> delete {0} {1} {2}", oc, uid, oo);

        getHandler(oc).delete(oc, uid, oo);

        LOG.info(">>> delete finished");
    }

    @Override
    public Uid addAttributeValues(ObjectClass oc, Uid uid, Set<Attribute> set, OperationOptions oo) {
        LOG.info(">>> addAttributeValues {0} {1} {2} {3}", oc, uid, set, oo);

        Uid u = getHandler(oc).addAttributeValues(oc, uid, set, oo);

        LOG.info(">>> addAttributeValues finished");

        return u;
    }

    @Override
    public Uid removeAttributeValues(ObjectClass oc, Uid uid, Set<Attribute> set, OperationOptions oo) {
        LOG.info(">>> removeAttributeValues {0} {1} {2} {3}", oc, uid, set, oo);

        Uid u = getHandler(oc).removeAttributeValues(oc, uid, set, oo);

        LOG.info(">>> removeAttributeValues finished");

        return u;
    }

    @Override
    public Uid update(ObjectClass oc, Uid uid, Set<Attribute> set, OperationOptions oo) {
        LOG.info(">>> update {0} {1} {2}", oc, set, oo);

        Uid u = getHandler(oc).update(oc, uid, set, oo);

        LOG.info(">>> update finished");

        return u;
    }

	@Override
	public Object runScriptOnConnector(ScriptContext request, OperationOptions oo) {
        return runScriptOnResource(request, oo);
	}

	@Override
	public Object runScriptOnResource(ScriptContext request, OperationOptions oo) {
		String command = request.getScriptText();
		String[] commandArray = command.split("\\s+");
		ProcessBuilder pb = new ProcessBuilder(commandArray);
		Map<String, String> env = pb.environment();
		//iterate map of arguments
		for (Entry<String,Object> argEntry: request.getScriptArguments().entrySet()) {
			String varName = argEntry.getKey();
			Object varValue = argEntry.getValue();
			if (varValue == null) {
				env.remove(varName);
			} else {
				env.put(varName, varValue.toString());
			}
		}
		//execute command
		Process process;
		try {
			LOG.ok("Executing ''{0}''", command);
			process = pb.start();
			int exitCode = process.waitFor();
			LOG.ok("Execution of ''{0}'' finished, exit code {1}", command, exitCode);
			return exitCode;
		}
		catch (IOException e) {
			LOG.error("Execution of ''{0}'' failed (exec): {1} ({2})", command, e.getMessage(), e.getClass());
			throw new ConnectorIOException(e.getMessage(), e);
		}
		catch (InterruptedException e) {
			LOG.error("Execution of ''{0}'' failed (waitFor): {1} ({2})", command, e.getMessage(), e.getClass());
			throw new ConnectionBrokenException(e.getMessage(), e);
		}
	}

    @Override
    public void testPartialConfiguration() {
        LOG.info(">>> test partial configuration started");
        handlers.values().forEach(handler -> {
            LOG.info("test partial configuration started for {0}", handler.getObjectClass());

            handler.testPartialConfiguration();

            LOG.info("test partial configuration finished for {0}", handler.getObjectClass());
        });
        LOG.info(">>> test partial configuration finished");
    }

    @Override
    public Map<String, SuggestedValues> discoverConfiguration() {
        LOG.info(">>> discover configuration started");

        Map<String, SuggestedValues> suggestions = new HashMap<>();
        handlers.values().forEach(handler -> {
            suggestions.putAll(handler.discoverConfiguration());
        });

        LOG.info(">>> discover configuration finished");

        return suggestions;
    }

    private void generateAssociationHolders() {

        String[] associationPairs = configuration.getManagedAssociationPairs();
        Set<String[]> obtainArrays = new HashSet<>();
        Set<String[]> refersArrays = null;
        boolean isAccess = false;

        for (String associationPair : associationPairs) {

            String[] pairArray;
            if (associationPair.contains(REFERS_TO.value)) {

                pairArray = associationPair.split(REFERS_TO.value);
                if (refersArrays == null) {
                    refersArrays = new HashSet<>();
                }

                refersArrays.add(pairArray);

            } else if (associationPair.contains(OBTAINS.value)) {

                pairArray = associationPair.split(OBTAINS.value);
                obtainArrays.add(pairArray);

            } else {

                throw new InvalidAttributeValueException("Association pair syntax contains none of the permitted " +
                        "delimiters \"" + REFERS_TO + "\", \"" + OBTAINS + " \"");
            }
        }


        if (refersArrays != null) {

            isAccess = true;
            for (String[] refArray : refersArrays) {

                constructAssociationHolders(refArray, REFERS_TO, false);
            }
        }

       Set<String> objectClassesWithMultipleRefAttributes = checkForReferenceAttributeMultiplicity(obtainArrays);

        for (String[] obtainsArray : obtainArrays) {

            if(objectClassesWithMultipleRefAttributes != null) {

                constructAssociationHolders(obtainsArray, OBTAINS, isAccess, objectClassesWithMultipleRefAttributes);
            }else {

                constructAssociationHolders(obtainsArray, OBTAINS, isAccess);
            }
        }
    }

    private Set<String> checkForReferenceAttributeMultiplicity(Set<String[]> obtainArrays) {

        Map<String, Set<String>> multipleRefAttributes = new HashMap<>();
        Set<String> finalSet = new HashSet<>();
        for (String[] array : obtainArrays) {
            Set<String> objectClassesWithMultipleRefAttributes = new HashSet<>();
            String subjectName = "";
            for (int i = 0; i <= 1; i++) {

                String objectObjectClass = array[i];
                String[] objectClassAndMemberAttributes = objectObjectClass.split(CONF_ASSOC_ATTR_DELIMITER);
                String objectClassName = objectClassAndMemberAttributes[0].
                        trim().substring(1);

                if (i == 0) {
                    subjectName = objectClassName;
                    objectClassesWithMultipleRefAttributes = multipleRefAttributes.get(objectClassName);

                } else {

                    if (objectClassesWithMultipleRefAttributes == null) {
                        objectClassesWithMultipleRefAttributes = new HashSet<>();
                    }

                    if (!objectClassesWithMultipleRefAttributes.contains(objectClassName)) {

                        objectClassesWithMultipleRefAttributes.add(objectClassName);
                        multipleRefAttributes.put(subjectName, objectClassesWithMultipleRefAttributes);
                    } else {
                        finalSet.add(objectClassName);
                    }
                }
            }
        }

        return finalSet;
    }

    private void constructAssociationHolders(String[] pairArray, AssociationCharacter character, boolean access) {

        constructAssociationHolders(pairArray, character, access, null);
    }

    private void constructAssociationHolders(String[] pairArray, AssociationCharacter character, boolean access,
                                             Set<String> multiReferenceObjectClasses) {
        if (associationHolders == null) {
            associationHolders = new HashMap<>();
        }

        AssociationHolder associationHolder = constructAssociationHolder(pairArray, character, access,
                false, multiReferenceObjectClasses);

        if (associationHolders != null && !associationHolders.isEmpty()) {

            Map<String, String> objectClasses = new HashMap<>();
            objectClasses.put(Util.R_I_R_SUBJECT, associationHolder.getSubjectObjectClassName());
            objectClasses.put(Util.R_I_R_OBJECT, associationHolder.getObjectObjectClassName());

            for (String spec : objectClasses.keySet()) {

                String ocName = objectClasses.get(spec);

                HashSet associationHolderSet;
                if (!associationHolders.containsKey(ocName)) {

                    associationHolderSet = new HashSet<>();
                } else {
                    associationHolderSet = associationHolders.get(ocName);
                }

                if (Util.R_I_R_SUBJECT.equals(spec)) {
                    associationHolderSet.add(associationHolder);
                    associationHolders.put(ocName, associationHolderSet);
                } else {

                    if (OBTAINS.equals(character)) {
                        AssociationHolder associationHolderReferredOc = constructAssociationHolder(pairArray, character,
                                access, true, multiReferenceObjectClasses);
                        associationHolderSet.add(associationHolderReferredOc);
                        associationHolders.put(ocName, associationHolderSet);
                    } else {
                        associationHolderSet.add(associationHolder);
                        associationHolders.put(ocName, associationHolderSet);
                    }
                }

            }
        } else {
            HashSet hashSet = new HashSet();

            if (OBTAINS.equals(character)) {
                hashSet.add(associationHolder);
                associationHolders.put(associationHolder.getSubjectObjectClassName(), hashSet);
                associationHolders.put(associationHolder.getObjectObjectClassName(), (HashSet<AssociationHolder>) hashSet.clone());
            } else {
                hashSet.add(associationHolder);
                associationHolders.put(associationHolder.getSubjectObjectClassName(), hashSet);

                HashSet objHashSet = new HashSet();
                AssociationHolder associationHolderReferredOc = constructAssociationHolder(pairArray, character, access,
                        true, multiReferenceObjectClasses);
                objHashSet.add(associationHolderReferredOc);
                associationHolders.put(associationHolder.getObjectObjectClassName(), objHashSet);
            }
        }
    }

    private AssociationHolder constructAssociationHolder(String[] pairArray, AssociationCharacter character,
                                                         boolean access, boolean omitFromSchema,
                                                         Set<String> multiReferenceObjectClasses) {
        if(LOG.isOk()){

            LOG.ok("Constructing association holder. Association Character {0}, Is Access {1}, Will be omitted from" +
                    " schema {2}. {3}", character, access, omitFromSchema, System.lineSeparator());
        }

        AssociationHolder associationHolder = new AssociationHolder();
        if (pairArray.length == 2) {

            for (int i = 0; i < 2; i++) {

                String objectClassAndMemberAttribute = pairArray[i].trim();
                String[] objectClassAndMemberAttributes = objectClassAndMemberAttribute.split(CONF_ASSOC_ATTR_DELIMITER);

                if (i == 0) {
                    if (objectClassAndMemberAttributes.length != 0 && !objectClassAndMemberAttributes[0].isEmpty()) {

                        String subjectObjectClassName = objectClassAndMemberAttributes[0].
                                trim().substring(1);

                        if (!subjectObjectClassName.isEmpty()) {
                            associationHolder.setSubjectObjectClassName(subjectObjectClassName);
                        } else {

                            associationHolder.setSubjectObjectClassName(ObjectClass.ACCOUNT_NAME);
                        }
                    } else {

                        associationHolder.setSubjectObjectClassName(ObjectClass.ACCOUNT_NAME);
                    }

                    if (OBTAINS.equals(character)) {

                        if (objectClassAndMemberAttributes.length != 2) {

                            associationHolder.setAssociationAttributeName(null);
                        } else {

                            associationHolder.setAssociationAttributeName(objectClassAndMemberAttributes[1].
                                    trim());
                        }
                    } else if (REFERS_TO.equals(character)) {

                        if (objectClassAndMemberAttributes.length != 2) {

                            throw new InvalidAttributeValueException("Association pair syntax contain no or " +
                                    "multiple delimiters \" " + CONF_ASSOC_ATTR_DELIMITER + " \"");
                        }

                        associationHolder.setValueAttributeName(objectClassAndMemberAttributes[1].trim());
                    }
                } else {

                    if (objectClassAndMemberAttributes.length != 2) {

                        throw new InvalidAttributeValueException("Association pair syntax contain no or " +
                                "multiple delimiters \" " + CONF_ASSOC_ATTR_DELIMITER + " \"");
                    }

                    associationHolder.setObjectObjectClassName(objectClassAndMemberAttributes[0].
                            trim().substring(1));
                    if (OBTAINS.equals(character)) {

                        associationHolder.setCharacter(OBTAINS);
                        associationHolder.setAccess(access);
                        associationHolder.setValueAttributeName(objectClassAndMemberAttributes[1].trim());

                        if(access){

                        associationHolder.setReferenceName(ASSOC_ATTR_ACCESS);
                        } else {

                            if(multiReferenceObjectClasses!=null &&
                                    multiReferenceObjectClasses.contains(associationHolder.getObjectObjectClassName())){

                                associationHolder.setReferenceName(ASSOC_ATTR_GROUP +"-"
                                        + objectClassAndMemberAttributes[1].trim());
                            } else {

                                associationHolder.setReferenceName(ASSOC_ATTR_GROUP);
                            }
                        }

                    } else if (REFERS_TO.equals(character)) {

                        associationHolder.setCharacter(REFERS_TO);
                        associationHolder.setAssociationAttributeName(objectClassAndMemberAttributes[1].trim());
                        associationHolder.setReferenceName(ASSOC_ATTR_GROUP);
                    }
                }
            }
        } else {

            throw new InvalidAttributeValueException("Association pair syntax contains multiple delimiters \""
                    + "\"" + REFERS_TO
                    + "\" or \"" + OBTAINS + " \"");
        }

        associationHolder.setOmitFromSchema(omitFromSchema);

        if(LOG.isOk()){
            LOG.ok("The constructed Association Holder {0}. {1}", associationHolder, System.lineSeparator());
        }

        return associationHolder;
    }

    private Map<String, HashSet<AssociationHolder>> getAssociationHolders() {

        if (associationHolders != null && !associationHolders.isEmpty()) {

        } else {

            generateAssociationHolders();
        }

        return associationHolders;
    }

}
