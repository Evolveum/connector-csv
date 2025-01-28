package com.evolveum.polygon.connector.csv;

import com.evolveum.polygon.connector.csv.util.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.ArrayUtils;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.exceptions.*;
import org.identityconnectors.framework.common.objects.*;
import org.identityconnectors.framework.common.objects.filter.*;
import org.identityconnectors.framework.spi.SyncTokenResultsHandler;
import org.identityconnectors.framework.spi.operations.*;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.text.ParseException;
import java.util.*;

import static com.evolveum.polygon.connector.csv.util.Util.*;

/**
 * todo check new FileSystem().newWatchService() to create exclusive tmp file https://docs.oracle.com/javase/tutorial/essential/io/notification.html
 * <p>
 * Created by lazyman on 27/01/2017.
 */
public class ObjectClassHandler implements CreateOp, DeleteOp, TestOp, SearchOp<Filter>,
		UpdateAttributeValuesOp, AuthenticateOp, ResolveUsernameOp, SyncOp, DiscoverConfigurationOp {

	protected Map<String, HashSet<AssociationHolder>> associationHolders;
	protected Map<String, HashSet<String>> syncHook;


	public void validate() {
		configuration.validateAttributeNames();
	}
	private static final Log LOG = Log.getLog(ObjectClassHandler.class);


	private ObjectClassHandlerConfiguration configuration;
	private Map<ObjectClass, ObjectClassHandler> handlers;

	private Map<String, Column> header;

	public ObjectClassHandler(ObjectClassHandlerConfiguration configuration) {
		this.configuration = configuration;
	}

	public Map<String, Column> getHeader() {
		if (header == null) {
			this.header = initHeader(configuration.getFilePath());
		}
		return header;
	}

	private Map<String, Column> initHeader(File csvFile) {
		synchronized (CsvConnector.SYNCH_FILE_LOCK) {
			CSVFormat csv = createCsvFormat(configuration);
			try (Reader reader = createReader(csvFile, configuration)) {
				CSVParser parser = csv.parse(reader);
				Iterator<CSVRecord> iterator = parser.iterator();

				CSVRecord record = null;
				while (iterator.hasNext()) {
					record = iterator.next();
					if (!isRecordEmpty(record)) {
						break;
					}
				}

				if (record == null) {
					throw new ConfigurationException("Couldn't initialize headers, nothing in csv file for object class "
							+ configuration.getObjectClass());
				}

				return createHeader(record);
			} catch (IOException ex) {
				throw new ConnectorIOException("Couldn't initialize connector for object class "
						+ configuration.getObjectClass(), ex);
			}
		}
	}

	private String getAvailableAttributeName(Map<String, Column> header, String realName) {
		String availableName = realName;
		for (int i = 1; i <= header.size(); i++) {
			if (!header.containsKey(availableName)) {
				break;
			}

			availableName = realName + i;
		}

		return availableName;
	}

	private Map<String, Column> createHeader(CSVRecord record) {
		Map<String, Column> header = new HashMap<>();

		if (configuration.isHeaderExists()) {
			for (int i = 0; i < record.size(); i++) {
				String name = record.get(i);

				if (StringUtil.isEmpty(name)) {
					name = DEFAULT_COLUMN_NAME + 0;
				}


                if (name.startsWith(UTF8_BOM)) {

					name = name.substring(1);
				}

				String availableName = getAvailableAttributeName(header, name);
				header.put(availableName, new Column(name, i));
			}
		} else {
			// header doesn't exist, we just create col0...colN
			for (int i = 0; i < record.size(); i++) {
				header.put(DEFAULT_COLUMN_NAME + i, new Column(null, i));
			}
		}

		LOG.ok("Created header {0}", header);

		testHeader(header);

		return header;
	}

	private void testHeader(Map<String, Column> headers) {
		boolean uniqueFound = false;
		boolean passwordFound = false;

		for (String header : headers.keySet()) {
			if (header.equals(configuration.getUniqueAttribute())) {
				uniqueFound = true;
				continue;
			}

			if (header.equals(configuration.getPasswordAttribute())) {
				passwordFound = true;
				continue;
			}

			if (uniqueFound && passwordFound) {
				break;
			}
		}

		if (!uniqueFound) {
			throw new ConfigurationException("Header in csv file doesn't contain "
					+ "unique attribute name as defined in configuration.");
		}

		if (StringUtil.isNotEmpty(configuration.getPasswordAttribute()) && !passwordFound) {
			throw new ConfigurationException("Header in csv file doesn't contain "
					+ "password attribute name as defined in configuration.");
		}
	}

	public ObjectClass getObjectClass() {
		return configuration.getObjectClass();
	}

	public void schema(SchemaBuilder schema) {
		try {

			ObjectClassInfoBuilder objClassBuilder = new ObjectClassInfoBuilder();
			objClassBuilder.setType(getObjectClass().getObjectClassValue());
			objClassBuilder.setAuxiliary(configuration.isAuxiliary());
			objClassBuilder.setContainer(configuration.isContainer());
			objClassBuilder.addAllAttributeInfo(createAttributeInfo(getHeader()));

			schema.defineObjectClass(objClassBuilder.build());
		} catch (Exception ex) {
			handleGenericException(ex, "Couldn't initialize connector");
		}
	}

	private Set<AttributeInfo.Flags> createFlags(AttributeInfo.Flags... flags) {
		Set<AttributeInfo.Flags> set = new HashSet<>();
		set.addAll(Arrays.asList(flags));

		return set;
	}

	private List<AttributeInfo> createAttributeInfo(Map<String, Column> columns) {
		List<String> multivalueAttributes = getMultivaluedAttributes();
		List<AttributeInfo> infos = new ArrayList<>();

		if (!ArrayUtils.isEmpty(configuration.getManagedAssociationPairs())) {

			String objectClassName = getObjectClassName();

			HashSet<AssociationHolder> holders = null;
			for (String holderOcName :  associationHolders.keySet()){

				if(holderOcName.equalsIgnoreCase(objectClassName)){

					holders = associationHolders.get(holderOcName);
				}
			}
			AttributeInfoBuilder builder = null;
			Set<AttributeInfo> attributeInfos = new HashSet<>();

			if (holders != null && !holders.isEmpty()) {

				for (AssociationHolder holder : holders) {
					if(holder.isOmitFromSchema()){
						continue;
					}

					StringBuilder subTypeBuilder = new StringBuilder();
					String associationAttr = holder.getAssociationAttributeName();
					String evaluatedAttribute = null;

					if (associationAttr != null && !associationAttr.isEmpty()) {
						subTypeBuilder.append(associationAttr);
					} else {
						subTypeBuilder.append(configuration.getUniqueAttribute());
					}
					subTypeBuilder.append("-" + holder.getObjectObjectClassName());

					if (objectClassName.equals(holder.getSubjectObjectClassName())) {

						evaluatedAttribute = associationAttr;
						if(holder.isAccess()){
							builder = new AttributeInfoBuilder(ASSOC_ATTR_ACCESS, ConnectorObjectReference.class);
						} else {
							builder = new AttributeInfoBuilder(ASSOC_ATTR_GROUP, ConnectorObjectReference.class);
						}
						builder = new AttributeInfoBuilder(ASSOC_ATTR_GROUP, ConnectorObjectReference.class);
						builder.setCreateable(true);
						builder.setUpdateable(true);
						builder.setReadable(true);
						builder.setMultiValued(true);
						builder.setReturnedByDefault(true);

						builder.setRoleInReference(R_I_R_SUBJECT);
						builder.setReferencedObjectClassName(holder.getObjectObjectClassName());
						builder.setSubtype(subTypeBuilder.toString());

					} else {

						evaluatedAttribute = holder.getValueAttributeName();
						builder = new AttributeInfoBuilder(evaluatedAttribute, ConnectorObjectReference.class);
						builder.setType(String.class);
						builder.setReturnedByDefault(false);
						builder.setReadable(false);
					}

					if (evaluatedAttribute != null) {

						if (!configuration.getUniqueAttribute().equals(evaluatedAttribute)) {

							if (columns.containsKey(evaluatedAttribute)) {
								columns.remove(evaluatedAttribute);
								if (multivalueAttributes.contains(evaluatedAttribute)) {
									builder.setMultiValued(true);
								}
							} else {

								throw new ConfigurationException("Reference Attribute \"" + evaluatedAttribute + "\" not found " + "amongst attributes.");
							}
						}
					}
					attributeInfos.add(builder.build());
				}
				if (builder != null) {
					infos.addAll(attributeInfos);
				}
			}
		}

		for (String name : columns.keySet()) {
			if (name == null || name.isEmpty()) {
				continue;
			}

			if (name.equals(configuration.getUniqueAttribute())) {
				// unique column
				AttributeInfoBuilder builder = new AttributeInfoBuilder(Uid.NAME);
				builder.setType(String.class);
				builder.setNativeName(name);

				infos.add(builder.build());

				if (!isUniqueAndNameAttributeEqual()) {
					builder = new AttributeInfoBuilder(name);
					builder.setType(String.class);
					builder.setNativeName(name);
					builder.setRequired(true);

					infos.add(builder.build());

					continue;
				}
			}

			if (name.equals(configuration.getNameAttribute())) {
				AttributeInfoBuilder builder = new AttributeInfoBuilder(Name.NAME);
				builder.setType(String.class);
				builder.setNativeName(name);

				if (isUniqueAndNameAttributeEqual()) {
					builder.setRequired(true);
				}

				infos.add(builder.build());

				continue;
			}

			if (name.equals(configuration.getPasswordAttribute())) {
				AttributeInfoBuilder builder = new AttributeInfoBuilder(OperationalAttributes.PASSWORD_NAME);
				builder.setType(GuardedString.class);
				builder.setNativeName(name);

				infos.add(builder.build());

				continue;
			}

            if (name.equals(configuration.getLastLoginDateAttribute())) {
                AttributeInfoBuilder builder = new AttributeInfoBuilder(PredefinedAttributes.LAST_LOGIN_DATE_NAME);
                builder.setType(Long.class);
                builder.setNativeName(name);

                infos.add(builder.build());

                continue;
            }

			AttributeInfoBuilder builder = new AttributeInfoBuilder(name);
			if (name.equals(configuration.getPasswordAttribute())) {
				builder.setType(GuardedString.class);
			} else {
				builder.setType(String.class);
			}
			builder.setNativeName(name);
			if (multivalueAttributes.contains(name)) {
				builder.setMultiValued(true);
			}

			infos.add(builder.build());
		}

		return infos;
	}



	@Override
	public Uid authenticate(ObjectClass oc, String username, GuardedString password, OperationOptions oo) {
		return resolveUsername(username, password, oo, true);
	}

	@Override
	public Uid create(ObjectClass oc, Set<Attribute> set, OperationOptions oo) {
		if (configuration.isReadOnly()) {
			throw new ConnectorException("Can't add attribute values. Readonly set to true.");
		}

		Set<Attribute> attributes = normalize(set);

		String uidValue = findUidValue(attributes);
		Uid uid = new Uid(uidValue);

		FileLock lock = obtainTmpFileLock(configuration);
		Reader reader = null;
		Writer writer = null;
		boolean holderOfComplexRefObject = mayContainComplexReferenceObject();

		try {
			Set<ReferenceDataPayload> referenceDataPayload = new HashSet<>();

			synchronized (CsvConnector.SYNCH_FILE_LOCK) {
				reader = createReader(configuration);
				writer = new BufferedWriter(Channels.newWriter(lock.channel(), configuration.getEncoding()));

				CSVFormat csv = createCsvFormat(configuration);
				CSVParser parser = csv.parse(reader);

				csv = createCsvFormat(configuration);
				CSVPrinter printer = csv.print(writer);

				Iterator<CSVRecord> iterator = parser.iterator();
				// we don't want to skip header in any case, but if it's there just
				// write it to tmp file as "standard" record. We can't handle first row
				// as header in case there are more columns with the same name.
				if (configuration.isHeaderExists() && iterator.hasNext()) {
					CSVRecord record = iterator.next();
					printer.printRecord(record);
				}

				// handling real records
				while (iterator.hasNext()) {
					CSVRecord record = iterator.next();
					ConnectorObject obj = createConnectorObject(record, false);

					if (uid.equals(obj.getUid())) {
						throw new AlreadyExistsException("Account already exists '" + uid.getUidValue() + "'.");
					}

					printer.printRecord(record);
				}


				Set<ReferenceDataDeliveryVector> referenceDataDeliveryVector = null;

				Attribute referenceAttribute = null;
				if (!ArrayUtils.isEmpty(configuration.getManagedAssociationPairs())) {

					referenceAttribute = getAttributeByName(holderOfComplexRefObject ? ASSOC_ATTR_ACCESS : ASSOC_ATTR_GROUP
							, attributes);

					if (referenceAttribute != null) {
						referenceDataDeliveryVector = determineReferenceObjectDataDeliveryVectors(referenceAttribute,
								uidValue);
					}
				}

				if (referenceDataDeliveryVector != null) {
					referenceDataPayload = createReferenceDataFromAttributes(referenceDataDeliveryVector, attributes,
							referenceAttribute);
				}

				printer.printRecord(createNewRecord(attributes));

				writer.close();
				reader.close();

				moveTmpToOrig();
			}

			if(referenceDataPayload!=null){
			updateReferences(Operation.ADD_ATTR_VALUE, referenceDataPayload, false);
			}
		} catch (Exception ex) {
			handleGenericException(ex, "Error during account '" + uid + "' create");
		} finally {
			cleanupResources(writer, reader, lock, configuration);
		}

		return uid;
	}

	private boolean mayContainComplexReferenceObject() {

		if (!ArrayUtils.isEmpty(configuration.getManagedAssociationPairs())) {
			HashSet<AssociationHolder> holders = associationHolders.get(getObjectClassName());

			if(holders == null){

				return false;
			}

			for(AssociationHolder holder : holders){
				if (holder.getSubjectObjectClassName().equals(getObjectClassName())){
					if (holder.isAccess()){
						return true;
					}
				}
			}
		}

		return false;
	}

	private void updateReferences(Operation operation, Set<ReferenceDataPayload> referenceDataPayload, boolean isAccess ) {
		if (referenceDataPayload != null && !referenceDataPayload.isEmpty()) {
			for (ReferenceDataPayload dataPayload : referenceDataPayload) {

				ObjectClass objectClass = dataPayload.getConnectorObjectId().getObjectClass();
				String objectId = dataPayload.getConnectorObjectId().getId();

				if (!getObjectClass().equals(objectClass)) {
					Map<ObjectClass, ObjectClassHandler> handlers = getHandlers();
					ObjectClassHandler handler = handlers.get(objectClass);

					handler.update(operation, objectClass, new Uid(objectId),
							dataPayload.getAttributes(), null, isAccess);
				} else {

					update(operation, objectClass, new Uid(objectId),
							dataPayload.getAttributes(), null);
				}
			}
		}
	}

	private Set<ReferenceDataPayload> createReferenceDataFromAttributes(
			Set<ReferenceDataDeliveryVector> referenceDataDeliveryVectors, Set<Attribute> attributes,
			Attribute referenceAttribute) {
		return createReferenceDataFromAttributes(referenceDataDeliveryVectors, attributes, referenceAttribute,
				null, null);
	}

	private Set<ReferenceDataPayload> createReferenceDataFromAttributes(
			Set<ReferenceDataDeliveryVector> referenceDataDeliveryVectors, Set<Attribute> attributes,
			Attribute referenceAttribute, Operation operation, Uid uid) {

		Map<ConnectorObjectId, ReferenceDataPayload> referenceDataPayloadMap = new HashMap<>();
		Iterator<Attribute> attributeIterator = attributes.iterator();

		while (attributeIterator.hasNext()) {
			Attribute attribute = attributeIterator.next();

			if (ASSOC_ATTR_GROUP.equals(attribute.getName()) || ASSOC_ATTR_ACCESS.equals(attribute.getName())) {
				attributeIterator.remove();
				break;
			}
		}

		for (ReferenceDataDeliveryVector referenceDataDeliveryVector : referenceDataDeliveryVectors) {
			String valueAttributeName = referenceDataDeliveryVector.getIdAttributeName();

			Map<String, Set<Attribute>> referencedObjectdata = getReferenceData(valueAttributeName, referenceAttribute);

			if (referenceDataDeliveryVector.originIsRecipient()) {
				processOriginAsReferenceDataVector(referenceDataDeliveryVector, referencedObjectdata.keySet(),
						attributes);

			} else if (referenceDataDeliveryVector.isAccess() && operation != Operation.REMOVE_ATTR_VALUE) {

				processAccessAsReferenceDataVector(referenceDataDeliveryVector.getObjectClass(),
						referencedObjectdata, referenceDataPayloadMap);

			} else {
				processStandardReferenceDataVector(referenceDataDeliveryVector, attributes, uid,
						referencedObjectdata.keySet(), referenceDataPayloadMap);
			}
		}

		if (!referenceDataPayloadMap.isEmpty()) {

			return new HashSet<>(referenceDataPayloadMap.values());
		} else {
			return null;
		}
	}

	private void processOriginAsReferenceDataVector(ReferenceDataDeliveryVector referenceDataDeliveryVector,
													Set<String> referencedObjectdata, Set<Attribute> attributes) {

		AttributeBuilder attributeBuilder = new AttributeBuilder()
				.setName(referenceDataDeliveryVector.getAttributeName())
				.addValue(referencedObjectdata);

		attributes.add(attributeBuilder.build());
	}

	private void processAccessAsReferenceDataVector(ObjectClass referenceObjectClass,
													Map<String, Set<Attribute>> referencedObjectdata,
													Map<ConnectorObjectId, ReferenceDataPayload> referenceDataPayloadMap) {

		for (String id : referencedObjectdata.keySet()) {
			ConnectorObjectId connId = new ConnectorObjectId(id, referenceObjectClass);
			referenceDataPayloadMap.put(connId, new ReferenceDataPayload(connId, referencedObjectdata.get(id)));
		}
	}

	private void processStandardReferenceDataVector(ReferenceDataDeliveryVector referenceDataDeliveryVector,
													Set<Attribute> attributes, Uid uid,
													Set<String> referencedObjectdata, Map<ConnectorObjectId,
													ReferenceDataPayload> referenceDataPayloadMap) {
		List dataPayloadValue;
		Attribute attribute = getAttributeByName(referenceDataDeliveryVector.getIdAttributeName(), attributes);

		if (attribute != null) {

			dataPayloadValue = attribute.getValue();
		} else {

			if (!referenceDataDeliveryVector.originIsRecipient()) {
				if (configuration.getUniqueAttribute().equals(referenceDataDeliveryVector.getIdAttributeName())) {

					dataPayloadValue = Arrays.asList(uid.getUidValue());
					;
				} else {

					throw new ConnectorException("Invalid operation outcome in case of " +
							"determining reference attribute value. Subject Id attribute name is not the" +
							" configured unique id attribute.");
				}
			} else {

				throw new ConnectorException("Invalid operation outcome in case of " +
						"determining reference attribute value. Subject is determined as the reference " +
						"data recipient which would result in a non acyclic graph.");
			}
		}

		for (String id : referencedObjectdata) {

			ConnectorObjectId objectId = new ConnectorObjectId(id, referenceDataDeliveryVector.getObjectClass());

			if (referenceDataPayloadMap.containsKey(objectId)) {

				ReferenceDataPayload dataPayload = referenceDataPayloadMap.get(objectId);
				Set<Attribute> attributeSet = dataPayload.getAttributes();

				AttributeBuilder attributeBuilder = new AttributeBuilder().
						setName(referenceDataDeliveryVector.getAttributeName()).addValue(dataPayloadValue);
				attributeSet.add(attributeBuilder.build());

			} else {

				AttributeBuilder attributeBuilder = new AttributeBuilder().
						setName(referenceDataDeliveryVector.getAttributeName()).addValue(dataPayloadValue);
				Set<Attribute> attributeSet = new HashSet<>();
				attributeSet.add(attributeBuilder.build());
				referenceDataPayloadMap.put(objectId, new ReferenceDataPayload(objectId, attributeSet));
			}
		}
	}

	private Map<String, Set<Attribute> > getReferenceData(String valueAttributeName, Attribute referenceAttribute) {
		Map<String, Set<Attribute>> referenceData = new HashMap<>();
		List<Object> references = referenceAttribute.getValue();

		for (Object o : references) {

			if (o instanceof ConnectorObjectReference) {
				BaseConnectorObject bco = ((ConnectorObjectReference) o).getValue();
				ObjectClass objectClassOfReferencedObject = bco.getObjectClass();
				String uidAttrName;

				if (getObjectClass().equals(objectClassOfReferencedObject)) {
					uidAttrName = configuration.getUniqueAttribute();
				} else {

					ObjectClassHandler objectClassHandler = getHandlers().get(bco.getObjectClass());
					uidAttrName = objectClassHandler.configuration.getUniqueAttribute();
				}

				valueAttributeName = valueAttributeName.equals(uidAttrName) ? Uid.NAME : valueAttributeName;
				Attribute attribute = bco.getAttributeByName(valueAttributeName);

				List<Object> referenceValues = attribute.getValue();

				for (Object object : referenceValues) {
					if (object instanceof String) {

						referenceData.put((String) object, bco.getAttributes());
					}
				}
			}
		}
		return referenceData;
	}

	private Attribute getAttributeByName(String name, Set<Attribute> attributes) {

		try {
			Attribute attribute = attributes.stream()
					.filter(a -> name.equals(a.getName()))
					.findFirst()
					.get();
			return attribute;

		} catch (NoSuchElementException e) {

			return null;
		}
	}

	private void moveTmpToOrig() throws IOException {
		// moving existing file
		String path = configuration.getFilePath().getPath();
		File orig = new File(path);

		File tmp = createTmpPath(configuration);

		Files.move(tmp.toPath(), orig.toPath(), StandardCopyOption.REPLACE_EXISTING);
	}

	private boolean isPassword(String column) {
		return StringUtil.isNotEmpty(configuration.getPasswordAttribute())
				&& configuration.getPasswordAttribute().equals(column);
	}

	private boolean isUid(String column) {
		return configuration.getUniqueAttribute().equals(column);
	}

	private List<Object> createNewRecord(Set<Attribute> attributes) {
		final Object[] record = new Object[getHeader().size()];
		Attribute nameAttr = AttributeUtil.getNameFromAttributes(attributes);
		Object name = nameAttr != null ? AttributeUtil.getSingleValue(nameAttr) : null;

		Attribute uniqueAttr = AttributeUtil.find(configuration.getUniqueAttribute(), attributes);
		Object uid = uniqueAttr != null ? AttributeUtil.getSingleValue(uniqueAttr) : null;

		for (String column : getHeader().keySet()) {
			Object value;
			if (isPassword(column)) {
				Attribute attr = AttributeUtil.find(OperationalAttributes.PASSWORD_NAME, attributes);
				if (attr == null) {
					continue;
				}

				value = createRawValue(attr, configuration);
			} else if (isName(column)) {
				value = name;
			} else if (isUid(column)) {
				value = uid;
			} else {

				Attribute attr = AttributeUtil.find(column, attributes);
				if (attr == null) {
					continue;
				}

				value = createRawValue(attr, configuration);
			}

			record[getHeader().get(column).getIndex()] = value;
		}

		return Arrays.asList(record);
	}

	private Set<ReferenceDataDeliveryVector> determineReferenceObjectDataDeliveryVectors(Attribute referenceAttribute,
																						 String identifierValue) {
		Set<ReferenceDataDeliveryVector> setOfVectors = new HashSet<>();
		List<Object> objectList = referenceAttribute.getValue();
		Set<String> attributeNames;
		ObjectClass referencedOc = null;

		if (objectList !=null && objectList.size() > 0) {
			Object o = objectList.get(0);

			if (o instanceof ConnectorObjectReference) {
				BaseConnectorObject bco = ((ConnectorObjectReference) o).getValue();
				referencedOc = bco.getObjectClass();
				attributeNames = new HashSet<>();
				bco.getAttributes().stream().forEach(a -> {

					if (a.getValue().contains(identifierValue)) {

						attributeNames.add(a.getName());
					}
				});

			} else {
				throw new ConnectorException("Reference attribute " + referenceAttribute.getName() +
						" is of not supported object type.");
			}
		} else {
			attributeNames = null;
		}

		String currentObjectClassName = getObjectClassName();

		if (referencedOc != null) {
			if (associationHolders.containsKey(currentObjectClassName)) {
				HashSet<AssociationHolder> holders = associationHolders.get(currentObjectClassName);

				for (AssociationHolder holder : holders) {
					String objectObjectClassName = holder.getObjectObjectClassName();
					if (objectObjectClassName.equals(referencedOc.getObjectClassValue())) {

						ReferenceDataDeliveryVector referenceDataDeliveryVector = constructReferenceDataVector(
								referencedOc, holder, configuration.getUniqueAttribute(), currentObjectClassName,
								getHandlers(), attributeNames);

						if(referenceDataDeliveryVector != null){setOfVectors.add(referenceDataDeliveryVector);}
					}
				}
			}
		} else {
			throw new ConnectorException("The value of reference attribute " + referenceAttribute.getName() +
					" does not contain an object class.");
		}
		return setOfVectors;
	}

	private String findUidValue(Set<Attribute> attributes) {
		Attribute uniqueAttr = AttributeUtil.find(configuration.getUniqueAttribute(), attributes);
		Object uid = uniqueAttr != null ? AttributeUtil.getSingleValue(uniqueAttr) : null;

		if (uid == null) {
			throw new InvalidAttributeValueException("Unique attribute value not defined");
		}

		return uid.toString();
	}

	@Override
	public void delete(ObjectClass oc, Uid uid, OperationOptions oo) {
		if (configuration.isReadOnly()) {
			throw new ConnectorException("Can't add attribute values. Readonly set to true.");
		}

		update(Operation.DELETE, oc, uid, null, oo);
	}

	@Override
	public Uid resolveUsername(ObjectClass oc, String username, OperationOptions oo) {
		return resolveUsername(username, null, oo, false);
	}

	@Override
	public FilterTranslator<Filter> createFilterTranslator(ObjectClass oc, OperationOptions oo) {
		// Should never be reached, this is handled in the main connector code.
		throw new RuntimeException("Unreachable code reached");
	}

	private boolean skipRecord(CSVRecord record) {
		if (configuration.isHeaderExists() && record.getRecordNumber() == 1) {
			return true;
		}

		if (isRecordEmpty(record)) {
			return true;
		}

		return false;
	}

	@Override
	public void executeQuery(ObjectClass oc, Filter filter, ResultsHandler handler, OperationOptions oo) {
		CSVFormat csv = createCsvFormatReader(configuration);
		try (Reader reader = createReader(configuration)) {

			CSVParser parser = csv.parse(reader);
			boolean shouldReiterate = false;
			Iterator<CSVRecord> iterator = parser.iterator();

			HashMap <ConnectorObjectId, Set<ConnectorObjectCandidate>> candidates = new HashMap<>();
			while (iterator.hasNext()) {
				CSVRecord record = iterator.next();
				if (skipRecord(record)) {
					continue;
				}

				if (!ArrayUtils.isEmpty(configuration.getManagedAssociationPairs())) {
					Object ob = createConnectorObjectOrCandidateObject(record, false);
					if (ob instanceof ConnectorObjectCandidate) {
						ConnectorObjectId cid = ((ConnectorObjectCandidate) ob).getId();
						saturateCandidates(cid, candidates, (ConnectorObjectCandidate) ob);

						if (!shouldReiterate) {

							shouldReiterate = appendToCandidateMap((ConnectorObjectCandidate) ob, candidates, true);
						} else {

							appendToCandidateMap((ConnectorObjectCandidate) ob, candidates, true);
						}
					}
				} else {
					ConnectorObject obj = createConnectorObject(record);
					if (!handleQueriedObject(filter, obj, handler)) {
						break;
					}
				}
			}

			if (!ArrayUtils.isEmpty(configuration.getManagedAssociationPairs())) {
				if (shouldReiterate) {

					reIterateCandidates(candidates);
				}

				retrieveAssociationData(candidates);

				Set<ConnectorObjectCandidate> finalCandidateSet = new HashSet<>();
				candidates.values().forEach(val -> finalCandidateSet.addAll(val));

				for (ConnectorObjectCandidate candidate : finalCandidateSet) {

					candidate.evaluateDependencies();
					if (candidate.complete()) {
						if (!handleQueriedObject(filter, candidate.getCandidateBuilder().build(), handler)) {
							break;
						}
					}
				}
			}
		} catch (Exception ex) {
			handleGenericException(ex, "Error during query execution");
		}
	}

	private void saturateCandidates(ConnectorObjectId cid,
									HashMap<ConnectorObjectId, Set<ConnectorObjectCandidate>> candidates,
									ConnectorObjectCandidate ob) {

		if (candidates.containsKey(cid)) {
			Set<ConnectorObjectCandidate> candidatesSet = candidates.get(cid);
			Iterator candidateSetIterator = candidatesSet.iterator();

			while (candidateSetIterator.hasNext()) {
				ConnectorObjectCandidate candidate = (ConnectorObjectCandidate) candidateSetIterator.next();
				candidate.addCandidateUponWhichThisDepends(ob);
			}
		}

	}

	private boolean appendToCandidateMap(ConnectorObjectCandidate ob,
										 HashMap<ConnectorObjectId, Set<ConnectorObjectCandidate>> candidates) {

		return appendToCandidateMap(ob, candidates, false);
	}

	private boolean appendToCandidateMap(ConnectorObjectCandidate ob,
										 HashMap<ConnectorObjectId, Set<ConnectorObjectCandidate>> candidates,
										 boolean appendWithoutAssociatedObjects) {
		boolean shouldReiterate = false;

		Set<ConnectorObjectId> associatedObjectUIDs = (ob)
				.getObjectIdsToBeProcessed();

		if(associatedObjectUIDs.isEmpty() && appendWithoutAssociatedObjects) {

			Set<ConnectorObjectId> values = new HashSet<>();
			values.add(ob.getId());
			associatedObjectUIDs = values;
		}

		for (ConnectorObjectId auid : associatedObjectUIDs) {
			if(!shouldReiterate){

				if(getObjectClass().equals(auid.getObjectClass())){
					if(ob.getId()!=auid){

					shouldReiterate = true;
					}
				}
			}
			if (candidates.containsKey(auid)) {

				Set<ConnectorObjectCandidate> candidatesSet = candidates.get(auid);
				candidatesSet.add(ob);
			} else {

				Set<ConnectorObjectCandidate> candidatesSet = new HashSet<>();
				candidatesSet.add(ob);
				candidates.put(auid, candidatesSet);
			}
		}

		return shouldReiterate;
	}

	private void reIterateCandidates(HashMap<ConnectorObjectId, Set<ConnectorObjectCandidate>> candidatesToProcess) {
		reIterateCandidates(candidatesToProcess, null, null);
	}

	private void reIterateCandidates(HashMap<ConnectorObjectId, Set<ConnectorObjectCandidate>> candidatesToProcess,
									 HashMap<ConnectorObjectId, Set<ConnectorObjectCandidate>> originalCandidates,
									 Map<ConnectorObjectId, ConnectorObjectCandidate> processedAndReferencedCandidates) {
		CSVFormat csv = createCsvFormatReader(configuration);
		if (processedAndReferencedCandidates == null) {

			processedAndReferencedCandidates = new HashMap<>();
		}
		HashMap<ConnectorObjectId, Set<ConnectorObjectCandidate>> nonCompleteCandidates = new HashMap<>();
		try (Reader reader = createReader(configuration)) {

			CSVParser parser = csv.parse(reader);
			Iterator<CSVRecord> iterator = parser.iterator();

			while (iterator.hasNext()) {
				CSVRecord record = iterator.next();

				if (skipRecord(record)) {
					continue;
				}
				ConnectorObjectCandidate ob = createConnectorObjectOrCandidateObject(record, false);

				if (processedAndReferencedCandidates.containsKey(ob.getId())) {

					ob = processedAndReferencedCandidates.get(ob.getId());
				}

				if (candidatesToProcess.containsKey(ob.getId())) {
					Set<ConnectorObjectCandidate> candidatesSet = candidatesToProcess.get(ob.getId());
					Iterator candidateSetIterator = candidatesSet.iterator();

					while (candidateSetIterator.hasNext()) {
						ConnectorObjectCandidate candidate = (ConnectorObjectCandidate) candidateSetIterator.next();

						if (candidate.getId() == ob.getId()) {

							continue;
						}

						if (!candidate.getAlreadyProcessedObjectIds().contains(ob.getId())) {

							candidate.addCandidateUponWhichThisDepends(ob);

						}

						if (candidatesToProcess.containsKey(candidate.getId())) {

							processedAndReferencedCandidates.put(candidate.getId(), candidate);
						}
					}
				}
				if (!ob.complete()) {

					for (ConnectorObjectId objectId : ob.getObjectIdsToBeProcessed()) {
						if (candidatesToProcess.containsKey(objectId)) {
							Set<ConnectorObjectCandidate> candidateSet = candidatesToProcess.get(objectId);
							if (candidateSet.contains(ob)) {
								break;
							}
						}

						if (originalCandidates != null) {
							if (originalCandidates.containsKey(ob.getId()) ||
									originalCandidates.containsKey(objectId)) {
								continue;
							}
						}

						if (nonCompleteCandidates.containsKey(objectId)) {

							Set<ConnectorObjectCandidate> candidateSet = nonCompleteCandidates.get(objectId);
							candidateSet.add(ob);
						} else {

							Set<ConnectorObjectCandidate> candidateSet = new HashSet<>();
							candidateSet.add(ob);
							nonCompleteCandidates.put(objectId, candidateSet);
						}
					}
				}
			}

			if (!nonCompleteCandidates.isEmpty()) {

				for (ConnectorObjectId id : candidatesToProcess.keySet()) {
					if (nonCompleteCandidates.containsKey(id)) {
						nonCompleteCandidates.remove(id);
					}
				}
				if (!nonCompleteCandidates.isEmpty()) {

					reIterateCandidates(nonCompleteCandidates, candidatesToProcess,
							processedAndReferencedCandidates);
				}
			}

		} catch (IOException e) {
			handleGenericException(e, "Error during query execution, while re-iterating candidate objects");
		}
	}

	private boolean handleQueriedObject(Filter filter, ConnectorObject obj, ResultsHandler handler) {

		String uid = extractUidFromFilter(filter);

		if (uid == null) {
			if (filter == null || filter.accept(obj)) {
				if (!handler.handle(obj)) {
					return false;
				}
			}

			return true;
		}

		if (!uidMatches(uid, obj.getUid().getUidValue(), configuration.isIgnoreIdentifierCase())) {

			return true;
		}

		if (!handler.handle(obj)) {
			return false;
		}

		return true;
	}

	private ConnectorObjectCandidate createConnectorObjectOrCandidateObject(CSVRecord record, boolean omitAssociations) {

		return createConnectorObjectOrCandidateObject(record, omitAssociations, null, null);
	}


	private ConnectorObjectCandidate createConnectorObjectOrCandidateObject(CSVRecord record, boolean omitAssociations,
														  SyncToken syncToken, SyncDeltaType syncDeltaType) {

		String uid = "";

		String referenceName = ASSOC_ATTR_GROUP;
		Set<ConnectorObjectId> associationDataObject = new HashSet<>();
		Set<ConnectorObjectId> associationDataSubject = new HashSet<>();
		Set<ObjectClass> relationToObjectClasses = new HashSet<>();

		ConnectorObjectBuilder builder = new ConnectorObjectBuilder();
		builder.setObjectClass(getObjectClass());
		Map<Integer, String> header = reverseHeaderMap();

		if (header.size() != record.size()) {
			throw new ConnectorException("Number of columns in header (" + header.size()
					+ ") doesn't match number of columns for record (" + record.size()
					+ "). File row number: " + record.getRecordNumber());
		}

		for (int i = 0; i < record.size(); i++) {
			String name = header.get(i);
			String value = record.get(i);
			boolean addAttributeToBuilder = true;

			if (StringUtil.isEmpty(value)) {
				continue;
			}

			if (!ArrayUtils.isEmpty(configuration.getManagedAssociationPairs()) && !omitAssociations) {

				String objectClassName = getObjectClassName();

				Set<AssociationHolder> associationSet = associationHolders.get(objectClassName);
				if (associationSet != null && !associationSet.isEmpty()) {
					String analysedAttributeName;
					for (AssociationHolder holder : associationSet) {
						String objectObjectClassName = holder.getObjectObjectClassName();
						String subjectObjectClassName = holder.getSubjectObjectClassName();

						if(subjectObjectClassName.equals(getObjectClassName()) && holder.isAccess()){

							referenceName = ASSOC_ATTR_ACCESS;
						}

						if (AssociationCharacter.REFERS_TO.equals(holder.getCharacter())) {

							analysedAttributeName = holder.getValueAttributeName();
						} else {
							if (!objectClassName.equalsIgnoreCase(subjectObjectClassName)) {

								analysedAttributeName = holder.getValueAttributeName();
							} else {

								analysedAttributeName = holder.getAssociationAttributeName();
							}
						}

						if (name.equalsIgnoreCase(analysedAttributeName)) {
							if (objectClassName.equalsIgnoreCase(subjectObjectClassName)) {

								if (analysedAttributeName.equals(configuration.getUniqueAttribute())) {

									relationToObjectClasses.add(Util.getObjectClass(objectObjectClassName));

									continue;
								}
							}

							List<String> attrValues = createAttributeValues(value);
							Iterator<String> iterator = attrValues.iterator();

							while (iterator.hasNext()) {

								if (subjectObjectClassName.equalsIgnoreCase(objectClassName)) {

									associationDataObject.add(new ConnectorObjectId(iterator.next(),
											Util.getObjectClass(objectObjectClassName)));

									addAttributeToBuilder = false;
								} else {
									associationDataSubject.add(new ConnectorObjectId(iterator.next(),
											Util.getObjectClass(subjectObjectClassName)));

									addAttributeToBuilder = true;
								}
							}
						}
					}
				}
			}

			if (name.equals(configuration.getUniqueAttribute())) {
				builder.setUid(value);
				uid = value;
				if (!isUniqueAndNameAttributeEqual()) {
					continue;
				}
			}

			if (name.equals(configuration.getNameAttribute())) {
				builder.setName(new Name(value));
				continue;
			}

			if (name.equals(configuration.getPasswordAttribute())) {
				builder.addAttribute(OperationalAttributes.PASSWORD_NAME, new GuardedString(value.toCharArray()));
				continue;
			}

			if (name.equals(configuration.getLastLoginDateAttribute())) {
				builder.addAttribute(PredefinedAttributes.LAST_LOGIN_DATE_NAME, createLastLoginDateValue(value));
				continue;
			}

			if(addAttributeToBuilder) {

				builder.addAttribute(name, createAttributeValues(value));
			}
		}

		ConnectorObjectId cid = new ConnectorObjectId(uid, getObjectClass(), relationToObjectClasses);

		if (syncDeltaType != null) {
			return new SyncDeltaObjectCandidate(cid,
					builder, associationDataObject, associationDataSubject, syncToken, syncDeltaType, referenceName);
		} else {
			return new ConnectorObjectCandidate(cid,
					builder, associationDataObject, associationDataSubject, referenceName);
		}
	}

	private String extractUidFromFilter(Filter filter) {
        if (!(filter instanceof EqualsFilter eq)) {
			return null;
		}
        if (Uid.NAME.equals(eq.getName())) {
            List<Object> values = eq.getAttribute().getValue();
			if (values == null || values.isEmpty()) {
				return null;
			}
			if (values.size() > 1) {
				throw new IllegalArgumentException("Illegal search filter for CSV connector, requesting multiple UID values");
			}
			return (String) values.get(0);
		} else {
			return null;
		}
	}

	private boolean uidMatches(String uid1, String uid2, boolean ignoreCase) {
		return uid1.equals(uid2) || ignoreCase && uid1.equalsIgnoreCase(uid2);
	}

	private void validateAuthenticationInputs(String username, GuardedString password, boolean authenticate) {
		if (StringUtil.isEmpty(username)) {
			throw new InvalidCredentialException("Username must not be empty");
		}

		if (authenticate && StringUtil.isEmpty(configuration.getPasswordAttribute())) {
			throw new ConfigurationException("Password attribute not defined in configuration");
		}

		if (authenticate && password == null) {
			throw new InvalidPasswordException("Password is not defined");
		}
	}

	private Uid resolveUsername(String username, GuardedString password, OperationOptions oo, boolean authenticate) {
		validateAuthenticationInputs(username, password, authenticate);

		CSVFormat csv = createCsvFormatReader(configuration);
		try (Reader reader = createReader(configuration)) {

			ConnectorObject object = null;

			CSVParser parser = csv.parse(reader);
			Iterator<CSVRecord> iterator = parser.iterator();
			while (iterator.hasNext()) {
				CSVRecord record = iterator.next();
				if (skipRecord(record)) {
					continue;
				}

				ConnectorObject obj = createConnectorObject(record, false);

				Name name = obj.getName();
				if (name != null && username.equals(AttributeUtil.getStringValue(name))) {
					object = obj;
					break;
				}
			}

			if (object == null) {
				String message = authenticate ? "Invalid username and/or password" : "Invalid username";
				throw new InvalidCredentialException(message);
			}

			if (authenticate) {
				authenticate(username, password, object);
			}

			Uid uid = object.getUid();
			if (uid == null) {
				throw new UnknownUidException("Unique attribute doesn't have value for account '" + username + "'");
			}

			return uid;
		} catch (Exception ex) {
			handleGenericException(ex, "Error during authentication");
		}

		return null;
	}

	private void authenticate(String username, GuardedString password, ConnectorObject foundObject) {
		GuardedString objPassword = AttributeUtil.getPasswordValue(foundObject.getAttributes());
		if (objPassword == null) {
			throw new InvalidPasswordException("Password not defined for username '" + username + "'");
		}

		// we don't want to authenticate against empty password
		StringAccessor acc = new StringAccessor();
		objPassword.access(acc);
		if (StringUtil.isEmpty(acc.getValue())) {
			throw new InvalidPasswordException("Password not defined for username '" + username + "'");
		}

		if (!objPassword.equals(password)) {
			throw new InvalidPasswordException("Invalid username and/or password");
		}
	}

	private void handleJustNewToken(SyncToken token, SyncResultsHandler handler) {
		if (!(handler instanceof SyncTokenResultsHandler)) {
			return;
		}

		SyncTokenResultsHandler tokenHandler = (SyncTokenResultsHandler) handler;
		tokenHandler.handleResult(token);
	}

	@Override
	public void sync(ObjectClass oc, SyncToken token, SyncResultsHandler handler, OperationOptions oo) {
		File syncLockFile = createSyncLockFile(configuration);
		FileLock lock = obtainTmpFileLock(syncLockFile);
		try {
			long tokenLongValue = getTokenValue(token);
			LOG.info("Token {0}", tokenLongValue);

			if (tokenLongValue == -1) {
				//token doesn't exist, we only create new sync file - we're synchronizing from now on
				String newToken = createNewSyncFile();
				handleJustNewToken(new SyncToken(newToken), handler);
				LOG.info("Token value was not defined {0}, only creating new sync file, synchronizing from now on.", token);
				return;
			}

			doSync(tokenLongValue, handler);
		} finally {
			closeQuietly(lock);
			syncLockFile.delete();
		}
	}

	private File findOldCsv(long token, String newToken, SyncResultsHandler handler) {
		File oldCsv = createSyncFileName(token, configuration);
		if (!oldCsv.exists()) {
			// we'll try to find first sync file which is newer than token (there's a possibility
			// that we loose some changes this way - same as for example ldap)
			oldCsv = findOldestSyncFile(token, configuration);
			if (oldCsv == null || oldCsv.equals(createSyncFileName(Long.parseLong(newToken), configuration))) {
				// we didn't found any newer file, we should stop and handle this situation as if this
				// is first time we're doing sync operation (like getLatestSyncToken())
				handleJustNewToken(new SyncToken(newToken), handler);
				LOG.info("File for token wasn't found, sync will stop, new token {0} will be returned.", token);
				return null;
			}
		}

		return oldCsv;
	}

	private void doSync(long token, SyncResultsHandler handler) {
		String newToken = createNewSyncFile();
		SyncToken newSyncToken = new SyncToken(newToken);

		File newCsv = createSyncFileName(Long.parseLong(newToken), configuration);

		Integer uidIndex = getHeader().get(configuration.getUniqueAttribute()).getIndex();

		File oldCsv = findOldCsv(token, newToken, handler);
		if (oldCsv == null) {
			LOG.error("Couldn't find old csv file to create diff, finishing synchronization.");
			return;
		}

		LOG.ok("Comparing files. Old {0} (exists: {1}, size: {2}) with new {3} (exists: {4}, size: {5})",
				oldCsv.getName(), oldCsv.exists(), oldCsv.length(), newCsv.getName(), newCsv.exists(), newCsv.length());

		try (Reader reader = createReader(newCsv, configuration)) {
			Map<String, CSVRecord> oldData = loadOldSyncFile(oldCsv);

			Set<String> oldUsedOids = new HashSet<>();

			CSVFormat csv = createCsvFormatReader(configuration);

			CSVParser parser = csv.parse(reader);
			Iterator<CSVRecord> iterator = parser.iterator();

			int changesCount = 0;

			boolean shouldContinue = true;
			boolean shouldReiterate = false;
			HashMap <ConnectorObjectId, Set<ConnectorObjectCandidate>> candidates = new HashMap<>();

			while (iterator.hasNext()) {
				CSVRecord record = iterator.next();
				if (skipRecord(record)) {
					continue;
				}

				String uid = record.get(uidIndex);
				if (StringUtil.isEmpty(uid)) {
					throw new ConnectorException("Unique attribute not defined for record number "
							+ record.getRecordNumber() + " in " + newCsv.getName());
				}

				if (!ArrayUtils.isEmpty(configuration.getManagedAssociationPairs()) &&
						associationHolders.containsKey(getObjectClassName())) {

					SyncDeltaObjectCandidate syncDeltaObjectCandidate = fetchCandidateForSyncDelta(record, uid, oldData,
							oldUsedOids, newSyncToken);

					if (syncDeltaObjectCandidate == null) {
						continue;
					}

					ConnectorObjectId cid = syncDeltaObjectCandidate.getId();
					saturateCandidates(cid, candidates, syncDeltaObjectCandidate);

					if (!shouldReiterate) {

						shouldReiterate = appendToCandidateMap(syncDeltaObjectCandidate, candidates,
								true);
					} else {

						appendToCandidateMap(syncDeltaObjectCandidate, candidates, true);
					}

				} else {
					SyncDelta delta = doSyncCreateOrUpdate(record, uid, oldData, oldUsedOids, newSyncToken);

					if (delta == null) {
						continue;
					}

					changesCount++;
					shouldContinue = handler.handle(delta);
					if (!shouldContinue) {
						break;
					}
				}
			}

			if(!candidates.isEmpty()){

				if(shouldReiterate){

					reIterateCandidates(candidates);
				}
				retrieveAssociationData(candidates);
				Set<ConnectorObjectCandidate> finalCandidateSet = new HashSet<>();
				candidates.values().forEach(val -> finalCandidateSet.addAll(val));

				for (ConnectorObjectCandidate candidate : finalCandidateSet) {

					candidate.evaluateDependencies();
					if (candidate.complete()) {
					SyncDeltaObjectCandidate cd = (SyncDeltaObjectCandidate) candidate;

						SyncDeltaBuilder builder = new SyncDeltaBuilder();
						builder.setDeltaType(cd.getSyncDeltaType());
						builder.setObjectClass(getObjectClass());
						builder.setToken(cd.getSyncToken());
						builder.setObject(cd.getCandidateBuilder().build());

						changesCount++;
						shouldContinue = handler.handle(builder.build());
						if (!shouldContinue) {
							break;
						}
					} else {

						candidate.evaluateDependencies();
					}
				}
			}

			if (shouldContinue) {
				changesCount += doSyncDeleted(oldData, oldUsedOids, newSyncToken, handler);
			}

			if (changesCount == 0) {
				handleJustNewToken(new SyncToken(newToken), handler);
			}
		} catch (Exception ex) {
			handleGenericException(ex, "Error during synchronization");
		} finally {
			cleanupOldSyncFiles();
		}
	}

	private void evaluateAssociationData(CSVRecord oldRecord, CSVRecord newRecord) {

		String ocName = getObjectClassName();
		Set<AssociationHolder> associationHoldersPerOC = associationHolders.get(ocName);
		Map<String, Set<String>> valueAttributes = new HashMap<>();

		for (AssociationHolder holder : associationHoldersPerOC) {
			if (ocName.equalsIgnoreCase(holder.getObjectObjectClassName())) {

				String subjectOcName = holder.getSubjectObjectClassName();

				if (valueAttributes.containsKey(subjectOcName)) {
					Set<String> attrSet = valueAttributes.get(subjectOcName);
					attrSet.add(holder.getValueAttributeName());
					valueAttributes.put(subjectOcName, attrSet);
				} else {

					HashSet valueSet = new HashSet();
					valueSet.add(holder.getValueAttributeName());
					valueAttributes.put(subjectOcName, valueSet);
				}
			}
		}

		if (valueAttributes.isEmpty()) {
			return;
		}

		Map<Integer, String> header = reverseHeaderMap();
		for (int i = 0; i < newRecord.size(); i++) {
			String name = header.get(i);

			for (String subjectOc : valueAttributes.keySet()) {
				Set<String> valueAttrNames = valueAttributes.get(subjectOc);
				if (valueAttrNames.contains(name)) {

					String oldValue;
					ArrayList<String> valuesOld = new ArrayList<>();

					if (oldRecord != null) {
						oldValue = oldRecord.get(i);
						valuesOld = (ArrayList<String>) createAttributeValues(oldValue);
						Collections.sort(valuesOld);
					}

					String newValue = newRecord.get(i);
					ArrayList<String> valuesNew = (ArrayList<String>) createAttributeValues(newValue);
					Collections.sort(valuesNew);

					if (valuesOld.equals(valuesNew)) {
						continue;
					}

					ArrayList<String> tmpList = new ArrayList<>();
					tmpList.addAll(valuesNew);
					tmpList.removeAll(valuesOld);

					if (tmpList != null && !tmpList.isEmpty()) {

						updateSyncHook(subjectOc, tmpList);
					}

					tmpList.clear();
					tmpList.addAll(valuesOld);
					tmpList.removeAll(valuesNew);

					if (tmpList != null && !tmpList.isEmpty()) {

						updateSyncHook(subjectOc, tmpList);
					}
				}
			}
		}
	}

	private void updateSyncHook(String objectClassName, List<String> values ) {

		if (syncHook.containsKey(objectClassName)) {

			HashSet<String> idValues = syncHook.get(objectClassName);
			idValues.addAll(values);
			syncHook.put(objectClassName, idValues);
		} else {

			HashSet<String> idValues = new HashSet<>();
			idValues.addAll(values);
			syncHook.put(objectClassName, idValues);
		}
	}

	private Map<String, CSVRecord> loadOldSyncFile(File oldCsv) {
		Map<String, Column> header = initHeader(oldCsv);
		if (!this.getHeader().equals(header)) {
			throw new ConnectorException("Headers of sync file '" + oldCsv + "' and current csv don't match");
		}

		Integer uidIndex = header.get(configuration.getUniqueAttribute()).getIndex();

		Map<String, CSVRecord> oldData = new HashMap<>();

		CSVFormat csv = createCsvFormatReader(configuration);
		try (Reader reader = createReader(oldCsv, configuration)) {
			CSVParser parser = csv.parse(reader);
			Iterator<CSVRecord> iterator = parser.iterator();
			while (iterator.hasNext()) {
				CSVRecord record = iterator.next();
				if (skipRecord(record)) {
					continue;
				}

				String uid = record.get(uidIndex);
				if (StringUtil.isEmpty(uid)) {
					throw new ConnectorException("Unique attribute not defined for record number "
							+ record.getRecordNumber() + " in " + oldCsv.getName());
				}

				if (oldData.containsKey(uid)) {
					throw new ConnectorException("Unique attribute value '" + uid + "' is not unique in "
							+ oldCsv.getName());
				}

				oldData.put(uid, record);
			}
		} catch (Exception ex) {
			handleGenericException(ex, "Error during query execution");
		}

		return oldData;
	}

	private void cleanupOldSyncFiles() {
		String[] tokenFiles = listTokenFiles(configuration);
		Arrays.sort(tokenFiles);

		int preserve = configuration.getPreserveOldSyncFiles();
		if (preserve <= 1) {
			LOG.info("Not removing old token files. Preserve last tokens: {0}.", preserve);
			return;
		}

		File parentFolder = configuration.getTmpFolder();
		for (int i = 0; i + preserve < tokenFiles.length; i++) {
			File tokenSyncFile = new File(parentFolder, tokenFiles[i]);
			if (!tokenSyncFile.exists()) {
				continue;
			}

			LOG.info("Deleting file {0}.", tokenSyncFile.getName());
			tokenSyncFile.delete();
		}
	}

	private SyncDeltaType isCreateOrUpdateDelta(CSVRecord newRecord, String newRecordUid, Map<String, CSVRecord> oldData,
												Set<String> oldUsedOids, SyncToken newSyncToken){

		CSVRecord oldRecord = oldData.get(newRecordUid);
		if (oldRecord == null) {

			return SyncDeltaType.CREATE;
		} else {
			boolean buildDelta = false;
			oldUsedOids.add(newRecordUid);

			// this will be an update if records aren't equal
			List old = copyOf(oldRecord.iterator());
			List _new = copyOf(newRecord.iterator());

			if(syncHook!=null && !syncHook.isEmpty()) {
				if (syncHook.containsKey(getObjectClassName())) {

					HashSet<String> objectsToSync = syncHook.get(getObjectClassName());
					if (objectsToSync.contains(newRecordUid)) {

						objectsToSync.remove(newRecordUid);
						buildDelta = true;
					}
				}
			}

			if (old.equals(_new) && !buildDelta) {
				// record are equal, no update
				return null;
			}

			return SyncDeltaType.UPDATE;
		}
	}

	private SyncDeltaObjectCandidate fetchCandidateForSyncDelta(CSVRecord newRecord, String newRecordUid,
																Map<String, CSVRecord> oldData, Set<String> oldUsedOids,
																SyncToken newSyncToken) {

		SyncDeltaType deltaType
				= isCreateOrUpdateDelta(newRecord, newRecordUid, oldData, oldUsedOids, newSyncToken);

		if (deltaType == null) {
			return null;

		} else {
			SyncDeltaObjectCandidate deltaCandidate = buildSyncDeltaCandidate(deltaType, newSyncToken, newRecord);

			if (associationHolders != null && !associationHolders.isEmpty()) {

				if(associationHolders.containsKey(getObjectClassName())){
					CSVRecord oldRecord = oldData.get(newRecordUid);
					evaluateAssociationData(oldRecord, newRecord);
				}
			}

			return deltaCandidate;
		}
	}

	private SyncDelta doSyncCreateOrUpdate(CSVRecord newRecord, String newRecordUid, Map<String, CSVRecord> oldData,
										   Set<String> oldUsedOids, SyncToken newSyncToken) {

		SyncDeltaType deltaType
				= isCreateOrUpdateDelta(newRecord, newRecordUid, oldData, oldUsedOids, newSyncToken);

		if(deltaType == null){
			return null;
		} else {
			SyncDelta delta = buildSyncDelta(deltaType, newSyncToken, newRecord);
			LOG.ok("Created delta {0}", delta);

			return delta;
		}
	}

	private int doSyncDeleted(Map<String, CSVRecord> oldData, Set<String> oldUsedOids, SyncToken newSyncToken,
							  SyncResultsHandler handler) {

		int changesCount = 0;

		for (String oldUid : oldData.keySet()) {
			if (oldUsedOids.contains(oldUid)) {
				continue;
			}

			// deleted record
			CSVRecord deleted = oldData.get(oldUid);
			SyncDelta delta = buildSyncDelta(SyncDeltaType.DELETE, newSyncToken, deleted);

			LOG.ok("Created delta {0}", delta);
			changesCount++;

			if (!handler.handle(delta)) {
				break;
			}
		}

		return changesCount;
	}

	private SyncDelta buildSyncDelta(SyncDeltaType type, SyncToken token, CSVRecord record){
		SyncDeltaBuilder builder = new SyncDeltaBuilder();
		builder.setDeltaType(type);
		builder.setObjectClass(getObjectClass());
		builder.setToken(token);

		ConnectorObject object = createConnectorObject(record);
		builder.setObject(object);

		return builder.build();
	}

	private SyncDeltaObjectCandidate buildSyncDeltaCandidate(SyncDeltaType type, SyncToken token, CSVRecord record) {

			return (SyncDeltaObjectCandidate) createConnectorObjectOrCandidateObject(record, false,
					token, type);
	}

	private long getTokenValue(SyncToken token) {
		if (token == null || token.getValue() == null) {
			return -1;
		}
		String object = token.getValue().toString();
		if (!object.matches("[0-9]{13}")) {
			return -1;
		}

		return Long.parseLong(object);
	}

	private String createNewSyncFile() {
		long timestamp = System.currentTimeMillis();

		String token = null;
		try {
			File real = configuration.getFilePath();

			File last = createSyncFileName(timestamp, configuration);

			LOG.info("Creating new sync file {0} file {1}", timestamp, last.getName());
			Files.copy(real.toPath(), last.toPath(), StandardCopyOption.REPLACE_EXISTING);
			LOG.ok("New sync file created, name {0}, size {1}", last.getName(), last.length());

			token = Long.toString(timestamp);
		} catch (IOException ex) {
			handleGenericException(ex, "Error occurred while creating new sync file " + timestamp);
		}

		return token;
	}

	@Override
	public SyncToken getLatestSyncToken(ObjectClass oc) {
		String token;
		LOG.info("Creating token, synchronizing from \"now\".");
		token = createNewSyncFile();

		return new SyncToken(token);
	}

	@Override
	public void test() {
		configuration.validate();

		initHeader(configuration.getFilePath());
	}

	@Override
	public void testPartialConfiguration() {
		configuration.validateCsvFile();
	}

	@Override
	public Map<String, SuggestedValues> discoverConfiguration() {
		Map<String, SuggestedValues> suggestions = new HashMap<>();
		ConfigurationDetector detector = new ConfigurationDetector(configuration);

		suggestions.putAll(detector.detectDelimiters());
		suggestions.putAll(detector.detectAttributes());

		return suggestions;
	}

	@Override
	public Uid addAttributeValues(ObjectClass oc, Uid uid, Set<Attribute> set, OperationOptions oo) {
		if (configuration.isReadOnly()) {
			throw new ConnectorException("Can't add attribute values. Readonly set to true.");
		}

		return update(Operation.ADD_ATTR_VALUE, oc, uid, set, oo);
	}

	@Override
	public Uid removeAttributeValues(ObjectClass oc, Uid uid, Set<Attribute> set, OperationOptions oo) {
		if (configuration.isReadOnly()) {
			throw new ConnectorException("Can't add attribute values. Readonly set to true.");
		}

		return update(Operation.REMOVE_ATTR_VALUE, oc, uid, set, oo);
	}

	@Override
	public Uid update(ObjectClass oc, Uid uid, Set<Attribute> set, OperationOptions oo) {
		if (configuration.isReadOnly()) {
			throw new ConnectorException("Can't add attribute values. Readonly set to true.");
		}

		return update(Operation.UPDATE, oc, uid, set, oo);
	}

	private boolean isRecordEmpty(CSVRecord record) {
		if (!configuration.isIgnoreEmptyLines()) {
			return false;
		}

		for (int i = 0; i < record.size(); i++) {
			String value = record.get(i);
			if (StringUtil.isNotBlank(value)) {
				return false;
			}
		}

		return true;
	}

	private Map<Integer, String> reverseHeaderMap() {
		Map<Integer, String> reversed = new HashMap<>();
		this.getHeader().forEach((key, value) -> {

			reversed.put(value.getIndex(), key);
		});

		return reversed;
	}

	private ConnectorObject createConnectorObject(CSVRecord record , boolean omitAssociations) {

		ConnectorObjectCandidate cc =  createConnectorObjectOrCandidateObject(record,
				omitAssociations);

		return cc.getCandidateBuilder().build();
	}

	public ConnectorObject createConnectorObject(CSVRecord record) {

		return createConnectorObject(record, true);
	}

	public String getObjectClassName() {

		return Util.getObjectClassName(getObjectClass());
	}
//	private void retrieveAssociationData(HashMap <ConnectorObjectId, Set<ConnectorObjectCandidate>>  candidates) {
//
//		retrieveAssociationData(candidates, 0);
//	}

	private void retrieveAssociationData(HashMap <ConnectorObjectId, Set<ConnectorObjectCandidate>>  candidates) {
// TODO # A refactor
		Set<AssociationHolder> associationSet = associationHolders.get(getObjectClassName());

		if (associationSet == null) {

			return;
		}

		Map<ObjectClass, Set<String>> valuesPerObjectClass = new HashMap<>();
		String objectClass = getObjectClassName();
		Map<ObjectClass, ObjectClassHandler> availableHandlers;
		Map<ObjectClassHandler, Set<CSVRecord>> recordSet = new HashMap<>();
		Map<String, Set<AssociationHolder>> objectClassAndAttrs = null;

		for (ConnectorObjectId id : candidates.keySet()) {
			Set<ObjectClass> relatedObjectClasses = id.getRelatedObjectClasses();

			if (relatedObjectClasses != null) {
				for (ObjectClass relatedObjectClass : relatedObjectClasses) {

					if (valuesPerObjectClass.containsKey(relatedObjectClass)) {
						Set<String> values = valuesPerObjectClass.get(relatedObjectClass);
						values.add(id.getId());
					} else {
						Set<String> values = new HashSet<>();
						values.add(id.getId());
						valuesPerObjectClass.put(relatedObjectClass, values);
					}
				}
			} else {

				if (valuesPerObjectClass.containsKey(id.getObjectClass())) {
					Set<String> values = valuesPerObjectClass.get(id.getObjectClass());
					values.add(id.getId());
				} else {
					Set<String> values = new HashSet<>();
					values.add(id.getId());
					valuesPerObjectClass.put(id.getObjectClass(), values);
				}
			}
		}

		if(valuesPerObjectClass.isEmpty()){

			return;
		}

		for (AssociationHolder holder : associationSet) {

			String objectObjectCLassName = holder.getObjectObjectClassName();
			if (objectObjectCLassName.equalsIgnoreCase(objectClass)) {

				continue;
			}

			if (objectClassAndAttrs != null) {

				Set<AssociationHolder> holders = objectClassAndAttrs.get(objectObjectCLassName);
				if (holders != null) {

					holders.add(holder);
				} else {

					holders = new HashSet<>();
					holders.add(holder);
				}
			} else {

				objectClassAndAttrs = new HashMap<>();
				Set<AssociationHolder> holders = new HashSet<>();
				holders.add(holder);
				objectClassAndAttrs.put(objectObjectCLassName, holders);
			}
		}
		if (objectClassAndAttrs != null) {

			availableHandlers = getHandlers();

			for (String objectClassName : objectClassAndAttrs.keySet()) {

				Set<AssociationHolder> holders = objectClassAndAttrs.get(objectClassName);
				Map<String, Set<String>> attrsAndValues = new HashMap<>();
				ObjectClass evaluatedObjectClass;

				evaluatedObjectClass = Util.getObjectClass(objectClassName);

				for (AssociationHolder associationHolder : holders) {

					if (AssociationCharacter.REFERS_TO.equals(associationHolder.getCharacter())) {

						attrsAndValues.put(associationHolder.getAssociationAttributeName(),
								valuesPerObjectClass.get(evaluatedObjectClass));
					} else {

						attrsAndValues.put(associationHolder.getValueAttributeName(),
								valuesPerObjectClass.get(evaluatedObjectClass));
					}
				}

				ObjectClassHandler associatedObjectClassHandler =
						availableHandlers.get(Util.getObjectClass(objectClassName));

				Set<CSVRecord> records = associatedObjectClassHandler.fetchCSVRecords(attrsAndValues);

				if (records != null && !records.isEmpty()) {
					if (recordSet.containsKey(associatedObjectClassHandler)) {

						Set<CSVRecord> oldRcords = recordSet.get(associatedObjectClassHandler);
						oldRcords.addAll(records);
					} else {
						recordSet.put(associatedObjectClassHandler, records);
					}
				}
			}
		}

		if (recordSet != null && !recordSet.isEmpty()) {
			for (ObjectClassHandler objectClassHandler : recordSet.keySet()) {

				Set<CSVRecord> records = recordSet.get(objectClassHandler);
				HashMap<ConnectorObjectId, Set<ConnectorObjectCandidate>> retrievedCandidates = new HashMap<>();
				boolean shouldReiterate = false;
				for (CSVRecord record : records) {
					ConnectorObjectCandidate oc;

						oc = objectClassHandler.createConnectorObjectOrCandidateObject(record,
								false);

					ConnectorObjectId cid = oc.getId();
					objectClassHandler.saturateCandidates(cid, retrievedCandidates, oc);

					if (!shouldReiterate) {

						shouldReiterate = objectClassHandler.appendToCandidateMap(oc, retrievedCandidates,
								true);
					} else {

						objectClassHandler.appendToCandidateMap(oc, retrievedCandidates,
								true);
					}
				}

				if (shouldReiterate) {

					objectClassHandler.reIterateCandidates(retrievedCandidates);
				}

				objectClassHandler.retrieveAssociationData(retrievedCandidates);

				Set<ConnectorObjectCandidate> finalCandidateSet = new HashSet<>();
				retrievedCandidates.values().forEach(val -> finalCandidateSet.addAll(val));
				for (ConnectorObjectCandidate candidate : finalCandidateSet) {

					candidate.evaluateDependencies();
					if (candidate.complete()) {
						if (candidates.containsKey(candidate.getId())) {
							Set<ConnectorObjectCandidate> candidateSet = candidates.get(candidate.getId());

							for (ConnectorObjectCandidate candidateFromSet : candidateSet) {

								candidateFromSet.addCandidateUponWhichThisDepends(candidate);
							}

						} else if (!candidate.getSubjectIdsToBeProcessed().isEmpty()) {

							Set<ConnectorObjectId> idSet = candidate.getSubjectIdsToBeProcessed();
							for (ConnectorObjectId id : idSet) {

								if (candidates.containsKey(id)) {
									Set<ConnectorObjectCandidate> candidateSet = candidates.get(id);

									for (ConnectorObjectCandidate candidateFromSet : candidateSet) {

										candidateFromSet.addCandidateUponWhichThisDepends(candidate);
									}
								}
							}
						}
					} else {

						throw new ConnectorException("Invalid operation outcome, Connector Object Candidate '"+
								candidate.getId()+"' ");
					}
				}
			}
		}
	}

    private Long createLastLoginDateValue(String value) {
        if (StringUtil.isEmpty(value)) {
            return null;
        }

        if (configuration.getLastLoginDateFormat() == null) {
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException ex) {
                throw new InvalidAttributeValueException("Value " + value + " for last login date ("
                        + configuration.getLastLoginDateAttribute() + ") is not a number (long)", ex);
            }
        }

        try {
            return configuration.getLastLoginDateFormatInstance().parse(value).getTime();
        } catch (ParseException ex) {
            throw new InvalidAttributeValueException("Value " + value + " for last login date ("
                    + configuration.getLastLoginDateAttribute() + ") doesn't have proper format (" + configuration.getLastLoginDateFormat() + ")", ex);
        }
    }

	private boolean isUniqueAndNameAttributeEqual() {
		String uniqueAttribute = configuration.getUniqueAttribute();
		String nameAttribute = configuration.getNameAttribute();

		return uniqueAttribute == null ? nameAttribute == null : uniqueAttribute.equals(nameAttribute);
	}

	private List<String> createAttributeValues(String attributeValue) {
		List<String> values = new ArrayList<>();

		if (StringUtil.isEmpty(configuration.getMultivalueDelimiter())) {
			values.add(attributeValue);
		} else {
			String[] array = attributeValue.split(configuration.getMultivalueDelimiter());
			for (String item : array) {
				if (StringUtil.isEmpty(item)) {
					continue;
				}

				values.add(item);
			}
		}

		return values;
	}

	private Uid update(Operation operation, ObjectClass objectClass, Uid uid, Set<Attribute> attributes,
					   OperationOptions oo) {
		return update(operation, objectClass, uid, attributes, oo, false);
	}

	private Uid update(Operation operation, ObjectClass objectClass, Uid uid, Set<Attribute> attributes,
					   OperationOptions oo, boolean isSubjectForCleanup) {

		notNull(uid, "Uid must not be null");
		Uid originalUid = uid;
		boolean areReferencesManaged = !ArrayUtils.isEmpty(configuration.getManagedAssociationPairs());
		boolean holderOfComplexRefObject = false;
		boolean handleReferentialIntegrity = false;

		if(areReferencesManaged){

			holderOfComplexRefObject = mayContainComplexReferenceObject();
		}

		if ((Operation.ADD_ATTR_VALUE.equals(operation) || Operation.REMOVE_ATTR_VALUE.equals(operation))
				&& attributes.isEmpty()) {
			return uid;
		}

		Map<Integer, String> header = reverseHeaderMap();

		attributes = normalize(attributes);

		FileLock lock = obtainTmpFileLock(configuration);
		Reader reader = null;
		Writer writer = null;

		boolean createNewReference = false;
		try {
			Set<ReferenceDataPayload> referenceDataPayload = new HashSet<>();

			synchronized (CsvConnector.SYNCH_FILE_LOCK) {
				reader = createReader(configuration);
				writer = new BufferedWriter(Channels.newWriter(lock.channel(), configuration.getEncoding()));

				boolean found = false;

				CSVFormat csv = createCsvFormat(configuration);
				CSVParser parser = csv.parse(reader);

				csv = createCsvFormat(configuration);
				CSVPrinter printer = csv.print(writer);

				Iterator<CSVRecord> iterator = parser.iterator();

				while (iterator.hasNext()) {
					CSVRecord record = iterator.next();

					Map<String, String> data = new HashMap<>();
					for (int i = 0; i < record.size(); i++) {
						data.put(header.get(i), record.get(i));
					}

					String recordUidValue = data.get(configuration.getUniqueAttribute());
					if (StringUtil.isEmpty(recordUidValue)) {
						continue;
					}

					if (!uidMatches(uid.getUidValue(), recordUidValue, configuration.isIgnoreIdentifierCase())) {
						printer.printRecord(record);
						continue;
					}

					found = true;

					if (!Operation.DELETE.equals(operation)) {
						Set<ReferenceDataDeliveryVector> referenceDataDeliveryVector = null;

						Attribute referenceAttribute;

						if(areReferencesManaged) {

							referenceAttribute = getAttributeByName(holderOfComplexRefObject ? ASSOC_ATTR_ACCESS : ASSOC_ATTR_GROUP,
									attributes);

							if (referenceAttribute != null) {

								referenceDataDeliveryVector = determineReferenceObjectDataDeliveryVectors(referenceAttribute,
										uid.getUidValue());
							}
							if(referenceDataDeliveryVector != null) {

								referenceDataPayload = createReferenceDataFromAttributes(referenceDataDeliveryVector, attributes,
										referenceAttribute, operation, uid);
							}
						}
						List<Object> updated;


						if(isSubjectForCleanup) {

							CleanupFlag cleanupFlag = new CleanupFlag(isSubjectForCleanup);
							updated = updateObject(operation, data, attributes, cleanupFlag);

							if (cleanupFlag.isRemove()) {

								continue;
							}
						} else {

							updated = updateObject(operation, data, attributes, null);
						}

						int uidIndex = this.getHeader().get(configuration.getUniqueAttribute()).getIndex();
						Object newUidValue = updated.get(uidIndex);
						uid = new Uid(newUidValue.toString());

						printer.printRecord(updated);
					}
				}

				writer.close();
				reader.close();

				if (!found) {

					if (areReferencesManaged) {
						if (isSubjectForCleanup) {

							createNewReference = true;
						}
					}

					if (!createNewReference){

						throw new UnknownUidException("Account '" + uid + "' not found");
					}
				}

				moveTmpToOrig();
			}
			if(referenceDataPayload != null && !referenceDataPayload.isEmpty()){

			updateReferences(operation, referenceDataPayload, holderOfComplexRefObject);
			}
		} catch (Exception ex) {
			handleGenericException(ex, "Error during account '" + uid + "' " + operation.name());
		} finally {
			cleanupResources(writer, reader, lock, configuration);
		}

		if (areReferencesManaged) {

			if (createNewReference) {
				create(objectClass, attributes, null);
			}
			String changedIdValue = referentialIntegrityChange(operation, attributes);

			if (changedIdValue != null) {

				ReferentialInterityHandler referentialInterityHandler = new ReferentialInterityHandler(this);
				if(changedIdValue.isEmpty()){

					referentialInterityHandler.handle(uid.getUidValue(), operation);
				} else {

					referentialInterityHandler.handle(originalUid.getUidValue(), changedIdValue, operation);
				}

			}
		}

		return uid;
	}

	private String referentialIntegrityChange(Operation operation, Set<Attribute> attributes) {

		if (!associationHolders.containsKey(getObjectClassName())) {

			return null;
		}

		if (Operation.DELETE.equals(operation)) {

			return "";
		} else {

			Iterator<Attribute> iterator = attributes.iterator();
			String uniqueAttributeName = configuration.getUniqueAttribute();
			while (iterator.hasNext()) {
				Attribute attribute = iterator.next();
				if (attribute.getName().equals(uniqueAttributeName)) {

					List<Object> values = attribute.getValue();

					if (values != null) {
						return (String) values.get(0);
					}

					throw new ConnectorException("Unexpected situation." +
							" The updated ID attribute does not contain a value.");
				}
			}
		}

		return null;
	}

	private Set<Attribute> normalize(Set<Attribute> attributes) {
		if (attributes == null) {
			return null;
		}

		Set<Attribute> result = new HashSet<>();
		result.addAll(attributes);

		Attribute nameAttr = AttributeUtil.getNameFromAttributes(result);
		Object name = nameAttr != null ? AttributeUtil.getSingleValue(nameAttr) : null;

		Attribute uniqueAttr = AttributeUtil.find(configuration.getUniqueAttribute(), result);
		Object uid = uniqueAttr != null ? AttributeUtil.getSingleValue(uniqueAttr) : null;

		if (isUniqueAndNameAttributeEqual()) {
			if (name == null && uid != null) {
				if (nameAttr == null) {
					nameAttr = AttributeBuilder.build(Name.NAME, uid);
					result.add(nameAttr);
				}
			} else if (uid == null && name != null) {
				if (uniqueAttr == null) {
					uniqueAttr = AttributeBuilder.build(configuration.getUniqueAttribute(), name);
					result.add(uniqueAttr);
				}
			} else if (uid != null && name != null) {
				if (!name.equals(uid)) {
					throw new InvalidAttributeValueException("Unique attribute value doesn't match name attribute value");
				}
			}
		}

		Set<String> columns = getHeader().keySet();
		for (Attribute attribute : result) {
			String attrName = attribute.getName();
			if (Uid.NAME.equals(attrName) || Name.NAME.equals(attrName)
					|| OperationalAttributes.PASSWORD_NAME.equals(attrName)) {
				continue;
			}

            if (configuration.getLastLoginDateAttribute() != null &&
                    PredefinedAttributes.LAST_LOGIN_DATE_NAME.equals(attrName)) {
                continue;
            }

			if (!columns.contains(attrName)) {
				///TODO # A what if csv has native attribute called group?
				if(!ArrayUtils.isEmpty(configuration.getManagedAssociationPairs())) {
					if (ASSOC_ATTR_GROUP.equals(attribute.getName()) || ASSOC_ATTR_ACCESS.equals(attribute.getName())){
						continue;
					}
				}
				throw new ConnectorException("Unknown attribute " + attrName);
			}

			if (!isUniqueAndNameAttributeEqual() && isName(attrName)) {
				throw new ConnectorException("Column used as " + Name.NAME + " attribute");
			}
		}

		return result;
	}

	private boolean isName(String column) {
		return configuration.getNameAttribute().equals(column);
	}

	private List<Object> updateObject(Operation operation, Map<String, String> data, Set<Attribute> attributes,
									  CleanupFlag cleanupFlag) {

		Object[] result = new Object[getHeader().size()];

		List<String> mandatoryAccessAttributes = null;
		if(cleanupFlag !=null) {
			if (cleanupFlag.isSubjectForCleanup()) {
				mandatoryAccessAttributes = new ArrayList<>();
				Set<AssociationHolder> associationHoldersSet = associationHolders.get(getObjectClassName());

				for (AssociationHolder associationHolder : associationHoldersSet) {
					mandatoryAccessAttributes.add(associationHolder.getValueAttributeName());
				}
			}
		}

		// prefill actual data
		for (String column : getHeader().keySet()) {
			result[getHeader().get(column).getIndex()] = data.get(column);
		}

		// update data based on attributes parameter
		switch (operation) {
			case UPDATE:
				for (Attribute attribute : attributes) {
                    int index = getColumnIndex(attribute);

					String value = createRawValue(attribute, configuration);
					result[index] = value;
				}
				break;
			case ADD_ATTR_VALUE:
			case REMOVE_ATTR_VALUE:
				for (Attribute attribute : attributes) {
					String name = attribute.getName();

					int index = getColumnIndex(attribute);
					Class type = String.class;
					if (OperationalAttributes.PASSWORD_NAME.equals(name)) {
						type = GuardedString.class;
					}

					List<Object> current = Util.createAttributeValues((String) result[index], type, configuration);
					List<Object> updated = Operation.ADD_ATTR_VALUE.equals(operation) ?
							addValues(current, attribute.getValue()) :
							removeValues(current, attribute.getValue());

					if (isUid(name) && updated.size() != 1) {
						throw new IllegalArgumentException("Unique attribute '" + name + "' must contain single value");
					}

					String value = createRawValue(attribute.getName(), updated, configuration);

					result[index] = value;

					if (cleanupFlag != null && cleanupFlag.isSubjectForCleanup()) {
						if (mandatoryAccessAttributes != null && !mandatoryAccessAttributes.isEmpty()) {
							if (mandatoryAccessAttributes.contains(name)) {
								if (!(value != null && !value.isEmpty())) {

									cleanupFlag.setRemove(true);
									return null;
								}
							}
						}
					}
				}
		}

		return Arrays.asList(result);
	}

	private List<Object> updateObject(Map<String, String> data, Set <AttributeDelta> attributeDeltas,
									  CleanupFlag cleanupFlag) {
		Object[] result = new Object[getHeader().size()];

		List<String> mandatoryAccessAttributes = null;
		if(cleanupFlag !=null) {
			if (cleanupFlag.isSubjectForCleanup()) {
				mandatoryAccessAttributes = new ArrayList<>();
				Set<AssociationHolder> associationHoldersSet = associationHolders.get(getObjectClassName());

				for (AssociationHolder associationHolder : associationHoldersSet) {
					mandatoryAccessAttributes.add(associationHolder.getValueAttributeName());
				}
			}
		}

		// prefill actual data
		for (String column : getHeader().keySet()) {
			result[getHeader().get(column).getIndex()] = data.get(column);
		}

		for (AttributeDelta attributeDelta : attributeDeltas){

			String name = attributeDelta.getName();

			List<Object> objectsToAdd = attributeDelta.getValuesToAdd();
			List<Object> objectsToRemove = attributeDelta.getValuesToRemove();
			List<Object> objectsToReplace = attributeDelta.getValuesToReplace();


			if (objectsToReplace != null && !objectsToReplace.isEmpty()) {

				int index = getColumnIndex(attributeDelta.getName());
				result[index] = Util.createAttributeValues((String) result[index], String.class, configuration);

			} else {

				int index = getColumnIndex(name);
				Class type = String.class;

				List<Object> current = Util.createAttributeValues((String) result[index], type, configuration);
				List<Object> updated = current;
				if (objectsToAdd != null && !objectsToAdd.isEmpty()) {
					updated = addValues(current, objectsToAdd);
				}

				if (objectsToRemove != null && !objectsToRemove.isEmpty()) {
					updated = removeValues(updated, objectsToRemove);
				}

				String value = createRawValue(name, updated, configuration);

				result[index] = value;

				if (cleanupFlag != null && cleanupFlag.isSubjectForCleanup()) {
					if (mandatoryAccessAttributes != null && !mandatoryAccessAttributes.isEmpty()) {
						if (mandatoryAccessAttributes.contains(name)) {
							if (!(value != null && !value.isEmpty())) {

								cleanupFlag.setRemove(true);
								return null;
							}
						}
					}
				}
			}
		}

		return Arrays.asList(result);
	}

	public Map<ObjectClass ,ObjectClassHandler> getHandlers() {
		return handlers;
	}

	public void setHandlers(Map<ObjectClass ,ObjectClassHandler> handlers) {
		this.handlers = handlers;
	}

	public Set<CSVRecord> fetchCSVRecords(Map<String, Set<String>> searchedAttributeAndValues) {
		CSVFormat csv = createCsvFormatReader(configuration);

		Set<CSVRecord> recordSet = new HashSet<>();
		try (Reader reader = createReader(configuration)) {

			CSVParser parser = csv.parse(reader);
			Iterator<CSVRecord> iterator = parser.iterator();
			while (iterator.hasNext()) {
				CSVRecord record = iterator.next();
				if (skipRecord(record)) {
					continue;
				}
				Map<Integer, String> header = reverseHeaderMap();

				for (int i = 0; i < record.size(); i++) {
					String name = header.get(i);
					String value = record.get(i);
					ArrayList<String> values = (ArrayList<String>) createAttributeValues(value);

					if (searchedAttributeAndValues.containsKey(name)) {

						Set<String> searchedValues = searchedAttributeAndValues.get(name);
						boolean isMatch = !Collections.disjoint(values, searchedValues);
						if (isMatch){

							recordSet.add(record);
							break;
						}
					}
				}
			}
		} catch (Exception ex) {
			handleGenericException(ex, "Error during query execution");
		}
		return recordSet;
	}

	public Map<String, HashSet<AssociationHolder>> getAssociationHolders() {
		return associationHolders;
	}

	public void setAssociationHolders(Map<String, HashSet<AssociationHolder>> associationHolders) {
		this.associationHolders = associationHolders;
	}

	public void setSyncHook(Map<String, HashSet<String>> syncHook) {
		this.syncHook = syncHook;
	}

    private int getColumnIndex(Attribute attribute) {
        String name = attribute.getName();
		return getColumnIndex(name);
	}

	private int getColumnIndex(String name) {

		if (name.equals(Uid.NAME)) {
			return getHeader().get(configuration.getUniqueAttribute()).getIndex();
		} else if (name.equals(Name.NAME)) {
			return getHeader().get(configuration.getNameAttribute()).getIndex();
		} else if (name.equals(OperationalAttributes.PASSWORD_NAME)) {
			return getHeader().get(configuration.getPasswordAttribute()).getIndex();
		} else if (name.equals(PredefinedAttributes.LAST_LOGIN_DATE_NAME)) {
			return getHeader().get(configuration.getLastLoginDateAttribute()).getIndex();
		}
		return getHeader().get(name).getIndex();
	}

	public ObjectClassHandlerConfiguration getConfiguration() {
		return configuration;
	}

	public void updateAllReferencesOfValue(Filter filter , Set<AttributeDelta> attributeDeltaSet,
										   String value, boolean isSubjectForCleanup) {

		Map<Integer, String> header = reverseHeaderMap();
		boolean compareCaseInsensitive = false;

		FileLock lock = obtainTmpFileLock(configuration);
		Reader reader = null;
		Writer writer = null;

		List<String> filteredAttributeNames = new ArrayList<>();
		if (filter instanceof OrFilter) {

			for (Filter filterFromCollection : ((OrFilter) filter).getFilters()) {
				if (filterFromCollection instanceof EqualsFilter) {

					Attribute attribute = ((EqualsFilter) filterFromCollection).getAttribute();
					filteredAttributeNames.add(attribute.getName());
				} else if (filterFromCollection instanceof EqualsIgnoreCaseFilter) {

					compareCaseInsensitive = true;
					Attribute attribute = ((EqualsIgnoreCaseFilter) filterFromCollection).getAttribute();
					filteredAttributeNames.add(attribute.getName());
				} else {

					throw new ConnectorException("Unsupported type of Filter used in update all action.");
				}
			}

		} else if (filter instanceof EqualsFilter || filter instanceof EqualsIgnoreCaseFilter) {

			Attribute attribute = ((AttributeFilter) filter).getAttribute();
			filteredAttributeNames.add(attribute.getName());

			if (filter instanceof EqualsIgnoreCaseFilter) {
				compareCaseInsensitive = true;
			}
		} else {

			throw new ConnectorException("Unsupported type of Filter used in update all action.");
		}

		try {
			synchronized (CsvConnector.SYNCH_FILE_LOCK) {
				reader = createReader(configuration);
				writer = new BufferedWriter(Channels.newWriter(lock.channel(), configuration.getEncoding()));

				CSVFormat csv = createCsvFormat(configuration);
				CSVParser parser = csv.parse(reader);

				csv = createCsvFormat(configuration);
				CSVPrinter printer = csv.print(writer);

				Iterator<CSVRecord> iterator = parser.iterator();

				while (iterator.hasNext()) {
					CSVRecord record = iterator.next();

					Map<String, String> data = new HashMap<>();
					for (int i = 0; i < record.size(); i++) {
						data.put(header.get(i), record.get(i));
					}

					List<String> multivaluedAttributes = getMultivaluedAttributes();
					List<String> recordAttrValues = new ArrayList<>();

					for (String filteredAttributeName : filteredAttributeNames) {

						if (multivaluedAttributes.contains(filteredAttributeName)) {

							String[] attributeData = data.get(filteredAttributeName).
									split(configuration.getMultivalueDelimiter());
							recordAttrValues.addAll(Arrays.asList(attributeData));
						} else {

							recordAttrValues.add(data.get(filteredAttributeName));
						}
					}

					if (compareCaseInsensitive) {

						boolean contains = recordAttrValues.stream().anyMatch(value::equalsIgnoreCase);
						if (!contains) {
							printer.printRecord(record);
							continue;
						}
					} else {

						if (!recordAttrValues.contains(value)) {
							printer.printRecord(record);
							continue;
						}
					}

					List<Object> updated;

					if (isSubjectForCleanup){

						CleanupFlag cleanupFlag = new CleanupFlag(isSubjectForCleanup);
						updated = updateObject(data, attributeDeltaSet, cleanupFlag);

						if(cleanupFlag.isRemove()){

							continue;
						}
					} else {

					updated = updateObject(data, attributeDeltaSet, null);
					}

					if (updated != null && !updated.isEmpty()) {
						printer.printRecord(updated);
					} else {
						throw new ConnectorException("Object empty after reference value update");
					}

				}

				writer.close();
				reader.close();

				moveTmpToOrig();
			}
		} catch (Exception ex) {

			handleGenericException(ex, "Error during bulk reference update, change related to the ID: '" + value);

		} finally {
			cleanupResources(writer, reader, lock, configuration);
		}
	}
	private List<String> getMultivaluedAttributes(){

		List<String> multivalueAttributes = new ArrayList<>();


		if (StringUtil.isNotEmpty(configuration.getMultivalueAttributes())) {
			String[] array = configuration.getMultivalueAttributes().split(configuration.getMultivalueDelimiter());
			multivalueAttributes = Arrays.asList(array);
		}

		return multivalueAttributes;
	}
}
