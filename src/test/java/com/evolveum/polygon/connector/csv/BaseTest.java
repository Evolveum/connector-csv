package com.evolveum.polygon.connector.csv;

import org.apache.commons.io.FileUtils;
import org.identityconnectors.framework.api.APIConfiguration;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.api.ConnectorFacadeFactory;
import org.identityconnectors.framework.common.objects.*;
import org.identityconnectors.test.common.TestHelpers;
import org.testng.AssertJUnit;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static com.evolveum.polygon.connector.csv.util.Util.ASSOC_ATTR_ACCESS;
import static com.evolveum.polygon.connector.csv.util.Util.ASSOC_ATTR_GROUP;
import static org.testng.AssertJUnit.*;

/**
 * Created by Viliam Repan (lazyman).
 */
public abstract class BaseTest {

    public static final String TEMPLATE_FOLDER_PATH = "./src/test/resources";

    public static final String CSV_FILE_PATH = "./target/data.csv";

    public static final String ATTR_UID = "uid";
    public static final String ATTR_FIRST_NAME = "firstName";
    public static final String ATTR_LAST_NAME = "lastName";
    public static final String ATTR_PASSWORD = "password";
    public static final String ATTR_MEMBER_OF = "memberOf";

    public static final String ATTR_ID = "id";
    public static final String ATTR_NAME = "name";
    public static final String ATTR_TITLE = "title";
    public static final String ATTR_EMPL = "empl";
    public static final String ATTR_DESCRIPTION = "description";
    public static final String ATTR_MEMBERS_TEST = "members-test";
    public static final String ATTR_MEMBERS_DEFAULT = "members-default";
    public static final String ATTR_MEMBERS_ADMIN = "members-admin";

    public static final String ATTR_LEVEL = "level";
    public static final String ATTR_SUBJECT_ID = "subject_id";
    public static final String ATTR_OBJECT_ID = "object_id";

    public static final String NEW_REFERENCE_ID = "1";
    public static final String NEW_USER_UID = "3";

    protected CsvConfiguration createConfiguration() {
        return createConfigurationNameEqualsUid();
    }

    protected CsvConfiguration createConfigurationNameEqualsUid() {
        CsvConfiguration config = new CsvConfiguration();

        config.setFilePath(new File(BaseTest.CSV_FILE_PATH));
        config.setTmpFolder(null);
        config.setUniqueAttribute(ATTR_UID);
        config.setPasswordAttribute(ATTR_PASSWORD);
        return config;
    }

    protected CsvConfiguration createConfigurationReferencedObjectClass(String filepath, String uniqueAttributeName) {
        CsvConfiguration config = new CsvConfiguration();

        config.setFilePath(new File(filepath));
        config.setTmpFolder(null);
        config.setUniqueAttribute(uniqueAttributeName);
        return config;
    }

    protected CsvConfiguration createConfigurationDifferent() {
        CsvConfiguration config = new CsvConfiguration();
        config.setFilePath(new File(BaseTest.CSV_FILE_PATH));
        config.setTmpFolder(null);
        config.setUniqueAttribute(ATTR_UID);
        config.setPasswordAttribute(ATTR_PASSWORD);
        config.setNameAttribute(ATTR_LAST_NAME);

        return config;
    }

    protected ConnectorFacade setupConnector(String csvTemplate) throws IOException {
        return setupConnector(csvTemplate, createConfiguration());
    }

    protected ConnectorFacade setupConnector(String csvTemplate, CsvConfiguration config) throws IOException {
        
    	copyDataFile(csvTemplate, config);

        return createNewInstance(config);
    }
    
    protected ConnectorFacade createNewInstance(CsvConfiguration config) {
    	ConnectorFacadeFactory factory = ConnectorFacadeFactory.getInstance();

        APIConfiguration impl = TestHelpers.createTestConfiguration(CsvConnector.class, config);
        return factory.newInstance(impl);
    }
    
    protected void copyDataFile(String csvTemplate, CsvConfiguration config) throws IOException {
    	File file = new File(CSV_FILE_PATH);
        file.delete();

        FileUtils.copyFile(new File(TEMPLATE_FOLDER_PATH + csvTemplate), new File(CSV_FILE_PATH));

        config.setFilePath(new File(CSV_FILE_PATH));
        config.setTmpFolder(null);

        config.validate();
    }

    protected void assertConnectorObject(Set<Attribute> expected, BaseConnectorObject object) {
        Set<Attribute> real = object.getAttributes();
        assertEquals(expected.size(), real.size());

        for (Attribute attr : expected) {
            List<Object> expValues = attr.getValue();

            String name = attr.getName();
            Attribute realAttr = object.getAttributeByName(name);
            assertNotNull(realAttr);

            List<String> expReferenceValues = getIdValuesFromReferenceAttribute(expValues);
            List<String> realReferenceValues = getIdValuesFromReferenceAttribute(realAttr.getValue());

            if (expReferenceValues != null && realReferenceValues != null) {
                assertEquals(expReferenceValues, realReferenceValues);
            } else {
                assertEquals(expValues.size(), realAttr.getValue().size());
                assertTrue(expValues.containsAll(realAttr.getValue()));
                assertTrue(realAttr.getValue().containsAll(expValues));
            }
        }
    }

   private List<String> getIdValuesFromReferenceAttribute(List<Object> values){

        List<String> referenceValues = new ArrayList<>();

        for (Object value : values) {

            if (value instanceof ConnectorObjectReference) {
                BaseConnectorObject bco = ((ConnectorObjectReference) value).getValue();
                Attribute attribute = bco.getAttributeByName(Uid.NAME);
                List<Object> idValues = attribute.getValue();

                for (Object o : idValues) {
                    if (o instanceof Uid) {
                        String idVal = ((Uid) o).getUidValue();
                        referenceValues.add(idVal);
                    }

                }
            } else {
                return null;
            }
        }

        return referenceValues;
    }

    protected Attribute createAttribute(String name, Object... values) {
        return AttributeBuilder.build(name, values);
    }

    public void objectContainsReferenceToObject(ConnectorObject connectorObject,
                                                ObjectClass objectClassOfReferencedObject, Set<Uid> uidOfReferencedObject,
                                                String accessAttributeName, String referenceAttributeName ) {

        Map<Uid, Set<Uid>> uidsOfAccessAndReferencedObject = new HashMap<>();

        if (uidOfReferencedObject != null && !uidOfReferencedObject.isEmpty()) {
            uidOfReferencedObject.forEach(o -> uidsOfAccessAndReferencedObject.put(o, null));
        }

        objectContainsReferenceToObject(connectorObject, null, objectClassOfReferencedObject,
                uidsOfAccessAndReferencedObject, accessAttributeName, referenceAttributeName);
    }

    public void objectContainsReferenceToObject(ConnectorObject connectorObject,
                                                ObjectClass objectClassOfRelationObject,
                                                ObjectClass objectClassOfReferencedObject,
                                                Map<Uid, Set<Uid>> uidsOfAccessAndReferencedObject,
                                                String accessAttributeName, String referenceAttrName) {

        Set<Attribute> connectorObjectAttributeSet = connectorObject.getAttributes();
        Map<Uid, Set<Uid>> foundReferenceUidList = new HashMap<>();

        for (Attribute attribute : connectorObjectAttributeSet) {
            if (referenceAttrName.equals(attribute.getName())) {
                List<Object> referenceList = attribute.getValue();

                for (Object connectorObjectReference : referenceList) {

                    if (connectorObjectReference instanceof ConnectorObjectReference) {

                        populateReferenceList(connectorObjectReference, objectClassOfRelationObject,
                                objectClassOfReferencedObject, accessAttributeName, foundReferenceUidList);
                    }
                }
            }
        }

        if (uidsOfAccessAndReferencedObject != null) {

            Set<Uid> expectedSet = uidsOfAccessAndReferencedObject.keySet();
            Set<Uid> resultSet = foundReferenceUidList.keySet();

            assertTrue("Returned object references do not match the expected result."
                    , expectedSet.equals(resultSet));

            for (Uid uid : uidsOfAccessAndReferencedObject.keySet()) {
                Set<Uid> expectedIdReferenceSetInAccessObject = uidsOfAccessAndReferencedObject.get(uid);
                Set<Uid> returnedIdReferenceSetInAccessObject = foundReferenceUidList.get(uid);

                if (expectedIdReferenceSetInAccessObject != null && !expectedIdReferenceSetInAccessObject.isEmpty()) {

                    assertTrue("The second level of expected references does not match the " +
                                    "test results ",
                            expectedIdReferenceSetInAccessObject.equals(returnedIdReferenceSetInAccessObject));
                } else {

                    assertTrue("The second level of expected references does not match the " +
                                    "test results ",
                            !(returnedIdReferenceSetInAccessObject != null &&
                                    !returnedIdReferenceSetInAccessObject.isEmpty()));
                }
            }

        } else {

            assertTrue("Object reference attributes present, where it shouldn't.",
                    foundReferenceUidList.isEmpty());
        }
    }

    private void populateReferenceList(Object connectorObjectReference,
                                       ObjectClass objectClassOfReferencedObject,
                                       String accessAttributeName, Map<Uid, Set<Uid>> foundReferenceUidList) {
        populateReferenceList(connectorObjectReference, null, objectClassOfReferencedObject, accessAttributeName, foundReferenceUidList);
    }

    private void populateReferenceList(Object connectorObjectReference, ObjectClass objectClassOfRelationObject,
                                       ObjectClass objectClassOfReferencedObject,
                                       String accessAttributeName, Map<Uid, Set<Uid>> foundReferenceUidList) {


        BaseConnectorObject baseConnectorObject = ((ConnectorObjectReference) connectorObjectReference).getValue();

        if (objectClassOfRelationObject != null) {

            assertTrue("Expected object class '" + objectClassOfRelationObject.getObjectClassValue() +
                            "' did not match the result '" + baseConnectorObject.getObjectClass().getObjectClassValue() + "'.",
                    objectClassOfRelationObject.equals(baseConnectorObject.getObjectClass()));
        } else {

            assertTrue("Expected object class '" + objectClassOfReferencedObject.getObjectClassValue() +
                            "' did not match the result '" + baseConnectorObject.getObjectClass().getObjectClassValue() + "'.",
                    objectClassOfReferencedObject.equals(baseConnectorObject.getObjectClass()));
        }


        Uid idValue = null;
        Map<Uid, Set<Uid>> referecedIds = new HashMap<>();

        if (accessAttributeName == null) {
            accessAttributeName = Uid.NAME;
        }

        Set<Attribute> attributeSet = baseConnectorObject.getAttributes();
        for (Attribute attributeInSet : attributeSet) {
            if (accessAttributeName.equals(attributeInSet.getName())) {
                List<Object> valueList = attributeInSet.getValue();

                if (valueList.size() == 1) {
                    idValue = new Uid((String) valueList.get(0));
                }

            } else if (ASSOC_ATTR_GROUP.equals(attributeInSet.getName()) ||
                    ASSOC_ATTR_ACCESS.equals(attributeInSet.getName())) {

                List<Object> referenceList = attributeInSet.getValue();

                for (Object objectReference : referenceList) {

                    if (objectReference instanceof ConnectorObjectReference) {

                        populateReferenceList(objectReference,
                                objectClassOfReferencedObject, accessAttributeName, referecedIds);
                    }
                }
            }
        }
        //  MAP referecedIds used only as a set of UID
        foundReferenceUidList.put(idValue, referecedIds.keySet());
    }

    protected ConnectorObject buildConnectorObject(String name, String uid, Set<Attribute> attributes, ObjectClass objectClass) {
        ConnectorObjectBuilder cob = new ConnectorObjectBuilder();

        cob.addAttribute((new AttributeBuilder().setName(Name.NAME).addValue(name)).build());
        cob.addAttribute((new AttributeBuilder().setName(Uid.NAME).addValue(uid)).build());

        if(attributes!=null) {
            for (Attribute attribute : attributes) {
                cob.addAttribute(attribute);
            }
        }

        cob.setObjectClass(objectClass);

        return cob.build();
    }

    protected BaseConnectorObject assertReferenceAndReturnReferenceObject(Set<Attribute> referenceAttributes,
                                                                          Attribute refAttr) {
        return assertReferenceAndReturnReferenceObject(referenceAttributes, refAttr, null);
    }

    protected BaseConnectorObject assertReferenceAndReturnReferenceObject(Set<Attribute> referenceAttributes,
                                                                        Attribute refAttr, Uid uid) {
        assertNotNull(refAttr);
        List<Object> valueList = refAttr.getValue();
        Object cor = null;
        if (!valueList.isEmpty() && valueList.size() == 1) {
            cor = valueList.get(0);

        } else if (uid != null) {

            for (Object connectorObjectReference : valueList) {
                BaseConnectorObject baseConnectorObject =
                        ((ConnectorObjectReference) connectorObjectReference).getValue();
                if (uid.equals(baseConnectorObject.getAttributeByName(Uid.NAME))) {
                    cor = connectorObjectReference;
                    break;
                }
            }
        }

        if (cor instanceof ConnectorObjectReference) {
            BaseConnectorObject bco = ((ConnectorObjectReference) cor).getValue();
            assertConnectorObject(referenceAttributes, bco);

            return bco;
        }


        return null;
    }
}
