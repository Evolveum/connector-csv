package com.evolveum.polygon.connector.csv;

import org.apache.commons.io.FileUtils;
import org.identityconnectors.framework.api.APIConfiguration;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.api.ConnectorFacadeFactory;
import org.identityconnectors.framework.common.objects.*;
import org.identityconnectors.test.common.TestHelpers;
import org.testng.AssertJUnit;
import org.w3c.dom.Attr;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static com.evolveum.polygon.connector.csv.util.Util.ASSOC_ATTR_GROUP;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

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
    public static final String ATTR_REFERENCE_GROUP = "group";
    public static final String ATTR_MEMBER_OF = "memberOf";

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

    protected void assertConnectorObject(Set<Attribute> expected, ConnectorObject object) {
        Set<Attribute> real = object.getAttributes();
        assertEquals(expected.size(), real.size());

        for (Attribute attr : expected) {
            List<Object> expValues = attr.getValue();

            String name = attr.getName();
            Attribute realAttr = object.getAttributeByName(name);
            assertNotNull(realAttr);

            List<String> expReferenceValues = getValuesFromReferenceAttribute(expValues);
            List<String> realReferenceValues = getValuesFromReferenceAttribute(realAttr.getValue());

            if (expReferenceValues != null && realReferenceValues != null) {
                assertEquals(expReferenceValues, realReferenceValues);
            } else {
                assertEquals(expValues, realAttr.getValue());
            }
        }
    }

   private List<String> getValuesFromReferenceAttribute(List<Object> values){

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
                                                String accessAttributeName ) {

        Map<Uid, Set<Uid>> uidsOfAccessAndReferencedObject = new HashMap<>();

        if (uidOfReferencedObject != null && !uidOfReferencedObject.isEmpty()) {
            uidOfReferencedObject.forEach(o -> uidsOfAccessAndReferencedObject.put(o, null));
        }

        objectContainsReferenceToObject(connectorObject, null, objectClassOfReferencedObject,
                uidsOfAccessAndReferencedObject, accessAttributeName);
    }

    public void objectContainsReferenceToObject(ConnectorObject connectorObject,
                                                ObjectClass objectClassOfRelationObject,
                                                ObjectClass objectClassOfReferencedObject,
                                                Map<Uid, Set<Uid>> uidsOfAccessAndReferencedObject,
                                                String accessAttributeName) {

        Set<Attribute> connectorObjectAttributeSet = connectorObject.getAttributes();
        Map<Uid, Set<Uid>> foundReferenceUidList = new HashMap<>();

        for (Attribute attribute : connectorObjectAttributeSet) {
            if (ASSOC_ATTR_GROUP.equals(attribute.getName())) {
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

            AssertJUnit.assertTrue("Returned object references do not match the expected result."
                    , expectedSet.equals(resultSet));

            for (Uid uid : uidsOfAccessAndReferencedObject.keySet()) {
                Set<Uid> expectedIdReferenceSetInAccessObject = uidsOfAccessAndReferencedObject.get(uid);
                Set<Uid> returnedIdReferenceSetInAccessObject = foundReferenceUidList.get(uid);

                if (expectedIdReferenceSetInAccessObject != null && !expectedIdReferenceSetInAccessObject.isEmpty()) {

                    AssertJUnit.assertTrue("The second level of expected references does not match the " +
                                    "test results ",
                            expectedIdReferenceSetInAccessObject.equals(returnedIdReferenceSetInAccessObject));
                } else {

                    //TODO check if returnedIdReferenceSetInAccessObject should be null || empty ?
                }
            }

        } else {

            AssertJUnit.assertTrue("Object reference attributes present, where it shouldn't.",
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

            AssertJUnit.assertTrue("Expected object class '" + objectClassOfRelationObject.getObjectClassValue() +
                            "' did not match the result '" + baseConnectorObject.getObjectClass().getObjectClassValue() + "'.",
                    objectClassOfRelationObject.equals(baseConnectorObject.getObjectClass()));
        } else {

            AssertJUnit.assertTrue("Expected object class '" + objectClassOfReferencedObject.getObjectClassValue() +
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

            } else if (ASSOC_ATTR_GROUP.equals(attributeInSet.getName())) {

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
}
