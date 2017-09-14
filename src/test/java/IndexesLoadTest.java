import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import org.testng.annotations.Test;
import utils.Counter;
import utils.Fields;
import utils.Operations;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static utils.BasicUtils.*;
import static utils.Operations.*;

public class IndexesLoadTest extends CreateDatabaseForLoadFixture {

    private static final Logger LOG = LoggerFactory.getLogger(IndexesLoadTest.class);

    private static final int ATTEMPTS_NUMBER = 500000;
/*    private static final int INSTANCES_MAX = 200000;
    private static final int INSTANCES_MIN = 100000;*/

    private static final int INSTANCES_MAX = 200;
    private static final int INSTANCES_MIN = 100;

    @Test
    public void shouldRecreateIndexes() throws InterruptedException, ExecutionException {

        for (int i = 0; i < RECORDS_NUMBER; i++) {
            ODocument record = new ODocument(CLASS_NAME);
            Counter.incrementInstance();
            fillInRecordProperties(record, Counter.returnNextId());
        }

        createAllIndexes();

        ExecutorService executor = Executors.newFixedThreadPool(THREADS);
        List<Callable<Object>> tasks = new ArrayList<>();
        AtomicBoolean interrupt = new AtomicBoolean(false);

        new Timer().schedule(
                new TimerTask() {
                    public void run() {
                        interrupt.set(true);
                    }
                },
                getDateToInterrupt());

        try {
            for (int i = 0; i < THREADS; i++) {
                tasks.add(() -> {
                    int iterationNumber = 0;
                    try {
                        ODatabaseDocumentTx db = new ODatabaseDocumentTx(PATH).open(USER, PASSWORD);
                        while (!interrupt.get()) {
                            iterationNumber++;
                            performOperationAgainstRecord(iterationNumber);
                        }
                        db.close();
                        return null;
                    } catch (Exception e) {
                        LOG.error("Exception during operation processing", e);
                        throw e;
                    }
                });
            }
            List<Future<Object>> futures = executor.invokeAll(tasks);
            for (Future future : futures) {
                future.get();
            }
        } finally {
            executor.shutdown();
        }

        dropAllIndexes();
        createAllIndexes();
        LOG.info("Number of ORecordDuplicationException caught during the test is: " + Counter.getDuplicationNumber());
    }

    private void fillInRecordProperties(ODocument record, int id) {
        record.field(ID_NAME, id);
        record.field(INTEGER_PROPERTY_NAME, (Counter.returnNextInt()));
        record.field(LIST_PROPERTY_NAME, getFilledList());
        record.field(SET_PROPERTY_NAME, getFilledSet());
        record.field(MAP_PROPERTY_NAME, getFilledMap());
        record.save();
    }

    private void createAllIndexes() {
        createUniqueIndexForProperties(ID_INDEX, ID_NAME);
        createUniqueIndexForProperties(INTEGER_PROPERTY_INDEX, INTEGER_PROPERTY_NAME);
        createUniqueIndexForProperties(LIST_PROPERTY_INDEX, LIST_PROPERTY_NAME);
        createUniqueIndexForProperties(SET_PROPERTY_INDEX, SET_PROPERTY_NAME);
        createUniqueIndexForProperties(MAP_PROPERTY_INDEX, MAP_PROPERTY_NAME);
        createUniqueIndexForProperties(COMPOSITE_INDEX_1, INTEGER_PROPERTY_NAME, LIST_PROPERTY_NAME);
        createUniqueIndexForProperties(COMPOSITE_INDEX_2, INTEGER_PROPERTY_NAME, SET_PROPERTY_NAME);
    }

    private List<Integer> getFilledList() {
        int listSize = Counter.generateSize();
        List<Integer> testList = new ArrayList<>(listSize);
        for (int i = 0; i < listSize; i++) {
            testList.add(Counter.returnNextInt());
        }
        return testList;
    }

    private Set<Integer> getFilledSet() {
        int setSize = Counter.generateSize();
        Set<Integer> testSet = new HashSet<>(setSize);
        for (int i = 0; i < setSize; i++) {
            testSet.add(Counter.returnNextInt());
        }
        return testSet;
    }

    private Map<String, Integer> getFilledMap() {
        int mapSize = Counter.generateSize();
        Map<String, Integer> testMap = new HashMap<>(mapSize);
        for (int i = 0; i < mapSize; i++) {
            testMap.put(Counter.returnNextInt().toString(), Counter.returnNextInt());
        }
        return testMap;
    }

    private void createUniqueIndexForProperties(String indexName, String... propertyNames) {
        database.getMetadata().getSchema().getClass(CLASS_NAME)
                .createIndex(indexName, OClass.INDEX_TYPE.UNIQUE, propertyNames);
    }

    private void performOperationAgainstRecord(int iterationNumber) {
        ODocument existingRecord;
        boolean done = false;
        int attempts = 0;
        switch (pickRandomOperation()) {
            case CREATE:
                ODocument newRecord = new ODocument(CLASS_NAME);
                Counter.incrementInstance();
                done = false;
                while (!done && attempts < ATTEMPTS_NUMBER) {
                    try {
                        int idForCreate;
                        if (Counter.getDeletedIds().isEmpty()) {
                            idForCreate = Counter.returnNextId();
                        } else {
                            idForCreate = Counter.getDeletedIds().poll();
                        }
                        fillInRecordProperties(newRecord, idForCreate);
                        done = true;
                    } catch (ORecordDuplicatedException e) {
                        attempts++;
                        logAttemptsCount(attempts);
                        Counter.incrementDuplicate();
                    }
                }
                break;
            case UPDATE:
                for (int i = 0; i < 4; i++) {
                    existingRecord = randomlySelectRecord();
                    done = false;
                    while (!done && attempts < ATTEMPTS_NUMBER) {
                        try {
                            modifyRecordProperties(existingRecord);
                            done = true;
                        } catch (NullPointerException | ORecordNotFoundException
                                | ONeedRetryException | ORecordDuplicatedException e) {
                            existingRecord = randomlySelectRecord();
                            attempts++;
                            logAttemptsCount(attempts);
                            if (e instanceof ORecordDuplicatedException) {
                                Counter.incrementDuplicate();
                            }
                        }
                    }
                }
                break;
            case DELETE:
                existingRecord = randomlySelectRecord();
                done = false;
                while (!done && attempts < ATTEMPTS_NUMBER) {
                    try {
                        int deletedId = existingRecord.field(ID_NAME);
                        existingRecord.delete();
                        Counter.getDeletedIds().offer(deletedId);
                        done = true;
                    } catch (NullPointerException | ORecordNotFoundException
                            | ONeedRetryException e) {
                        existingRecord = randomlySelectRecord();
                        attempts++;
                        logAttemptsCount(attempts);
                    }
                }
                Counter.decrementInstance();
                break;
        }
        if (!done) {
            throw new IllegalStateException("Maximum attempts count is reached");
        }
        if (iterationNumber % 1000 == 0) {
            LOG.info("Thread " + Thread.currentThread().getId() + " has performed " + iterationNumber + " operations");
        }
    }

    private ODocument randomlySelectRecord() {
        ODatabaseDocumentTx database = (ODatabaseDocumentTx) ODatabaseRecordThreadLocal.INSTANCE.get();
        ODocument selectedRecord;
        int randomId;
        while (true) {
            randomId = ThreadLocalRandom.current().nextInt(1, Counter.getInstanceNumber());
            List<ODocument> result = database.query(
                    new OSQLSynchQuery<>("select from " + CLASS_NAME + " where "
                            + ID_NAME + " = " + randomId));
            if (!result.isEmpty()) {
                selectedRecord = result.get(0);
                break;
            }
        }

        return selectedRecord;
    }

    private void modifyRecordProperties(ODocument record) {
        int numberOfFieldsToModify = ThreadLocalRandom.current().nextInt(
                1, Fields.values().length + 1);

        Set<Fields> fields = new HashSet<>();
        while (fields.size() < numberOfFieldsToModify) {
            fields.add(Fields.getRandom());
        }

        for (Fields field : fields) {
            switch (field) {
                case INTEGER_PROPERTY:
                    modifyIntegerProperty(record);
                    break;
                case LIST_PROPERTY:
                    modifyListProperty(record);
                    break;
                case SET_PROPERTY:
                    modifySetProperty(record);
                    break;
                case MAP_PROPERTY:
                    modifyMapProperty(record);
                    break;
            }
        }
        record.save();
    }

    private void modifyIntegerProperty(ODocument record) {
        record.field(INTEGER_PROPERTY_NAME, (Counter.generateInt()));
    }

    private void modifyListProperty(ODocument record) {
        List<Integer> listProperty = record.field(LIST_PROPERTY_NAME);
        for (int i = 0; i < listProperty.size() / 2; i++) {
            listProperty.set(new Random().nextInt(listProperty.size()), Counter.generateInt());
        }
    }

    private void modifySetProperty(ODocument record) {
        Set<Integer> setProperty = record.field(SET_PROPERTY_NAME);
        List<Integer> utilityIntegerList = new ArrayList<>(setProperty);
        for (int i = 0; i < setProperty.size() / 2; i++) {
            setProperty.remove(utilityIntegerList.get(new Random().nextInt(utilityIntegerList.size())));
            setProperty.add(Counter.generateInt());
        }
    }

    private void modifyMapProperty(ODocument record) {
        Map<String, Integer> mapProperty = record.field(MAP_PROPERTY_NAME);
        List<String> utilityStringList = new ArrayList<>(mapProperty.keySet());
        for (int i = 0; i < mapProperty.size() / 2; i++) {
            String key = utilityStringList.get(new Random().nextInt(utilityStringList.size()));
            mapProperty.remove(key);
            mapProperty.put(Counter.generateInt().toString(), Counter.generateInt());
        }
    }

    private Operations pickRandomOperation() {
        if (Counter.getInstanceNumber() >= INSTANCES_MAX) {
            return getRandomFrom(UPDATE, DELETE);
        } else if (Counter.getInstanceNumber() <= INSTANCES_MIN) {
            return getRandomFrom(CREATE, UPDATE);
        }
        return getRandomFrom(CREATE, UPDATE, DELETE);
    }

    private void dropAllIndexes() {
        OIndexManager manager = database.getMetadata().getIndexManager();
        manager.dropIndex(INTEGER_PROPERTY_INDEX);
        manager.dropIndex(LIST_PROPERTY_INDEX);
        manager.dropIndex(SET_PROPERTY_INDEX);
        manager.dropIndex(MAP_PROPERTY_INDEX);
        manager.dropIndex(COMPOSITE_INDEX_1);
        manager.dropIndex(COMPOSITE_INDEX_2);
    }

    private void logAttemptsCount(int attempts) {
        if (attempts % 5000 == 0) {
            LOG.warn(attempts + " attempts were made");
        }
    }

}
