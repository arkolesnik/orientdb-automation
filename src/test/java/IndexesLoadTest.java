import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import org.testng.annotations.Test;
import utils.Counter;
import utils.Operations;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static utils.BasicUtils.*;
import static utils.Operations.*;

public class IndexesLoadTest extends CreateDatabaseForLoadFixture {

    public static final Logger LOG = LoggerFactory.getLogger(IndexesLoadTest.class);

    @Test
    public void shouldRecreateIndexes() throws InterruptedException, ExecutionException {

        //TODO: change 100 to RECORDS_NUMBER
        for (int i = 0; i < 100; i++) {
            ODocument record = new ODocument(CLASS_NAME);
            Counter.increment();
            fillInRecordProperties(record);
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
                    ODatabaseDocumentTx db = new ODatabaseDocumentTx(PATH).open(USER, PASSWORD);
                    while (!interrupt.get()) {
                        performOperationAgainstRecord();
                    }
                    db.close();
                    return null;
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
    }

    private void fillInRecordProperties(ODocument record) {
        record.field(INTEGER_PROPERTY_NAME, (returnNextInt()));
        record.field(LIST_PROPERTY_NAME, getFilledList());
        record.field(SET_PROPERTY_NAME, getFilledSet());
        record.field(MAP_PROPERTY_NAME, getFilledMap());
        record.save();
    }

    private void createAllIndexes() {
        createUniqueIndexForProperties(INTEGER_PROPERTY_INDEX, INTEGER_PROPERTY_NAME);
        createUniqueIndexForProperties(LIST_PROPERTY_INDEX, LIST_PROPERTY_NAME);
        createUniqueIndexForProperties(SET_PROPERTY_INDEX, SET_PROPERTY_NAME);
        createUniqueIndexForProperties(MAP_PROPERTY_INDEX, MAP_PROPERTY_NAME);
        createUniqueIndexForProperties(COMPOSITE_INDEX_1, INTEGER_PROPERTY_NAME, LIST_PROPERTY_NAME);
        createUniqueIndexForProperties(COMPOSITE_INDEX_2, INTEGER_PROPERTY_NAME, SET_PROPERTY_NAME);
    }

    private List<Integer> getFilledList() {
        int listSize = generateSize();
        List<Integer> testList = new ArrayList<>(listSize);
        for (int i = 0; i < listSize; i++) {
            testList.add(returnNextInt());
        }
        return testList;
    }

    private Set<Integer> getFilledSet() {
        int setSize = generateSize();
        Set<Integer> testSet = new HashSet<>(setSize);
        for (int i = 0; i < setSize; i++) {
            testSet.add(returnNextInt());
        }
        return testSet;
    }

    private Map<String, Integer> getFilledMap() {
        int mapSize = generateSize();
        Map<String, Integer> testMap = new HashMap<>(mapSize);
        for (int i = 0; i < mapSize; i++) {
            testMap.put(returnNextInt().toString(), returnNextInt());
        }
        return testMap;
    }

    private void createUniqueIndexForProperties(String indexName, String... propertyNames) {
        database.getMetadata().getSchema().getClass(CLASS_NAME)
                .createIndex(indexName, OClass.INDEX_TYPE.UNIQUE, propertyNames);
    }

    private void performOperationAgainstRecord() {
        ODocument existingRecord;
        boolean done = false;
        int attempts = 0;
        switch (pickRandomOperation()) {
            case CREATE:
                ODocument newRecord = new ODocument(CLASS_NAME);
                Counter.increment();
                done = false;
                while (!done && attempts < 10) {
                    try {
                        fillInRecordProperties(newRecord);
                        done = true;
                    } catch (ORecordDuplicatedException e) {
                        attempts++;
                    }
                    LOG.info("C: " + newRecord.toString());
                }
                break;
            case UPDATE:
                for (int i = 0; i < 4; i++) {
                    existingRecord = randomlySelectRecord();
                    done = false;
                    while (!done && attempts < 10) {
                        try {
                            modifyRecordProperties(existingRecord);
                            done = true;
                        } catch (NullPointerException | ORecordNotFoundException
                                | ONeedRetryException | ORecordDuplicatedException e) {
                            existingRecord = randomlySelectRecord();
                            attempts++;
                        }
                        LOG.info("U: " + existingRecord.toString());
                    }
                }
                break;
            case DELETE:
                existingRecord = randomlySelectRecord();
                String objectInfo = existingRecord.toString();
                done = false;
                while (!done && attempts < 10) {
                    try {
                        existingRecord.delete();
                        done = true;
                    } catch (NullPointerException | ORecordNotFoundException
                            | ONeedRetryException e) {
                        existingRecord = randomlySelectRecord();
                        attempts++;
                    }
                    LOG.info("D: " + objectInfo);
                }
                Counter.decrement();
                break;
        }
        if (!done) {
            throw new IllegalStateException("Maximum attempts count is reached");
        }
    }

    private ODocument randomlySelectRecord() {
        ODatabaseDocumentTx database = (ODatabaseDocumentTx) ODatabaseRecordThreadLocal.INSTANCE.get();
        int[] clusterIDs = database.getMetadata().getSchema().getClass(CLASS_NAME).getClusterIds();
        OCluster cluster = null;
        int clusterID = 0;
        boolean success = false;
        long randomPosition = 0;
        try {
            while (!success) {
                clusterID = clusterIDs[new Random().nextInt(clusterIDs.length)];
                cluster = database.getStorage().getClusterById(clusterID);
                randomPosition = ThreadLocalRandom.current().nextLong(
                        cluster.getFirstPosition(), cluster.getLastPosition() + 1);
                if (randomPosition >= 0) {
                    success = true;
                }
            }
            OPhysicalPosition[] positions = cluster.ceilingPositions(new OPhysicalPosition(randomPosition));
            if (positions.length == 0) {
                positions = cluster.floorPositions(new OPhysicalPosition(randomPosition));
            }
            long position = positions[0].clusterPosition;
            return database.load(new ORecordId(clusterID, position));

        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private void modifyRecordProperties(ODocument record) {
        record.field(INTEGER_PROPERTY_NAME, (generateInt()));

        List<Integer> listProperty = record.field(LIST_PROPERTY_NAME);
        for (int i = 0; i < listProperty.size() / 2; i++) {
            listProperty.set(new Random().nextInt(listProperty.size()), generateInt());
        }

        Set<Integer> setProperty = record.field(SET_PROPERTY_NAME);
        List<Integer> utilityIntegerList = new ArrayList<>(setProperty);
        for (int i = 0; i < setProperty.size() / 2; i++) {
            setProperty.remove(utilityIntegerList.get(new Random().nextInt(utilityIntegerList.size())));
            setProperty.add(generateInt());
        }

        Map<String, Integer> mapProperty = record.field(MAP_PROPERTY_NAME);
        List<String> utilityStringList = new ArrayList<>(mapProperty.keySet());
        for (int i = 0; i < mapProperty.size() / 2; i++) {
            String key = utilityStringList.get(new Random().nextInt(utilityStringList.size()));
            mapProperty.remove(key);
            mapProperty.put(generateInt().toString(), generateInt());
        }
        record.save();
    }

    private Operations pickRandomOperation() {
        if (Counter.value() >= 200) {
            return getRandomFrom(UPDATE, DELETE);
        } else if (Counter.value() <= 100) {
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

}
