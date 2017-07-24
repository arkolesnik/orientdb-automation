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
import org.joda.time.DateTime;
import org.joda.time.Seconds;
import org.testng.annotations.Test;
import utils.Counter;
import utils.Operations;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static utils.BasicUtils.generateSize;
import static utils.BasicUtils.generateInt;
import static utils.BasicUtils.returnNextInt;
import static utils.Operations.*;

public class IndexesLoadTest extends CreateDatabaseForLoadFixture {

    public static final Logger LOG = LoggerFactory.getLogger(IndexesLoadTest.class);

    @Test
    public void shouldRecreateIndexes() throws InterruptedException, ExecutionException {

        //TODO: change to 100000
        for (int i = 0; i < 100; i++) {
            ODocument record = new ODocument(CLASS_NAME);
            Counter.increment();
            fillInRecordProperties(record);
        }

        try {
            createAllIndexes();
        } catch (ORecordDuplicatedException e) {
        }

        ExecutorService executor = Executors.newFixedThreadPool(THREADS);
        List<Callable<Object>> tasks = new ArrayList<>();
        DateTime start = new DateTime();
        try {
            for (int i = 0; i < THREADS; i++) {
                tasks.add(() -> {
                    ODatabaseDocumentTx db = new ODatabaseDocumentTx(PATH).open(USER, PASSWORD);
                    DateTime current = new DateTime();
                    //TODO: change seconds to hours
                    while (Seconds.secondsBetween(start, current).getSeconds() < THREAD_TIMEOUT) {
                        performOperationAgainstRecord();
                        current = new DateTime();
                    }
                    db.close();
                    return null;
                });
            }
            //TODO: delete extra seconds
            //TODO: change seconds to hours
            List<Future<Object>> futures = executor.invokeAll(tasks, THREAD_TIMEOUT + 10, TimeUnit.SECONDS);
            for (Future future : futures) {
                future.get();
            }
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } finally {
            if (!executor.isTerminated()) {
                executor.shutdownNow();
            }
        }

        dropAllIndexes();
        //TODO: try to catch specific exception?
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
        ODocument rec;
        boolean done = false;
        int attempts = 0;
        switch (pickRandomOperation()) {
            case CREATE:
                ODocument newRecord = new ODocument(CLASS_NAME);
                Counter.increment();
                LOG.info("Create operation is going to be performed");
                fillInRecordProperties(newRecord);
                LOG.info("C: " + newRecord.toString());
                done = true;
                break;
            case UPDATE:
                for (int i = 0; i < 4; i++) {
                    rec = randomlySelectRecord();
                    done = false;
                    while (!done && attempts < 10) {
                        try {
                            LOG.info("Update operation is going to be performed");
                            modifyRecordProperties(rec);
                            LOG.info("U: " + rec.toString());
                            done = true;
                        } catch (NullPointerException | ORecordNotFoundException
                                | ONeedRetryException | ORecordDuplicatedException e) {
                            rec = randomlySelectRecord();
                            attempts++;
                        }
                    }
                }
                break;
            case DELETE:
                rec = randomlySelectRecord();
                done = false;
                while (!done && attempts < 10) {
                    try {
                        LOG.info("Delete operation is going to be performed");
                        rec.delete();
                        LOG.info("D: " + rec.toString());
                        done = true;
                    } catch (NullPointerException | ORecordNotFoundException
                            | ONeedRetryException e) {
                        rec = randomlySelectRecord();
                        attempts++;
                    }
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
        int clusterID = clusterIDs[new Random().nextInt(clusterIDs.length)];
        OCluster cluster = database.getStorage().getClusterById(clusterID);
        try {
            long randomPosition = ThreadLocalRandom.current().nextLong(
                    cluster.getFirstPosition(), cluster.getLastPosition() + 1);

            OPhysicalPosition[] positions = cluster.ceilingPositions(new OPhysicalPosition(randomPosition));
            if (positions.length == 0) {
                positions = cluster.floorPositions(new OPhysicalPosition(randomPosition));
            }
            long position = positions[0].clusterPosition;
            return database.load(new ORecordId(clusterID, position));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
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
