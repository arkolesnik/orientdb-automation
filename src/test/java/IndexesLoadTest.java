import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OCluster;
import org.joda.time.DateTime;
import org.joda.time.Seconds;
import org.testng.annotations.Test;
import utils.BasicUtils;
import utils.Counter;
import utils.Operations;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

import static utils.BasicUtils.generateSize;
import static utils.BasicUtils.returnNextLong;
import static utils.Operations.*;

public class IndexesLoadTest extends CreateDatabaseForLoadFixture {

    @Test
    public void shouldRecreateIndexes() throws InterruptedException, ExecutionException {

        for (int i = 0; i < 100; i++) {
            ODocument record = new ODocument(CLASS_NAME);
            Counter.increment();
            fillInRecordProperties(record);
        }

        createAllIndexes();

        ExecutorService executor = Executors.newFixedThreadPool(THREADS);
        List<Callable<Object>> tasks = new ArrayList<>();
        DateTime start = new DateTime();
        try {
            for (int i = 0; i < THREADS; ++i) {
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

    private synchronized void fillInRecordProperties(ODocument record) {
        record.field(INTEGER_PROPERTY_NAME, returnNextLong());
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

    private List<Long> getFilledList() {
        int listSize = generateSize();
        List<Long> testList = new ArrayList<>(listSize);
        for (int i = 0; i < listSize; i++) {
            testList.add(returnNextLong());
        }
        return testList;
    }

    private Set<Long> getFilledSet() {
        int setSize = generateSize();
        Set<Long> testSet = new HashSet<>(setSize);
        for (int i = 0; i < setSize; i++) {
            testSet.add(returnNextLong());
        }
        return testSet;
    }

    private Map<Long, Long> getFilledMap() {
        int mapSize = generateSize();
        Map<Long, Long> testMap = new HashMap<>(mapSize);
        for (int i = 0; i < mapSize; i++) {
            testMap.put(returnNextLong(), returnNextLong());
        }
        return testMap;
    }

    private void createUniqueIndexForProperties(String indexName, String... propertyNames) {
        database.getMetadata().getSchema().getClass(CLASS_NAME)
                .createIndex(indexName, OClass.INDEX_TYPE.UNIQUE, propertyNames);
    }

    private synchronized void performOperationAgainstRecord() {
        ODocument rec;
        boolean done;
        switch (randomlySelectOperation()) {
            case CREATE:
                ODocument newRecord = new ODocument(CLASS_NAME);
                Counter.increment();
                fillInRecordProperties(newRecord);
                System.out.println("C: " + newRecord.toString());
                break;
            case UPDATE:
                for (int i = 0; i < 4; i++) {
                    rec = randomlySelectRecord();
                    done = false;
                    while (!done) {
                        try {
                            System.out.println("U: " + rec.toString());
                            fillInRecordProperties(rec);
                            done = true;
                        } catch (NullPointerException | ORecordNotFoundException | ONeedRetryException e) {
                            rec = randomlySelectRecord();
                        }
                    }
                }
                break;
            case DELETE:
                //TODO: add null check?
                rec = randomlySelectRecord();
                done = false;
                while (!done) {
                    try {
                        System.out.println("D: " + rec.toString());
                        rec.delete();
                        done = true;
                    } catch (NullPointerException | ORecordNotFoundException | ONeedRetryException e) {
                        rec = randomlySelectRecord();
                    }
                }
                Counter.decrement();
                break;
        }
    }

    private synchronized ODocument randomlySelectRecord() {
        ODatabaseDocumentTx database = (ODatabaseDocumentTx) ODatabaseRecordThreadLocal.INSTANCE.get();
        int[] clusterIDs = database.getMetadata().getSchema().getClass(CLASS_NAME).getClusterIds();
        int clusterID = clusterIDs[new Random().nextInt(clusterIDs.length)];
        OCluster cluster = database.getStorage().getClusterById(clusterID);
        try {
            long randomPosition = ThreadLocalRandom.current().nextLong(
                    cluster.getFirstPosition(), cluster.getLastPosition() + 1);
            return database.load(new ORecordId(clusterID, randomPosition));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private Operations randomlySelectOperation() {
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
