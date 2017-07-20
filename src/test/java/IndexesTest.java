import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import org.testng.annotations.Test;

public class IndexesTest extends CreateSampleDatabaseFixture {

    @Test(expectedExceptions = OIndexException.class)
    public void shouldNotCreateDuplicateIndices() {

        OSchema schema = database.getMetadata().getSchema();
        for (int i = 0; i < 2; i++) {
            schema.getClass(CLASS_NAME).getProperty(PROPERTY_NAME).createIndex(OClass.INDEX_TYPE.UNIQUE);
        }
    }

    @Test(expectedExceptions = ORecordDuplicatedException.class)
    public void shouldNotAddTwoDocsWithSameIndexedField() {

        OSchema schema = database.getMetadata().getSchema();
        schema.getClass(CLASS_NAME).getProperty(PROPERTY_NAME).createIndex(OClass.INDEX_TYPE.UNIQUE);

        ODocument doc1 = new ODocument(CLASS_NAME);
        doc1.field(PROPERTY_NAME, "Snoopy" );
        doc1.save();

        ODocument doc2 = new ODocument(CLASS_NAME);
        doc2.field(PROPERTY_NAME, "Snoopy" );
        doc2.save();
    }

}
