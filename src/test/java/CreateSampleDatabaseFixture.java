import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;

public class CreateSampleDatabaseFixture {

    protected ODatabaseDocumentTx database;
    public static final String PATH = "plocal:D:/tmp/database/petshop";
    public static final String CLASS_NAME = "Animal";
    public static final String PROPERTY_NAME = "name";

    @BeforeTest
    public void createDatabase() {
        database = new ODatabaseDocumentTx(PATH).create();

        OClass animal = database.getMetadata().getSchema().createClass(CLASS_NAME);
        animal.createProperty(PROPERTY_NAME, OType.STRING);
    }

    @AfterTest
    public void dropDatabase() {
        database.drop();
    }

}
