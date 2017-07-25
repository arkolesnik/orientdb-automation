import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;

public class CreateDatabaseForLoadFixture {

    protected ODatabaseDocumentTx database;
    //TODO: create flexible path
    public static final String PATH = "plocal:D:/tmp/database/testuniqueindexes";
    public static final String USER = "admin";
    public static final String PASSWORD = "admin";
    public static final String CLASS_NAME = "NewData";
    public static final String INTEGER_PROPERTY_NAME = "integerProperty";
    public static final String LIST_PROPERTY_NAME = "listProperty";
    public static final String SET_PROPERTY_NAME = "setProperty";
    public static final String MAP_PROPERTY_NAME = "mapProperty";

    public static final String INTEGER_PROPERTY_INDEX = CLASS_NAME + "." + INTEGER_PROPERTY_NAME;
    public static final String LIST_PROPERTY_INDEX = CLASS_NAME + "." + LIST_PROPERTY_NAME;
    public static final String SET_PROPERTY_INDEX = CLASS_NAME + "." + SET_PROPERTY_NAME;
    public static final String MAP_PROPERTY_INDEX = CLASS_NAME + "." + MAP_PROPERTY_NAME;

    public static final String COMPOSITE_INDEX_1 = CLASS_NAME + ".composite1";
    public static final String COMPOSITE_INDEX_2 = CLASS_NAME + ".composite2";

    public static final int RECORDS_NUMBER = 100000;
    public static final int THREADS = 8;

    @BeforeTest
    public void createDatabase() {
        database = new ODatabaseDocumentTx(PATH).create();

        OClass data = database.getMetadata().getSchema().createClass(CLASS_NAME);
        data.createProperty(INTEGER_PROPERTY_NAME, OType.INTEGER);
        data.createProperty(LIST_PROPERTY_NAME, OType.EMBEDDEDLIST, OType.INTEGER);
        data.createProperty(SET_PROPERTY_NAME, OType.EMBEDDEDSET, OType.INTEGER);
        data.createProperty(MAP_PROPERTY_NAME, OType.EMBEDDEDMAP, OType.INTEGER);
    }

    @AfterTest
    public void dropDatabase() {
        database.drop();
    }

}
