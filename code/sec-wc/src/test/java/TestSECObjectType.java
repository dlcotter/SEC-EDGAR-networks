
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class TestSECObjectType {

    @Test
    public void testSECObjectTypeParse() {
	
	SECObjectType tr0 = SECObjectType.parse( "contact" );
	SECObjectType tr4 = SECObjectType.parse( "filing" );
	SECObjectType tr6 = SECObjectType.parse( "form4" );
	SECObjectType tr8 = SECObjectType.parse( "none" );
	SECObjectType tr9 = SECObjectType.parse( "owner_rel" );

	assertEquals( "Incorrect SECObjectType.CONTACT",   tr0.to_int(),    1 );
	assertEquals( "Incorrect SECObjectType.FILING",    tr4.to_string(), SECObjectType.FILING.to_string() );
	assertEquals( "Incorrect SECObjectType.FORM4",     tr6.to_int(),    7 );
	assertEquals( "Incorrect SECObjectType.NONE",      tr8.to_string(), SECObjectType.NONE.to_string() );
	assertEquals( "Incorrect SECObjectType.OWNER_REL", tr9.to_int(),    10);
    }
}
