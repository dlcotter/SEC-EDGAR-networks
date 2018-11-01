
import junit.framework.Assert;
import org.junit.Test;

public class TestSECObjectFiling {

    static final String testParseData=
	"<SEC-HEADER>\n"
	+ "ACCESSION NUMBER:		0000003370-07-000179\n"
	+ "CONFORMED SUBMISSION TYPE:	4\n"
	+ "PUBLIC DOCUMENT COUNT:		1\n"
	+ "CONFORMED PERIOD OF REPORT:	20071025\n"
	+ "FILED AS OF DATE:		20071029\n"
	+ "DATE AS OF CHANGE:		20071029\n"
	+ "</SEC-HEADER>";

    @Test
    public void testSECObjectFilingParse() {

	
	SECObject[] filingData = SECObjectFiling.parse(testParseData, 0, testParseData.length());
	System.out.println( filingData[0].toCSV());
	
	Assert.assertEquals( filingData.length, 1 );
    }
}
