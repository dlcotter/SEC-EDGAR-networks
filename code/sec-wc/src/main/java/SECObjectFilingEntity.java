/**
 * SECObjectFilingEntity - Filing-Entity relationship
 *
 */

public class SECObjectFilingEntity extends SECObject {
    private String accessionNumber;
    private String cik;
    
    public SECObjectFilingEntity( String cik,
				  String accessionNumber ) {
	this.objectType = SECObjectType.FILING_ENTITY;
	this.cik           = cik;
	this.accessionNumber = accessionNumber;
    }
    
    /**
     * Return a String representing the object as comma separated value record(s).
     */
    @Override
    public String toCSV() {
	String csvLine =    cik+
                        "|"+accessionNumber
                        ;
	// System.out.println( "toCSV: "+csvLine );
	return csvLine;
    }
}


