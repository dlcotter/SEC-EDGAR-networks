/**
 * SECObjectEntity - Entity metadata
 *
 */

import java.util.Date;
import java.util.HashMap;
import java.text.DateFormat;
import java.text.DateFormat.Field;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

public class SECObjectEntity extends SECObject {
    private String cik;
    private String tradingSymbol;
    private String entityName;
    private String entityType;
    private long   irsNumber;
    private String sic;
    private int    sicNumber;
    private String stateOfInc;
    private String fiscalYearEnd;
    
    public SECObjectEntity( String cik,
			    String tradingSymbol,
			    String entityName,
			    String entityType,
			    String irsNumber,
			    String sic,
			    String sicNumber,
			    String stateOfInc,
			    String fiscalYearEnd ) {
	this.objectType    = SECObjectType.ENTITY;
	this.cik           = cik;
	this.tradingSymbol = tradingSymbol;
	this.entityName    = entityName;
	this.entityType    = entityType;
	if ( irsNumber != null ) {
	    this.irsNumber     = Long.parseLong(irsNumber);
	} else {
	    this.irsNumber     = 0;
	}
	this.sic           = sic;
	if ( sicNumber != null ) {
	    this.sicNumber     = Integer.parseInt(sicNumber);
	} else {
	    this.sicNumber     = 0;
	}
	this.stateOfInc    = stateOfInc;
	this.fiscalYearEnd = fiscalYearEnd;
    }
    
    /**
     * Return a String representing the object as comma separated value record(s).
     */
    @Override
    public String toCSV() {
	String csvLine =    cik+
                        ","+tradingSymbol+
	                ",\""+entityName+"\""+
                        ","+entityType+
                        ","+irsNumber+
                        ",\""+sic+"\""+
                        ","+sicNumber+
                        ","+stateOfInc+
                        ","+fiscalYearEnd
                        ;
	// System.out.println( "toCSV: "+csvLine );
	return csvLine;
    }
}


