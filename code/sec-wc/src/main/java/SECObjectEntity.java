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
    private StringBuffer csvLine;
    
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
	csvLine = new StringBuffer(100);
    }
    
    /**
     * Return a String representing the object as comma separated value record(s).
     */
    @Override
    public String toCSV() {
	csvLine.delete(0,csvLine.capacity());
	csvLine.append( cik );
	csvLine.append('|');
	if ( tradingSymbol != null ) {
	    csvLine.append(tradingSymbol );
	}
	csvLine.append('|');
	if ( entityName != null ) {
	    csvLine.append(entityName );
	}
	csvLine.append('|');
	if ( entityType != null ) {
	    csvLine.append(entityType );
	}
	csvLine.append('|');
	if ( irsNumber != 0 ) {
	    csvLine.append(irsNumber);
	}
	csvLine.append('|');
	if ( sic != null ) {
	    csvLine.append(sic);
	}
	csvLine.append('|');
	if ( sicNumber != 0 ) {
	    csvLine.append(sicNumber);
	}
	csvLine.append('|');
	if ( stateOfInc != null ) {
	    csvLine.append(stateOfInc);
	}
	csvLine.append('|');
	if ( fiscalYearEnd != null ) {
	    csvLine.append(fiscalYearEnd );
	}
                        ;
	// System.out.println( "toCSV: "+csvLine );
	return csvLine.toString();
    }
}


