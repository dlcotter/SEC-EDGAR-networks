/**
 * SECObjectContact
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

public class SECObjectContact extends SECObject {
    static final Date badDate = new Date(0,0,1);

    private Date   filingDate;
    private String cik;
    private String contactType;
    private String street1;
    private String street2;
    private String city;
    private String state;
    private String zipCode;
    private String phone;
    
    public SECObjectContact( String cik,
			    Date filingDate,
			    String contactType,
			    String street1,
			    String street2,
			    String city,
			    String state,
			    String zipCode,
			    String phone ) {
	this.objectType  = SECObjectType.ENTITY;
	this.cik         = cik;
	this.filingDate  = filingDate;
	this.contactType = contactType;
	this.street1     = street1;
	this.street2     = street2;
	this.city        = city;
	this.state       = state;
	this.zipCode     = zipCode;
	this.phone       = phone;
    }
    
    /**
     * Return a String representing the object as comma separated value record(s).
     */
    @Override
    public String toCSV() {
	SimpleDateFormat sdf0 = new SimpleDateFormat("YYYY-MM-dd");
	String date;
	try {
	    date = sdf0.format(filingDate);
	} catch (Exception e) {
	    date = sdf0.format(badDate);
	}

	String csvLine =    cik+
                        ","+date+
	                ",\""+contactType+"\""+
                        ","+street1+
                        ","+street2+
                        ","+city+
                        ","+state+
                        ","+zipCode+
                        ","+phone
                        ;
	// System.out.println( "toCSV: "+csvLine );
	return csvLine;
    }
}


