#
# Program to parse the SEC filings and generate CSV files from the data
#
# Steve Roggenkamp
#

import collections
import csv
import io
import re
import sys
import xml.parsers.expat


class SECDict(dict):
    def __missing__(self,key):
        return ""

# Convert a string to a date, using 1990-01-01 to indicate a missing value
def toDate(str):
    if len(str) == 8:
        return str[0:4]+'-'+str[4:6]+'-'+str[6:8]
    else:
        return "1990-01-01"

# Main class used for processing SEC filings
class SECFiling:
    """An SEC Filing"""

    objType = "SECFiling"

    global Token
    Token = collections.namedtuple( 'Token', ['type','value'])
    
    def __init__(self, accessionNumber, submissionType, docCount, reportingDate, filingDate, changeDate ):
        self.objType = SECFiling.objType

    # main method entry point to parse a file, accumulating rows of data
    # in outputrows
    def parse(fileName,outputrows):
        global parse
        accessionNumber = ''
        issuerCik       = ''
        filingDate      = ''
        missingDate     = '1990-01-01'

        # parse a document contained within the filing document
        #
        # This implements the handlers for XML SAX parsing events
        # generated as a result of parsing an embedded XML document
        # One qurik is that we have to pass data back through the
        # outputrrows since the expat parser is written in C/C++
        # and does not pass along the yield results
        def parseDocument( docContent, accessionNumber ):
 
            xmlObjs                   = []
            currentData               = ''
            currentElement            = ''
            documentType              = ''
            rptOwnerCik               = ''
            rptOwnerName              = ''
            rptOwnerStreet1           = ''
            rptOwnerStreet2           = ''
            rptOwnerCity              = ''
            rptOwnerState             = ''
            rptOwnerZipCode           = ''
            rptOwnerIsDirector        = 0
            rptOwnerIsOfficer         = 0
            rptOwnerIsTenPercentOwner = 0
            rptOwnerIsOther           = 0
            rptOwnerOfficerTitle      = ''
            state                     = 0

            # handle an EndElement event
            def endElement( name ):
                nonlocal accessionNumber,filingDate,issuerCik
                nonlocal currentData,currentElement,documentType
                nonlocal rptOwnerCik,rptOwnerName
                nonlocal rptOwnerStreet1,rptOwnerStreet2,rptOwnerCity,rptOwnerState,rptOwnerZipCode
                nonlocal rptOwnerIsDirector,rptOwnerIsOfficer,rptOwnerIsTenPercentOwner
                nonlocal rptOwnerIsOther,rptOwnerOfficerTitle
                nonlocal state,xmlObjs

#                print("endElement name="+name )
 
                if name == 'documentType':
                    state = 1
                elif name == 'reportingOwner':
                    state = 10
                elif name == 'rptOwnerCik':
                    rptOwnerCik = currentData
#                    print("endElement: rptOwnerCik: "+rptOwnerCik );
                elif name == 'rptOwnerName':
                    rptOwnerName = currentData
                elif name == 'reportingOwnerRelationship':
#                    print("endElement: state = 10 for "+issuerCik)
                    state = 10
                    xmlObjs.append(['owner_rels',
                                    issuerCik,
                                    rptOwnerCik,
                                    filingDate,
                                    rptOwnerIsDirector,
                                    rptOwnerIsOfficer,
                                    rptOwnerIsTenPercentOwner,
                                    rptOwnerIsOther,
                                    rptOwnerOfficerTitle ])
                elif name == 'isDirector':
                    if currentData == 'true':
                        currentData = '1'
                    elif currentData == 'false':
                        currentData = '0'
                    rptOwnerIsDirector        = currentData
                elif name == 'isOfficer':
                    if currentData == 'true':
                        currentData = '1'
                    elif currentData == 'false':
                        currentData = '0'
                    rptOwnerIsOfficer         = currentData
                elif name == 'isTenPercentOwner':
                    if currentData == 'true':
                        currentData = '1'
                    elif currentData == 'false':
                        currentData = '0'
                    rptOwnerIsTenPercentOwner = currentData
                elif name == 'isOther':
                    if currentData == 'true':
                        currentData = '1'
                    elif currentData == 'false':
                        currentData = '0'
                    rptOwnerIsOther           = currentData
                elif name == 'officerTitle':
                    rptOwnerOfficerTitle      = currentData

            # StartElement handler
            def startElement(name, attrs):
                nonlocal accessionNumber,filingDate,issuerCik
                nonlocal currentData,currentElement,documentType
                nonlocal rptOwnerCik,rptOwnerName
                nonlocal rptOwnerStreet1,rptOwnerStreet2,rptOwnerCity,rptOwnerState,rptOwnerZipCode
                nonlocal rptOwnerIsDirector,rptOwnerIsOfficer,rptOwnerIsTenPercentOwner
                nonlocal rptOwnerIsOther,rptOwnerOfficerTitle
                nonlocal state,xmlObjs
            
 #               print("startElement: name="+name)
            
                currentElement = name
                currentAttrs   = attrs
                currentData    = ''      # don't carry over data from previous elements
                if name == 'documentType':
                    documentType = currentData
                elif name == 'reportingOwner':
                    rptOwnerCik       = ''
                    state = 20
                elif name == 'rptOwnerCik':
                    state = 21
                elif name == 'rptOwnerName':
                    state = 22
                elif name == 'reportingOwnerRelationship':
                    rptOwnerIsDirector        = 0
                    rptOwnerIsOfficer         = 0
                    rptOwnerIsTenPercentOwner = 0
                    rptOwnerIsOther           = 0
                    rptOwnerOfficerTitle      = ''
                    state = 30
                elif name == 'isDirector':
                    state = 30
                elif name == 'isOfficer':
                    state = 30
                elif name == 'isTenPercentOwner':
                    state = 30
                elif name == 'isOther':
                    state = 30
                elif name == 'officerTitle':
                    state = 30

            # CharacterData event handler
            def charData( data ):
                nonlocal currentData
                # change new lines to
                currentData = currentData + data

            # method to parse an embedded XML document
            def parseXMLDocument( xmlContent ):
                xmlParser = xml.parsers.expat.ParserCreate()
                xmlParser.StartElementHandler  = startElement
                xmlParser.CharacterDataHandler = charData
                xmlParser.EndElementHandler    = endElement
                try:
                    xmlParser.Parse( xmlContent )
                except TypeError as inst:
                    sys.stderr.write("XML parsing error for {0}: {1}\n".format(f,str(inst)))
                    
            tags = [
                ('description',  r'<DESCRIPTION>'),
                ('filename',     r'<FILENAME>'),
                ('sequence',     r'<SEQUENCE>'),
                ('type',         r'<TYPE>'),
                # ('textSTag',     r'<TEXT>'),
                # ('textETag',     r'</TEXT>'),
                ('value',           r'([A-Za-z0-9-.&;/ \[\]]+)$'),
                ('xmlSTag',      r'<XML>'),
                ('xmlETag',      r'</XML>'),
            ]
            tag_regex = '|'.join('(?P<%s>%s)' % pair for pair in tags)
            values = SECDict()
            state = 0
            kind  = ""
            xmlStartLoc = 0
            lastkind = ''
            for mo in re.finditer( tag_regex, docContent, re.MULTILINE|re.ASCII ):
                kind     = mo.lastgroup
                v        = mo.group(0)
                startLoc = mo.end()
#                print("parseXMLDocument kind="+kind)
                if kind == 'description':
                    lastkind = kind
                elif kind == 'filename':
                    lastkind = kind
                elif kind == 'sequence':
                    lastkind = kind
                elif kind == 'type':
                    lastkind = kind
                elif kind == 'value':
                    values[lastkind] = v
                elif kind == 'xmlSTag':
                    xmlStartLoc = mo.end()
                elif kind == 'xmlETag':
                    xmlEndLoc = mo.start()
                    formType = values.get('type')
                    if formType == '3' or formType == '4' or formType == '5':
                        xmlObjs.append(['documents',                     # table
                                        accessionNumber,                 # accessionNumber
                                        values.get('sequence',''),       # sequence
                                        formType,                        # type
                                        values.get('filename',''),       # filename
                                        'xml',                           # format
                                        values.get('description','')])   # description
                    xmlContent = docContent[xmlStartLoc+1:xmlEndLoc]
                    parseXMLDocument( xmlContent )
                    yield xmlObjs

                    
        # parse the SEC Header section of a filing
        #
        # This uses as state-based approach to keeping track of where we are
        # in the header.
        def parseHeader(hdrContent):
            nonlocal accessionNumber,filingDate,issuerCik
            nOwnrPrint = 0
            tags = [
                ('accessionNumber', r'ACCESSION NUMBER:' ),
                ('businessAddress', r'BUSINESS ADDRESS:'),
                ('changeDate',      r'DATE AS OF CHANGE:'),
                ('cik',             r'CENTRAL INDEX KEY:'),
                ('city',            r'CITY:'),
                ('companyData',     r'COMPANY DATA:'),
                ('companyName',     r'COMPANY CONFORMED NAME:'),
                ('documentCount',   r'PUBLIC DOCUMENT COUNT:'),
                ('filingData',      r'FILING VALUES:'),
                ('filingDate',      r'FILED AS OF DATE:'),
                ('fiscalYearEnd',   r'FISCAL YEAR END:'),
                ('irsNumber',       r'IRS NUMBER:' ),
                ('issuer',          r'ISSUER:'),
                ('issuer1',          r'SUBJECT COMPANY:'),
                ('mailAddress',     r'MAIL ADDRESS:'),
                ('ownerData',       r'OWNER DATA:' ),
                ('phone',           r'BUSINESS PHONE:'),
                ('reportingOwner',  r'REPORTING-OWNER:'),
                ('reportingPeriod', r'CONFORMED PERIOD OF REPORT:'),
                ('sic',             r'STANDARD INDUSTRIAL CLASSIFICATION:'),
                ('state',           r'STATE:'),
                ('stateOfInc',      r'STATE OF INCORPORATION:'),
                ('street1',         r'STREET 1:'),
                ('street2',         r'STREET 2:'),
                ('street3',         r'STREET 3:'),
                ('submissionType',  r'CONFORMED SUBMISSION TYPE:'),
                ('tradingSymbol',   r'TRADING SYMBOL:'),
                ('zip',             r'ZIP:'),
                ('value',           r'([A-Za-z0-9-&;,./ \[\]]+)$'),
                #            ('value',           r'([A-Za-z0-9-&/ \[\]]+)$'),
                ]
            values = SECDict()
            state = 0
            kind  = ""
            lastkind = ""
            accessionNumber = None
            tag_regex = '|'.join('(?P<%s>%s)' % pair for pair in tags)

            # loop through the document looking for values found by the
            # regular expression generated from the symbols above
            for mo in re.finditer( tag_regex, hdrContent, re.MULTILINE|re.ASCII ):
                kind     = mo.lastgroup
                startLoc = mo.end()
                v        = mo.group(0)
#                print("parseHeader: state:"+str(state)+"  kind="+kind+"  lastkind="+lastkind+"  v="+v+"  startLoc = "+str(startLoc)) # +": ("+hdrContent[startLoc:(startLoc+20)]+")")
                
                if accessionNumber == None and 'accessionNumber' in values:
                    accessionNumber = values.get('accessionNumber')
                if kind == 'businessAddress':
                    if state >= 10 and state < 20:
                        sicNumber = ''
                        issuerCik = values.get('cik')
                        sic = values.get('sic')
                        if sic:
                            mo = re.search(r'\[(\d+)\]',sic)
                            if mo:
                                sicNumber = mo.group(1)
                        entityName = values.get('companyName')
                        if accessionNumber and entityName and issuerCik:
                            if state == 10:
                                state = 12
                                yield [[ 'filings_entities',
                                         accessionNumber,
                                         issuerCik,
                                         'issuer'],
                                       [ 'entities',
                                         issuerCik,
                                         filingDate,
                                         values.pop('tradingSymbol',''),
                                         entityName,
                                         values.pop('irsNumber',''),
                                         sic,
                                         sicNumber,
                                         values.pop('stateOfInc',''),
                                         values.pop('fiscalYearEnd','')]]
                            elif state == 13:
                                state = 14
                                if values.get('city'):
                                    yield [['contacts',
                                            issueCik,
                                            filingDate,
                                            'issuer-mail',
                                            values.get('street1',''),
                                            values.get('street2',''),
                                            values.get('street3',''),
                                            values.get('city',''),
                                            values.get('state',''),
                                            values.get('zip',''),
                                            values.get('phone','')]]
                    elif state == 20:
                        state = 21
                    elif state == 22:
                        state = 23
                        if values.get('city'):
                            yield [['contacts',
                                    ownerCik,
                                    filingDate,
                                    'owner-business',
                                    values.get('street1',''),
                                    values.get('street2',''),
                                    values.get('street3',''),
                                    values.get('city',''),
                                    values.get('state',''),
                                    values.get('zip',''),
                                    values.get('phone','')]]
                    elif state == 24:
                        if values.get('city'):
                            yield [['contacts',
                                    ownerCik,
                                    filingDate,
                                    'owner-mail',
                                    values.get('street1',''),
                                    values.get('street2',''),
                                    values.get('street3',''),
                                    values.get('city',''),
                                    values.get('state',''),
                                    values.get('zip',''),
                                    values.get('phone','')]]
                elif kind == 'filingData':
                    ownerCik = values.get('cik')
                    ownerName = values.get('companyName')
                    if state >= 20:
#                    state = 22
                        if accessionNumber and ownerCik and ownerName:
                            yield [[ 'filings_entities',
                                     accessionNumber,
                                     ownerCik,
                                     'owner'],
                                   [ 'entities',
                                     ownerCik,
                                     filingDate,
                                     '',
                                     ownerName,
                                     '',
                                     '',
                                     '',
                                     '',
                                     '']]
                elif kind == 'issuer' or kind == 'issuer1':
                    if state == 0:
                        state = 10
                        values['phone'] = ''
                        filingDate = toDate(values.get('filingDate',''))
                        yield [[ "filings",
                                 values['accessionNumber'],
                                 values.pop('submissionType'),
                                 values.pop('documentCount'),
                                 toDate(values.get('reportingPeriod',missingDate)),
                                 filingDate,
                                 toDate(values.get('changeDate',missingDate))]]
                        
                    elif state >= 20 and state < 30:
                        if state == 24:
                            ownerType = 'owner-mail'
                        else:
                            ownerType = 'owner-business'
                        state = 10
                        ownerCik = values.get('cik')
                        if ownerCik and filingDate and values.get('city'):
                            yield [['contacts',
                                    ownerCik,
                                    filingDate,
                                    ownerType,
                                    values.get('street1',''),
                                    values.get('street2',''),
                                    values.get('street3',''),
                                    values.get('city',''),
                                    values.get('state',''),
                                    values.get('zip',''),
                                    values.get('phone','')]]
                    ownerName = None
                elif kind == 'mailAddress':
                    cik = values.get('cik')

                    if state >= 10 and state < 20:
                        sicNumber = ''
                        issuerCik = values.get('cik')
                        sic = values.get('sic')
                        if sic:
                            mo = re.search(r'\[(\d+)\]',sic)
                            if mo:
                                sicNumber = mo.group(1)
                        entityName = values.get('companyName')
                        if accessionNumber and entityName and issuerCik:
                            if state == 10:
                                state = 13
                                yield [[ 'filings_entities',
                                         accessionNumber,
                                         issuerCik,
                                         'issuer'],
                                       [ 'entities',
                                         issuerCik,
                                         filingDate,
                                         values.pop('tradingSymbol',''),
                                         entityName,
                                         values.pop('irsNumber',''),
                                         sic,
                                         sicNumber,
                                         values.pop('stateOfInc',''),
                                         values.pop('fiscalYearEnd','')]]
                            elif state == 12:
                                state = 14
                                if values.get('city'):
                                    yield [['contacts',
                                            issuerCik,
                                            filingDate,
                                            'issuer-business',
                                            values.get('street1',''),
                                            values.get('street2',''),
                                            values.get('street3',''),
                                            values.get('city',''),
                                            values.get('state',''),
                                            values.get('zip',''),
                                            values.get('phone','')]]
                    elif state == 21:
                        state = 24
                        if values.get('city'):
                            yield [['contacts',
                                    ownerCik,
                                    filingDate,
                                    'owner-business',
                                    values.get('street1',''),
                                    values.get('street2',''),
                                    values.get('street3',''),
                                    values.get('city',''),
                                    values.get('state',''),
                                    values.get('zip',''),
                                    values.get('phone','')]]
                        
                elif kind == 'reportingOwner':
                    if state == 14:
                        state = 20
                        if values.get('city'):
                            yield [['contacts',
                                    issuerCik,
                                    filingDate,
                                    'issuer-mail',
                                    values.get('street1',''),
                                    values.get('street2',''),
                                    values.get('street3',''),
                                    values.get('city',''),
                                    values.get('state',''),
                                    values.get('zip',''),
                                    values.get('phone','')]]
                    elif state == 13:
                        state = 20
                        if values.get('city'):
                            yield [['contacts',
                                    issuerCik,
                                    filingDate,
                                    'issuer-business',
                                    values.get('street1',''),
                                    values.get('street2',''),
                                    values.get('street3',''),
                                    values.get('city',''),
                                    values.get('state',''),
                                    values.get('zip',''),
                                    values.get('phone','')]]
                    elif state == 0:
                        state = 20
                        filingDate = toDate(values.get('filingDate',''))
                        yield [[ "filings",
                                 values['accessionNumber'],
                                 values.pop('submissionType'),
                                 values.pop('documentCount'),
                                 toDate(values.get('reportingPeriod',missingDate)),
                                 filingDate,
                                 toDate(values.get('changeDate',missingDate))]]
                elif kind == 'ownerData':
                    state = 21
                elif kind == 'value':
                    values[lastkind] = v
                lastkind = kind
            if state == 12:
                state = 0
                cik = values.get('cik')
                if cik and filingDate and values.get('city'):
                    yield [['contacts',
                            cik,
                            filingDate,
                            'issuer-mail',
                            values.get('street1',''),
                            values.get('street2',''),
                            values.get('street3',''),
                            values.get('city',''),
                            values.get('state',''),
                            values.get('zip',''),
                            values.get('phone','')]]
            elif state == 14:
                state = 0
                cik = values.get('cik')
                if cik and filingDate and values.get('city'):
                    yield [['contacts',
                            cik,
                            filingDate,
                            'issuer-mail',
                            values.get('street1',''),
                            values.get('street2',''),
                            values.get('street3',''),
                            values.get('city',''),
                            values.get('state',''),
                            values.get('zip',''),
                            values.get('phone','')]]
            elif state == 22:
                state = 0
                cik = values.get('cik')
                if cik and filingDate and values.get('city'):
                    yield [['contacts',
                            cik,
                            filingDate,
                            'owner-mail',
                            values.get('street1',''),
                            values.get('street2',''),
                            values.get('street3',''),
                            values.get('city',''),
                            values.get('state',''),
                            values.get('zip',''),
                            values.get('phone','')]]
            elif state == 24:
                state = 0
                cik = values.get('cik')
                if cik and filingDate and values.get('city'):
                    yield [['contacts',
                            cik,
                            filingDate,
                            'owner-mail',
                            values.get('street1',''),
                            values.get('street2',''),
                            values.get('street3',''),
                            values.get('city',''),
                            values.get('state',''),
                            values.get('zip',''),
                            values.get('phone','')]]

        #
        # Now we're in the main parsing routine where we are looking
        # for sections of the SEC filing document consisting of a
        # SEC-HEADER and one or more DOCUMENT elements
        docTags = [
            ('headerSTag',   r'<SEC-HEADER>'),
            ('headerETag',   r'</SEC-HEADER>'),
            ('documentSTag', r'<DOCUMENT>'),
            ('documentETag', r'</DOCUMENT>'),
        ]
        with open( fileName, "r" ) as inFile:
            fileContent = inFile.read()
            inFile.close()
            docTag_regex =  '|'.join('(?P<%s>%s)' % pair for pair in docTags)
            documentStartLoc = 0
            documentEndLoc   = 0
            documentContentt = ''
            accessionNumber  = ''
            for mo in re.finditer( docTag_regex, fileContent, re.MULTILINE|re.ASCII ):
                # Header start tag
                if mo.lastgroup == 'headerSTag':
                    documentStartLoc = mo.start(0)

                # Header end tag
                elif mo.lastgroup == 'headerETag':
                    documentEndLoc = mo.start(0)
                    documentContent = fileContent[documentStartLoc:documentEndLoc]
                    for obj in parseHeader(documentContent):
                        for row in obj:
                            if row[0] == 'filings':
                                accessionNumber = row[1]
                        outputRows.extend(obj)

                # Document start tag
                elif mo.lastgroup == 'documentSTag':
                    documentStartLoc = mo.start(0)

                # Document end tag
                elif mo.lastgroup == 'documentETag':
                    documentEndLoc = mo.start(0)
                    documentContent = fileContent[documentStartLoc:documentEndLoc]
                    for obj in parseDocument(documentContent,accessionNumber):
                        outputRows.extend(obj)
            
if __name__ == "__main__":
    with io.StringIO("",newline='\n') as csvStrings:
        csvWriter = csv.writer(csvStrings, delimiter='|',lineterminator='\n',quoting=csv.QUOTE_MINIMAL )
        outputRows = []

        # loop through the program arguments, parsing the files passed in
        for f in sys.argv[1:]:
            try:
                SECFiling.parse( f, outputRows )
            except xml.parsers.expat.ExpatError as inst:
                sys.stderr.write("XML parsing error for {0}: {1}\n".format(f,str(inst)))
        # Write all of the data to a CSV file
        # We precede each line with the table it goes into, making
        # it easy to separate the rows out into separate CSV files later
        csvWriter.writerows(outputRows)
        print(csvStrings.getvalue())
