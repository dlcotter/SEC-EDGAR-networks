import collections
import csv
import io
import re
import sys
import xml.parsers.expat

class SECDict(dict):
    def __missing__(self,key):
        return ""
    
def toDate(str):
    if len(str) > 0:
        return str[0:4]+'-'+str[4:6]+'-'+str[6:8]
    else:
        return "1990-01-01"

class SECFiling:
    """An SEC Filing"""

    
    objType = "SECFiling"

    global Token
    Token = collections.namedtuple( 'Token', ['type','value'])
    
    def __init__(self, accessionNumber, submissionType, docCount, reportingDate, filingDate, changeDate ):
        self.objType = SECFiling.objType
        self.accessionNumber = accessionNumber
        self.submissionType = submissionType
        self.docCount = docCount
        self.reportingDate = reportingDate
        self.filingDate = filingDate
        self.changeDate = changeDate

    def setAccessionNumber(self, accessionNumber):
        self.accessionNumber = accessionNumber
            
    def subType(self, subType):
        self.subType = subType

    def docCount(self, docCount):
        self.docCount = docCount

    def reportingDate(self, reportingDate):
        self.reportingDate = reportingDate

    def filingDate(self, filingDate):
        self.filingDate = filingDate

    def changeDate(self, changeDate):
        self.changeDate = changeDate

    def toCSV(self,csvWriter):
        csvWriter.writerow([self.objType,
                            self.accessionNumber,
                            self.submissionType,
                            self.docCount,
                            self.reportingDate,
                            self.filingDate,
                            self.changeDate])

    def parse(fileName,csvWriter):
        global parse
        accessionNumber = ''
        issuerCik       = ''
        filingDate      = ''
        missingDate     = '1990-01-01'
        
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
                    rptOwnerIsDirector        = currentData
                elif name == 'isOfficer':
                    rptOwnerIsOfficer         = currentData
                elif name == 'isTenPercentOwner':
                    rptOwnerIsTenPercentOwner = currentData
                elif name == 'isOther':
                    rptOwnerIsOther           = currentData
                elif name == 'officerTitle':
                    rptOwnerOfficerTitle      = currentData

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

            def charData( data ):
                nonlocal currentData
                currentData = data
#                print( "charData: data="+currentData )
            
            def parseXMLDocument( xmlContent ):
                xmlParser = xml.parsers.expat.ParserCreate()
                xmlParser.StartElementHandler  = startElement
                xmlParser.CharacterDataHandler = charData
                xmlParser.EndElementHandler    = endElement
                xmlParser.Parse( xmlContent )
            
            tags = [
                #                ('filenameTag',  r'<FILENAME>'),
                #                ('textSTag',     r'<TEXT>'),
                #                ('textETag',     r'</TEXT>'),
                ('xmlSTag',      r'<XML>'),
                ('xmlETag',      r'</XML>'),
            ]
            xmlStartLoc = 0
            tag_regex = '|'.join('(?P<%s>%s)' % pair for pair in tags)
            for mo in re.finditer( tag_regex, docContent, re.MULTILINE|re.ASCII ):
                kind     = mo.lastgroup
                v        = mo.group(0)
#                print("parseXMLDocument kind="+kind)
                if kind == 'xmlSTag':
                    xmlStartLoc = mo.end()
                elif kind == 'xmlETag':
                    xmlEndLoc = mo.start()
                    xmlContent = docContent[xmlStartLoc+1:xmlEndLoc]
                    parseXMLDocument( xmlContent )
                    yield xmlObjs
        
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
                ('issuer',          r'ISSUER:'),
                ('mailAddress',     r'MAIL ADDRESS:'),
                ('ownerData',       r'OWNER DATA:' ),
                ('phone',           r'BUSINESS PHONE:'),
                ('reportingOwner',  r'REPORTING-OWNER:'),
                ('reportingPeriod', r'CONFORMED PERIOD OF REPORT:'),
                ('sic',             r'STANDARD INDUSTRIAL CLASSIFICATION:'),
                ('state',           r'STATE:'),
                ('stateOfInc',      r'STATE OF INCORPORATION:'),
                ('street1',         r'STREET 1:'),
                ('submissionType',  r'CONFORMED SUBMISSION TYPE:'),
                ('tradingSymbol',   r'TRADING SYMBOL:'),
                ('zip',             r'ZIP:'),
                ('value',           r'([A-Za-z0-9-&;/ \[\]]+)$'),
                #            ('value',           r'([A-Za-z0-9-&/ \[\]]+)$'),
                ]
            values = SECDict()
            state = 0
            kind  = ""
            lastkind = ""
            accessionNumber = None
            tag_regex = '|'.join('(?P<%s>%s)' % pair for pair in tags)
 #           print("tag_regex: "+tag_regex )
            for mo in re.finditer( tag_regex, hdrContent, re.MULTILINE|re.ASCII ):
                kind     = mo.lastgroup
                startLoc = mo.end()
                v        = mo.group(0)
#                print("parseHeader: state:"+str(state)+"  kind="+kind+"  lastkind="+lastkind+"  v="+v+"  startLoc = "+str(startLoc)) # +": ("+hdrContent[startLoc:(startLoc+20)]+")")
                
                if accessionNumber == None and 'accessionNumber' in values:
                    accessionNumber = values.get('accessionNumber')
                if kind == 'businessAddress':
                    if state == 10:
                        state = 11
                        sicNumber = ''
                        sic = values.get('sic')
                        if sic:
                            mo = re.search(r'\[(\d+)\]',sic)
                            if mo:
                                sicNumber = mo.group(1)
                                issuerCik = values.get('cik')
                                entityName = values.get('companyName')
                                if accessionNumber and entityName and issuerCik:
                                    yield [[ 'filings-entities',
                                             accessionNumber,
                                             issuerCik],
                                           [ 'entities',
                                             issuerCik,
                                             values.pop('tradingSymbol',''),
                                             entityName,
                                             'issuer',
                                             values.pop('irsNumber',''),
                                             sic,
                                             sicNumber,
                                             values.pop('stateOfInc',''),
                                             values.pop('fiscalYearEnd','') ]]
                elif kind == 'filingData':
                    state = 22
                    ownerCik = values.get('cik')
                    ownerName = values.get('companyName')
                    if accessionNumber and ownerCik and ownerName:
                        yield [[ 'filings-entities',
                                 accessionNumber,
                                 ownerCik],
                               [ 'entities',
                                 ownerCik,
                                 '',
                                 ownerName,
                                 'owner',
                                 '',
                                 '',
                                 '',
                                 '',
                                 '' ]]
                elif kind == 'issuer':
                    if state == 0:
                        state = 10
                        filingDate = toDate(values.get('filingDate',''))
                        yield [[ "filings",
                                 values['accessionNumber'],
                                 values.pop('submissionType'),
                                 values.pop('documentCount'),
                                 toDate(values.get('reportingPeriod',missingDate)),
                                 filingDate,
                                 toDate(values.get('changeDate',missingDate))]]
                        
                    elif state >= 20 and state < 30:
                        state = 10
                        ownerCik = values.get('cik')
                        if ownerCik and filingDate:
                            yield [['contacts',
                                    ownerCik,
                                    filingDate,
                                    'owner',
                                    values.get('street1',''),
                                    values.get('street2',''),
                                    values.get('street3',''),
                                    values.get('city',''),
                                    values.get('state',''),
                                    values.get('zip',''),
                                    values.get('phone','')]]
                elif kind == 'mailAddress':
                    cik = values.get('cik')
                    if state >= 11 and state < 20 and cik and filingDate:
                        state = 12
                        yield [['contacts',
                                cik,
                                filingDate,
                                'issuer',
                                values.get('street1',''),
                                values.get('street2',''),
                                values.get('street3',''),
                                values.get('city',''),
                                values.get('state',''),
                                values.get('zip',''),
                                values.get('phone','')]]
                elif kind == 'reportingOwner':
                    cik = values.get('cik')
                    if state >= 12 and state < 20 and cik and filingDate:
                        state = 20
                        yield [['contacts',
                                cik,
                                filingDate,
                                'issuer',
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
                if cik and filingDate:
                    yield [['contacts',
                            cik,
                            filingDate,
                            'issuer',
                            values.get('street1',''),
                            values.get('street2',''),
                            values.get('street3',''),
                            values.get('city',''),
                            values.get('state',''),
                            values.get('zip',''),
                            values.get('phone','')]]
                                                    
            if state == 22:
                state = 0
                cik = values.get('cik')
                if cik and filingDate:
                    yield [['contacts',
                            cik,
                            filingDate,
                            'owner',
                            values.get('street1',''),
                            values.get('street2',''),
                            values.get('street3',''),
                            values.get('city',''),
                            values.get('state',''),
                            values.get('zip',''),
                            values.get('phone','')]]
                    
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
                if mo.lastgroup == 'headerSTag':
                    documentStartLoc = mo.start(0)
                elif mo.lastgroup == 'headerETag':
                    documentEndLoc = mo.start(0)
                    documentContent = fileContent[documentStartLoc:documentEndLoc]
                    for obj in parseHeader(documentContent):
                        for row in obj:
                            if row[0] == 'filings':
                                accessionNumber = row[1]
                        csvWriter.writerows(obj)
#                        print("processing AN: "+accessionNumber)
                elif mo.lastgroup == 'documentSTag':
                    documentStartLoc = mo.start(0)
                elif mo.lastgroup == 'documentETag':
                    documentEndLoc = mo.start(0)
                    documentContent = fileContent[documentStartLoc:documentEndLoc]
                    for obj in parseDocument(documentContent,accessionNumber):
                        csvWriter.writerows(obj)
                    
            
            
if __name__ == "__main__":
    with io.StringIO("",newline='\n') as csvStrings:
        csvWriter = csv.writer(csvStrings, delimiter='|',quoting=csv.QUOTE_MINIMAL )
        for f in sys.argv[1:]:
            try:
                SECFiling.parse( f, csvWriter)
            except xml.parsers.expat.ExpatError as inst:
                sys.stderr.write("XML parsing error for {0}: {1}\n".format(f,str(inst)))
        print(csvStrings.getvalue())
