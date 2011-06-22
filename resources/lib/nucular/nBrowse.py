
"""
Web based nucular archive browser support (CGI/mod_python).
Please see HTML documentation in the "doc" directory and associated READMES
in "cgi" and "modpy" directories.
"""

import time
#import nucular
from nucular import Nucular
import urllib
#import entry
from nucular import entry
import types
#import stringAnnotator
from nucular import stringAnnotator
#import specialValues
from nucular import specialValues
import gc

PROXIMATE = True

TEMPLATE = """
<html>
<head>
<title> %(title)s </title>
<head>
<body style="background-color:#999999; font-family: Arial, Helvetica, sans-serif">
<center>

<table border>
    <tr><td style="background-color:#eeeeff" colspan="2">
    This is a demo and test program for exploring a NUCULAR fielded/full text index.
    Enter words or phrases in the search box to find entries in the archive.<br>
    For more information on NUCULAR please go
    <a href="http://nucular.sourceforge.net">http://nucular.sourceforge.net</a>.
    </td></tr>
<tr>
    <td colspan="2" width="100%(pct)s" style="background-color:#dddd99"><b>Nucular browse ::</b> %(title)s </td>
</tr>
<tr>
    <td valign="top" height="90%(pct)s" width="20%(pct)s" style="background-color:#dddd99">
    %(form)s
    </td>
    <td valign="top" style="background-color:#dddddd">
    entries:
    <table border width="100%(pct)s">
    <tr>
        <td valign="top" height="10%(pct)s">
        %(listings)s
        <center>
        %(prevLink)s
        [[ %(windowText)s ]]
        %(nextLink)s
        </center>
        </td>
    </tr>
    <tr>
        <td valign="top" style="background-color:#ffffff"> %(detail)s </td>
    </tr>
    </table>
    </td>
</tr>
</table>
<br>
elapsed:
<pre>
%(elapsed)s
</pre>
</center>
</body>
</html>
"""

EDITTEMPLATE = """
<html>
<head>
<title> %(title)s </title>
</head>
<body>

<h2>%(title)s</h2>
<p>
<b>Warning</b>: too many edits without follow up aggregation operations
will result in decreased performance.
</p>

<form action="%(script)s" name="submitForm" id="submitForm">
%(hidden)s

%(TextBoxes)s

<input type="hidden" name="mode" value="%(mode)s">
<input type="submit" name="Change" value="Change">
</form>
</body>
</html>
"""

INDEXINGTEMPLATE = """
<html>
<head>
<title> %(title)s </title>
</head>
<body>

%(return)s

<h2>%(title)s</h2>

%(indexing)s

<br>
%(return)s

</body>
</html>
"""
    
INPUTTEMPLATE = """
<a onclick="alert('for example: \\n%(examples)s')"><u>%(name)s</u></a>
<br>
<input type="text" name="%(fieldname)s" value="%(fieldvalue)s">
<br>
"""

FORMTEMPLATE = """
<form action="%(script)s" name="submitForm" id="submitForm">
<input type="hidden" name="FocusId" value="%(FocusId)s">
<input type="submit" name="SEARCH" value="SEARCH">
<br>
<a onclick="alert('for example: %(freeExamples)s')"><u><b><em>proximate full text</b></em></u></a>
<br>
<input type="text" name="FREETEXT" value="%(FREETEXT)s">
<br>
%(otherInputs)s
<br>
<input type="button" value="clear" onclick="clearform()">
</form>

<script>
function clearform() {
    var form = document.getElementById("submitForm");
    var elements = form.elements;
    for (var i=0; i<elements.length; i++) {
        var field = elements[i];
        if (field.type=="text") {
            field.value = "";
        }
    }
}
</script>
"""


DELIMITPAIRS = [
    ( '<span style="background-color:cyan">', '</span>' ),
    ( '<span style="background-color:magenta">', '</span>' ),
    ( '<span style="background-color:yellow">', '</span>' ),
    ( '<span style="background-color:pink">', '</span>' ),
    ]

class HtmlBrowse:

    truncateSize = 2000 # truncate intermediate results at this size
    materializeSize = 5 # always materialize when results fall below this size

    def __init__(self, archivePath, script, labelAttribute, arguments,
                 threaded=False, displayLimit=20, editable=True, preformattedText=False,
                 withFields=True, pathTranslation=None):
#        parameters.alloc(self, ("browse", archivePath))
        self.editable = editable
        self.withFields = withFields
        self.archivePath = archivePath
        self.script = script
        self.labelAttribute = labelAttribute
        self.archive = Nucular.Nucular(archivePath, threaded=threaded)
        if pathTranslation:
            #pr "<br>translating", pathTranslation, "<br>"
            (fromPath, toPath) = pathTranslation
            self.archive.addURLTranslation(fromPath, toPath)
        self.arguments = arguments
        self.displayLimit = displayLimit
        self.focusId = None # set later
        self.result = None
        self.Template = TEMPLATE
        self.EditTemplate = EDITTEMPLATE
        self.IndexingTemplate = INDEXINGTEMPLATE
        self.InputTemplate = INPUTTEMPLATE
        self.FormTemplate = FORMTEMPLATE
        self.DelimitPairs = DELIMITPAIRS
        self.preformattedText = preformattedText

#    def __del__(self):
#        parameters.dealloc(self)
        
    def link(self, arguments=None, addDict=None, attr=None, attrValue=None):
        "return a GET URL to link back to this script."
        if arguments is None:
            arguments = self.arguments
        if addDict is None:
            addDict = {}
        D = {}
        D.update(arguments)
        D["mode"] = "view" # unless explicitly overridden using attr
        # remove any key that has a tilde (only for updates!)
        for k in D.keys():
            if "~" in k:
                del D[k]
        for a in addDict:
            if D.has_key(a):
                D[a] = "%s %s" % (D[a], addDict[a])
        if attr:
            D[attr] = attrValue
        argsfmt = [ "%s=%s" % (attr, urllib.quote(str(val))) for (attr, val) in D.items() ]
        allargs = "&".join(argsfmt)
        return "%s?%s" % (self.script, allargs)
    
    def href(self, anchorText, arguments=None, addDict=None, attr=None, attrValue=None):
        "Return an 'a href' tag linking back to this script with arguments."
        link = self.link(arguments, addDict, attr, attrValue)
        return '<a href="%s">%s</a>' % (link, anchorText)

    def idLink(self, identity):
        descr = self.archive.describe(identity)
        ddict = descr.asDictionary()
        label = ddict.get(self.labelAttribute, "")
        label += " [%s]" %identity
        return self.href(label, attr="FocusId", attrValue=identity)
    
    def getIds(self):
        "Select a list of entry Id's based on query (or default to allIds prefix), and select a focus Id."
        archive = self.archive
        arguments = self.arguments
        fields = self.fields()
        freeTextPrefixes = arguments.get("FREETEXT", "").split()
        idList = focusId = None
        self.result = None
        if not (fields or freeTextPrefixes):
            self.query = None
            idList = archive.allIds(self.displayLimit)
            if not idList:
                focusId = None
            else:
                focusId = idList[len(idList)/2]
        else:
            self.query = query = archive.Query()
            for field in fields:
                value = fields[field]
                for prefix in value.split():
                    query.attributeWord(field, prefix)
            if not PROXIMATE:
                for prefix in freeTextPrefixes:
                    query.anyWord(prefix)
            else:
                if freeTextPrefixes:
                    query.proximateWords(freeTextPrefixes)
            (result, status) = query.evaluate(truncateSize=self.truncateSize, materializeSize=self.materializeSize)
            if not result:
                idList = []
            else:
                self.result = result
                idList = result.identities()
                if idList:
                    focusId = idList[0]
        self.idList = idList
        return self.getFocusId(focusId)
    
    def getFocusId(self, default=None):
        "choose a FocusId to display in detail."
        focusId = default
        focusValue = self.arguments.get("FocusId", None)
        if focusValue:
            focusId = focusValue
        if not focusId:
            focusId = self.focusId
        self.focusId = focusId
        return focusId
    
    def getAttributeExamples(self):
        "collect example attribute values"
        descriptions = self.descriptions
        idList = self.viewIdList
        attrExamples = {}
        freeWords = {}
        for i in idList:
            description = descriptions[i]
            description.words(attribute=None, collection=freeWords, attributeWords=attrExamples)
        self.attrExamples = attrExamples
        self.freeWords = freeWords
        
    def title(self):
        return "archive: %s; id: %s" % (repr(self.archivePath), repr(self.getFocusId()))
        #return "archive: %s; id: %s; flds: %s" % (repr(self.archivePath), repr(self.focusId), self.fields())
    
    def page(self):
        "determine summary display page number."
        page = 0
        PAGE = self.arguments.get("PAGE", "0")
        try:
            page = int(PAGE)
        except ValueError:
            page = 0
        return page
    
    def getDescriptions(self):
        "get descriptions for summary page."
        dL = self.displayLimit
        idList = self.idList
        page = self.page()
        startIndex = page*dL
        if startIndex>=len(idList):
            startIndex = 0
        endIndex = min(startIndex+dL, len(idList))
        viewIdList = idList[startIndex:endIndex]
        self.windowText = "entries %s to %s of %s total" % (startIndex, endIndex-1, len(idList))
        descriptions = {}
        archive = self.archive
        for i in viewIdList:
            description = archive.describe(i)
            descriptions[i] = description
        self.descriptions = descriptions
        self.viewIdList = viewIdList
        
    def listings(self):
        "construct listings display for summary page."
        idList = self.viewIdList
        descriptions = self.descriptions
        displayList = [self.makeDisplay(descriptions[i]) for i in idList]
        if displayList:
            listings = "<br>\n".join(displayList)
        else:
            listings = "<b><em>[[ query evaluates to empty ]]</em></b>"
        return listings
    
    def makeDisplay(self, entry0):
        "make a summary display for a given entry."
        attrDict = entry0.attrDict()
        identity = entry0.identity()
        labelvalue = str(identity)
        labelAttribute = self.labelAttribute
        if attrDict.has_key(labelAttribute):
            labelList = attrDict[labelAttribute]
            labelValues = [str(x) for x in labelList]
            labelvalue = " :: ".join(labelValues)
            labelvalue += " (%s)" % identity
        return self.href(entry.quote(labelvalue), attr="FocusId", attrValue=identity)
    
    def fieldName(self, attribute):
        "choose a CGI fieldname for a nucular attribute name"
        return "attr_%s" % attribute
    
    def fieldValue(self, attribute):
        "get a value for a nucular attribute from the CGI parameters."
        n = self.fieldName(attribute)
        return self.arguments.get(n, "")
    
    def fields(self):
        "find defined nucular fields encoded in CGI parameters."
        result = {}
        prefix = "attr_"
        for n in self.arguments:
            if n.startswith(prefix):
                fieldname = n[len(prefix):]
                result[fieldname] = self.arguments[n]
        return result
    
    def form(self):
        "generate the HTML form for the search parameters."
        displayLimit = self.displayLimit
        focusId = self.arguments.get("FocusId", "")
        freeList = self.freeWords.keys()[:displayLimit]
        freeExamples = (r"\n").join(freeList)
        attrExamples = self.attrExamples
        attrChoices = attrExamples.keys()#[:displayLimit]
        attrChoices.sort()
        attrList = []
        if self.withFields:
            for a in attrChoices:
                examplesList = attrExamples[a].keys()[:displayLimit]
                examples = (r"\n").join(examplesList)
                aD = {}
                aD["name"] = a
                aD["examples"] = examples
                aD["fieldname"] = self.fieldName(a)
                aD["fieldvalue"] = self.fieldValue(a)
                fmt = self.InputTemplate % aD
                attrList.append(fmt)
        FREETEXT = self.arguments.get("FREETEXT", "")
        D = {}
        D["script"] = self.script
        D["FocusId"] = focusId
        D["otherInputs"] = "\n".join(attrList)
        D["freeExamples"] = freeExamples
        D["FREETEXT"] = FREETEXT
        return self.FormTemplate % D
    
    def editFieldName(self, attr, index):
        "generate a CGI name for an edit field"
        return "field~%s~%s" % (attr, index)
    
    def parseFieldName(self, cgiParameterName):
        "from a CGI edit field name, find the nucular field name"
        s = cgiParameterName.split("~")
        if len(s)==3 and s[0]=="field":
            attr = s[1]
            numstr = s[2]
            try:
                num = int(numstr)
            except:
                return None
            return (attr, num)
        return None # fail
    
    def textBoxes(self, attr, values):
        "generate text areas for detail display edit mode."
        L = []
        for i in xrange(len(values)):
            fieldname = self.editFieldName(attr, i)
            v = values[i]
            box = '<textarea name="%s" cols="40" rows="4">%s</textarea>' % (fieldname, v)
            L.append(box)
        return L

    def valueRepr(self, val):
        "prepare value for display in detail area."
        # pr "valueRepr", type(val), val.__class__.__name__, "<hr>"
        if isinstance(val, specialValues.SpecialValue):
            val = val.html(self)
        elif type(val) is types.StringType:
            val = entry.quote(val)
        if self.preformattedText:
            return "<pre>%s</pre>" % val
        return repr(val)

    def matchFocuses(self, dvals):
        result = []
        for d in dvals:
            if isinstance(d, stringAnnotator.Annotator):
                summary = d.summarize()
                if summary:
                    if summary[0][0]!=0 or len(summary)>1: # show line number if not on first line
                        L = ["<table>"]
                        for (lineno, linetext) in summary:
                            L.append('<tr><th align="right" style="background-color:#eeddaa">[line %s]</th><td>%s</td></tr>' % (lineno, linetext))
                        L.append("</table>")
                        result.append("\n".join(L))
                    else:
                        result.append(summary[0][1])
        return result
    
    def detail(self, edit=False):
        "generate detail display for selected entry"
        SV = specialValues.SpecialValue
        detail = "no entry selected for detailed display"
        focusId = self.getFocusId()
        archive = self.archive
        linkAttributes = archive.linkAttributes()
        if focusId is not None:
            description = archive.describe(focusId)
            delimitedDict = attrDict = description.attrDict()
            if self.result is not None:
                delimitedDict = self.result.annotateDictionary(description, self.DelimitPairs)
            attributes = attrDict.keys()
            attributes.sort()
            rows = []
            # match displays
            for a in attributes:
                dvals = delimitedDict[a]
                matchFocuses = self.matchFocuses(dvals)
                if matchFocuses:
                    matchDisplay = "<br>\n".join(matchFocuses)
                    row = '<tr> <th valign="top" align="right">%s</th> <td> %s </td> </tr>' % (a, matchDisplay)
                    rows.append(row)
            # normal displays
            for a in attributes:
                vals = attrDict[a]
                dvals = delimitedDict[a]
                if a in linkAttributes:
                    # format cross links
                    entries = [ archive.describe(v) for v in vals ]
                    reprs = [ self.makeDisplay(e) for e in entries ]
                elif edit and self.editable:
                    # format as input form
                    reprs = self.textBoxes(a, vals)
                else:
                    #reprs = map(self.valueRepr, dvals)
                    reprs = list(vals)
                    for i in xrange(len(reprs)):
                        vi = vals[i]
                        di = dvals[i]
                        if isinstance(vi, SV):
                            reprs[i] = vi.html(self)
                        else:
                            reprs[i] = self.valueRepr(dvals[i])
                valDisplay = "<br>\n".join(reprs)
                row = '<tr> <th valign="top" align="right" style="background-color:ff9944"><em>%s</em></th> <td style="background-color:#bbbbbb" > %s </td> </tr>' % (a, valDisplay)
                rows.append(row)
            table = "<table> %s </table>" % ("\n".join(rows))
            if edit:
                detail = table
            else:
                links = indexing = self.href("view indexing", attr="mode", attrValue="indexing")
                if self.editable:
                    editlink = self.href("edit entry", attr="mode", attrValue="edit")
                    links = "%s || %s" % (indexing, editlink)
                detail = "%s\n<br>%s" % (table, links)
        return detail
    
    def Edit(self):
        "generate edit form for edit entry feature"
        D = {}
        D["script"] = self.script
        #D["FocusId"] = self.FocusId
        D["TextBoxes"] = self.detail(edit=True)
        hiddenList = []
        for (name, value) in self.arguments.items():
            if name!="mode":
                hiddenList.append('<input type="hidden" name="%s" value="%s">' % (name, value))
        D["hidden"] = "\n".join(hiddenList)
        D["mode"] = "change"
        D["title"] = "change entry: "+self.title()
        return self.EditTemplate % D
    
    def Indexing(self):
        "display indexing detail for selected entry"
        focusId = self.getFocusId()
        archive = self.archive
        focusIndexing = "<br><b>Cannot index. No focus selected.</b></br>"
        if focusId is not None:
            L = []
            entry = archive.describe(focusId)
            (descrIndexDict, attrIndexDict, AttrWordDict, freeWordsDict) = archive.index(entry, test=True)
            for (title, D) in [ ("description", descrIndexDict),
                                ("attribute", attrIndexDict),
                                ("attribute/word", AttrWordDict),
                                ("free word", freeWordsDict) ]:
                keys = D.keys()
                keys.sort()
                L.append("<br><br><b><em>%s</em></b></br>" % title)
                for k in keys:
                    v = D[k]
                    L.append("<br> %s[%s] = %s" % (title, k, v))
            focusIndexing = "\n".join(L)
        D = {}
        title = self.title()
        D["title"] = "indexing for %s [%s]" % (repr(focusId), title)
        D["indexing"] = focusIndexing
        D["return"] = self.href("return to view %s" % title, attr="mode", attrValue="view")
        return self.IndexingTemplate % D
    
    def changeEntry(self):
        "replace editted entry with values provided by edit form."
        focusId = self.getFocusId()
        archive = self.archive
        if not focusId:
            raise ValueError, "focusId not set"
        #entry = archive.describe(focusId)
        archive.remove(focusId)
        newData = {}
        for (name, value) in self.arguments.items():
            test = self.parseFieldName(name)
            if test:
                (attr, number) = test
                L = newData.get(attr, [])
                L.append(value)
                newData[attr] = L
        newEntry = entry.Entry(focusId, newData)
        archive.index(newEntry)
        # STORE IT (not lazy) OR NOTHING WILL HAPPEN!
        archive.store(lazy=False)
        
    def BrowsePage(self):
        "generate the 'browse' main page"
        now = time.time()
        gc.disable()
        mode = self.arguments.get("mode", "view")
        if mode=="edit":
            return self.Edit()
        elif mode=="indexing":
            return self.Indexing()
        elif mode=="change":
            self.changeEntry()
            # proceed to search functionality below
        elif mode!="view":
            raise ValueError, "unknown mode "+repr(mode)
        self.getIds()
        self.getDescriptions()
        self.getAttributeExamples()
        title = self.title()
        form = self.form()
        listings = self.listings()
        detail = self.detail()
        page = self.page()
        nextLink = prevLink = ""
        if (page+1)*self.displayLimit<len(self.idList):
            nextLink = self.href("NEXT&gt;&gt;", attr="PAGE", attrValue=page+1)
        if page>0:
            prevLink = self.href("&lt;&lt;PREV", attr="PAGE", attrValue=page-1)
        D = {}
        D["pct"] = "%"
        D["title"] = title
        D["form"] = form
        D["listings"] = listings
        D["nextLink"] = nextLink
        D["prevLink"] = prevLink
        D["detail"] = detail
        D["elapsed"] = str(time.time()-now)
        D["windowText"] = self.windowText
        if self.query:
            D["elapsed"] += "\n\n"+self.query.report()
        else:
            D["elapsed"] += "\n\n query missing"
        result = self.Template % D
        #for x in self.__dict__.keys():
        #    del self.__dict__[x]
        #print gc.collect()
        return result

def Browse(archiveDirectory, scriptName, displayAttribute, arguments,
           threaded=False, editable=False, preformattedText=False, withFields=True, pathTranslation=None):
    "return browse page for archive based on arguments dictionary"
    B = HtmlBrowse(archiveDirectory, scriptName, displayAttribute, arguments, threaded,
                   editable=editable, preformattedText=preformattedText, withFields=withFields,
                   pathTranslation=pathTranslation)
    return B.BrowsePage()

def cgiBrowse(archiveDirectory, scriptName, displayAttribute, cgiForm,
              threaded=False, editable=True, preformattedText=False, withFields=True,
              pathTranslation=None):
    "return browse page for archive based on cgi parameters"
    arguments = {}
    for k in cgiForm.keys():
        v = cgiForm[k]
        try:
            val = v.value
        except:
            pass
        else:
            arguments[k] = val
    #return repr(arguments)
    return Browse(archiveDirectory, scriptName, displayAttribute, arguments, threaded,
                  editable=editable, preformattedText=preformattedText, withFields=withFields,
                  pathTranslation=pathTranslation)

if __name__=="__main__":
    import sys
    ok = False
    try:
        testArchive = sys.argv[1]
        ok = True
    finally:
        if not ok:
            print "please provide a path to a nucular archive"
    arguments = {}
    #arguments["FocusId"] = "M10.7" # "M1001"
    arguments["FREETEXT"] = "isl"
    #arguments["attr_government"] = "co"
    print Browse(testArchive, "test.cgi", "name", arguments)
