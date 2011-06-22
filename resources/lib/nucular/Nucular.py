
"""
Top level Nucular archive collection interface
"""

import types
import pquery
import pfilter
import time
import marshal
import entry
import os
import parameters
import specialValues
from frames import fltree

OVERFLOWSTATUS = "overflow"
COMPLETESTATUS = "complete"
TRUNCATEDSTATUS = "truncated"
MAXBUFFERDEFAULT = parameters.QueryEvalMax
SWITCHFACTORDEFAULT = parameters.QuerySwitchFactor
VALUETRUNCATION = parameters.ValueIndexLengthTruncation
QUERYTIMELIMIT = parameters.DefaultQueryTimeLimit

# system op codes for initialization
URLTRANSLATE = "U"
ATTTRANSLATE = "A"
FREETEXTONLY = "F"
COUNTERVALUE = "C"
AUTOCLEAN = "CL"

class Nucular:
    """session interface for interacting with an archive"""

    lazyIndexing = True # set to make updates invisible until aggregation by default
    _autoClean = False  # set to automatically delete retired files
    threadSeparator = "\\"
    childAttributeName = "[child]"
    parentAttributeName = "[parent]"
    valueTruncation = VALUETRUNCATION
    idAttribute = "i"
    abbreviationCounter = 0
    autoAbbreviate = True # set to replace attribute names by abbreviations automatically
    # XXXX AUTOABBREV IS UNSAFE WHERE CONCURRENT ACCESSES INTRODUCE PREVIOUSLY UNKNOWN ATTRIBUTES!

    # set this to supress indexing by attribute/word and attribute/value
    _freeTextOnly = False
    
    def __init__(self, directory, sessionId=None, threaded=False, splitter=entry.parseWords, readOnly=False):
        #pr "using ltree implementation", fltree
        self.readOnly = readOnly
        if sessionId is None:
            sessionId = fltree.newSessionName()
        self.sessionId = sessionId
        self.directory = directory
        self.splitter = splitter
        self.threaded = threaded
        self.initializeIndices()
        # special values lookup dictionary
        self.specialValuesMakers = specialValues.SPECIALS.copy()
        self.URLTranslations = {}
        self.AttributeTranslations = {"i": "i"}
        self.AttributeUntranslations = {}
        self.initializeFromSystemIndex()

    def nextAbbreviation(self):
        counter = self.abbreviationCounter
        result = str(counter)
        counter += 1
        self.System.put( (COUNTERVALUE, None), counter)
        self.abbreviationCounter = counter
        return result

    def initializeFromSystemIndex(self):
        System = self.System
        if not System.exists():
            #pr "NOT INITIALIZING BECAUSE SYSTEM INDEX IS ABSENT"
            return # no info available or not initialized
        index = System.getExistingBaseIndex()
        kv = index.firstKeyValue()
        while kv:
            #pr  "SYSTEM initializing", kv
            (key, value) = kv
            (opcode, subkey) = key
            if opcode==URLTRANSLATE:
                fromUrl = subkey
                toUrl = value
                #pr "url translate", (fromUrl, toUrl)
                self.URLTranslations[fromUrl] = toUrl
            elif opcode==ATTTRANSLATE:
                fromAttribute = subkey
                toAttribute = value
                #pr  "SETTING attribute translate", (fromAttribute, toAttribute)
                tr = self.AttributeTranslations
                un = self.AttributeUntranslations 
                if tr.has_key(fromAttribute):
                    raise ValueError, "overdefined initial attribute translation "+repr((fromAttribute, toAttribute, tr[fromAttribute]))
                tr[fromAttribute] = toAttribute
                if un.has_key(toAttribute):
                    raise ValueError, "overdefined initial attribute untranslation "+repr((toAttribute, fromAttribute, un[toAttribute]))
                un[toAttribute] = fromAttribute
            elif opcode==FREETEXTONLY:
                test = value
                #pr "freetext", test
                self._freeTextOnly = test
            elif opcode==COUNTERVALUE:
                #pr "SETTING COUNTER", value
                self.abbreviationCounter = value
            elif opcode==AUTOCLEAN:
                self._autoClean = value
            else:
                raise ValueError, "encountered unknown opcode in system index"
            kv = index.nextKeyValueAfter(key)
        # end of init from system

    def autoClean(self, value=True):
        "set to delete retired files as quickly as possible"
        self._autoClean = value
        self.System.put( (AUTOCLEAN, None), value)
        
    def freeTextOnly(self, value=True):
        "set to suppress indexing of text by attribute"
        self._freeTextOnly = value
        self.System.put( (FREETEXTONLY, None), value )

    def addURLTranslation(self, fromPrefix, toPrefix):
        "for example 'http://my.domain.edu/~me' translates to 'file://usr/home/me/htdocs', archive should be empty!"
        self.URLTranslations[fromPrefix] = toPrefix
        record = (URLTRANSLATE, fromPrefix, toPrefix)
        self.System.put( (URLTRANSLATE, fromPrefix), toPrefix )

    def addAttributeTranslation(self, fromAttribute, toAttribute, check=True):
        "internally translate (verbose name) fromAttribute to (terse name) to Attribute to save space; attribute should be unused yet in archive."
        if check:
            try:
                test = int(toAttribute)
            except:
                pass
            else:
                raise ValueError, "to attribute name cannot be only numeric "+repr(test)
        test = self.AttributeTranslations.get(fromAttribute)
        if test is not None and test!=toAttribute:
            raise ValueError, "cannot change existing translation: "+repr((fromAttribute, toAttribute, self.AttributeTranslations[fromAttribute]))
        test = self.AttributeUntranslations.get(toAttribute)
        if test is not None and test!=fromAttribute:
            raise ValueError, "cannot change existing untranslation: "+repr((toAttribute, fromAttribute, self.AttributeUntranslations[toAttribute]))
        self.AttributeTranslations[fromAttribute] = toAttribute
        self.AttributeUntranslations[toAttribute] = fromAttribute
        #pr "RECORDING SYSTEM DIRECTIVE", ((ATTTRANSLATE, fromAttribute), toAttribute )
        self.System.put( (ATTTRANSLATE, fromAttribute), toAttribute )

    def translateDict(self, dict):
        result = {}
        translations = self.AttributeTranslations
        autoAbbreviate = self.autoAbbreviate
        for key in dict:
            value = dict[key]
            translation = translations.get(key, None)
            if translation is None:
                if autoAbbreviate:
                    # auto-abbreviate the key
                    abbrev = self.nextAbbreviation()
                    #pr "auto abbreviating", (key, abbrev)
                    self.addAttributeTranslation(key, abbrev, check=False)
                    translation = abbrev
                    translations = self.AttributeTranslations # not actually needed, but for clarity
                else:
                    translation = key # don't change on no translation
            result[translation] = value
        return result

    def untranslateDict(self, dict):
        result = {}
        translations = self.AttributeUntranslations
        for key in dict:
            value = dict[key]
            translation = translations.get(key, key)
            result[translation] = value
        return result

    def translateURL(self, url):
        "for optimized url retrieval: translate url (http:...) into better url (file:...)"
        trans = self.URLTranslations
        #pr "trans len=", len(trans)
        for prefix in trans:
            #pr "<hr>comparing", (prefix, trans)
            if url.startswith(prefix):
                #pr "<hr><hr>match"
                translationPrefix = trans[prefix]
                remainder = url[len(prefix):]
                translation = "%s%s" % (translationPrefix, remainder)
                #pr "translation=", translation, "<br>"
                return translation
        # no translation found
        return url

    def ExpandedURL(self, url):
        "return a special url which is expanded as HTML"
        return self.specialValue("ExpandedURL", url)

    def UnExpandedURL(self, url):
        "return a special url which is not expanded as HTML"
        return self.specialValue("UnExpandedURL", url)

    def UnIndexedURL(self, url):
        "return a special url which is not indexed"
        return self.specialValue("UnIndexedURL", url)

    def ImageURL(self, url):
        "return a special url which represents an image (not indexed)"
        return self.specialValue("ImageURL", url)

    def InternalLink(self, identity):
        "return a representation for an internal link"
        return self.specialValue("InternalLink", identity)

    def specialValue(self, flagname, text):
        maker = self.specialValuesMakers[flagname]
        result = maker(flagname, text, self)
        return result
        
    def linkAttributes(self):
        "return sequence of attributes which are automatic internal id links"
        return (self.childAttributeName, self.parentAttributeName, self.idAttribute)
    
    def sessionIdentifier(self):
        "return current session identification string"
        return self.sessionId
    
    def ancestorChain(self, identity):
        "from a threaded id determine id sequence from id to most remote ancestor"
        # XXXXX hmmm... this might be an algorithmic issue if chains get long
        sep = self.threadSeparator
        if not identity.find(sep):
            return (identity,)
        result = []
        idSplit = identity.split(sep)
        while idSplit:
            ancestorId = sep.join(idSplit)
            result.append(ancestorId)
            del idSplit[0]
        return result
    
    def parent(self, identity):
        "find direct ancestor of this identity"
        identity = str(identity)
        sep = self.threadSeparator
        idSplit = identity.split(sep)
        if len(idSplit)<2:
            return None # no parent
        del idSplit[0]
        return sep.join(idSplit)
    
    def initializeIndices(self):
        "set up index structures"
        directory = self.directory
        sid = self.sessionId
        ro = self.readOnly
        self.System = NucularIndex("System", "od", "v", directory, sid, ro)
        self.Description = NucularIndex("Description", "iav", "t", directory, sid, ro)
        #self.Identities = NucularIndex("Identities", "i", "dt", directory, sid, ro)
        self.Log = NucularIndex("Log", "ti", "", directory, sid, ro)
        self.AttrIndex = NucularIndex("AttrIndex", "avi", "t", directory, sid, ro)
        #self.ValueLog = NucularIndex("ValueLog", "tiav", "", directory, sid, ro)
        self.AttrWord = NucularIndex("AttrWord", "awi", "t", directory, sid, ro)
        self.WordIndex = NucularIndex("WordIndex", "wi", "t", directory, sid, ro)
        # WordLog? tiaw->""
        # AttrGroup? aiv->t
        
    def loadReport(self):
        "for debug: show how many entries have been loaded into each index"
        print "VERBOSE: load report", self.directory
        for i in self.indexList():
            print i.name, "loaded", i.loadCount # VERBOSE
            
    def AttributeMatchFilter(self, attr, value):
        "select entries where attribute has value"
        attr1 = self.AttributeTranslations.get(attr, attr)
        index = self.AttrIndex.getExistingBaseIndex()
        tup = (attr1, value)
        query = pquery.MatchTuple(index, tup)
        test = pfilter.AttributeMatchTest(attr, value)
        return pfilter.Filter(query, test)
    
    def AttributePrefixFilter(self, attr, prefix):
        "select entries where attribute has prefix"
        attr1 = self.AttributeTranslations.get(attr, attr)
        index = self.AttrIndex.getExistingBaseIndex()
        tup = (attr1, prefix)
        query = pquery.PrefixTuple(index, tup)
        test = pfilter.AttributePrefixTest(attr, prefix)
        return pfilter.Filter(query, test)
    
    def AttributeRangeFilter(self, attr, low, high):
        "select entries where attribute lies in alpha range"
        attr1 = self.AttributeTranslations.get(attr, attr)
        index = self.AttrIndex.getExistingBaseIndex()
        tupleLow = (attr1, low)
        tupleHigh = (attr1, high)
        query = pquery.Range(index, tupleLow, tupleHigh)
        test = pfilter.AttributeRangeTest(attr, low, high)
        return pfilter.Filter(query, test)
    
    def AttributeWordFilter(self, attr, word, splitter=None):
        "select entries where attribute contains word"
        attr1 = self.AttributeTranslations.get(attr, attr)
        if self._freeTextOnly:
            raise ValueError, "attribute indexing is disabled"
        if splitter is None:
            splitter = self.splitter
        word = word.lower()
        index = self.AttrWord.getExistingBaseIndex()
        tup = (attr1, word)
        query = pquery.PrefixTuple(index, tup)
        test = pfilter.AttributeWordPrefixTest(attr, word, splitter)
        return pfilter.Filter(query, test)
    
    def WordFilter(self, word, splitter=None):
        "select entries containing word"
        if splitter is None:
            splitter = self.splitter
        word = word.lower()
        index = self.WordIndex.getExistingBaseIndex()
        tup = (word,)
        query = pquery.PrefixTuple(index, tup)
        test = pfilter.WordPrefixTest(word, splitter)
        return pfilter.Filter(query, test)

    def ProximateFilter(self, words, nearLimit, splitter=None):
        "select entries containing proximate words"
        if splitter is None:
            splitter = self.splitter
        # no index used, no "query", just a test
        test = pfilter.ProximateTest(words, nearLimit, splitter)
        return pfilter.Filter(None, test)
    
    def indexList(self):
        "for use with common operations: provide subindices in a sequence."
        return [self.Description, self.AttrIndex, self.Log, self.AttrWord, self.WordIndex, self.System]
        
    def create(self):
        "create a new archive, die if exists"
        if not os.path.exists(self.directory):
            os.mkdir(self.directory)
        if os.listdir(self.directory):
            raise ValueError, "cannot create archive in non-empty directory -- please delete all files and try again "+repr(self.directory)
        iList = self.indexList()
        for ind in iList:
            ind.createBaseIndex()
            
    def sync(self):
        "store undecided information for later resumption"
        iList = self.indexList()
        for ind in iList:
            ind.sync()
            
    def indexDictionary(self, identity, dictionary):
        "special case: index a dictionary (no duplicates)"
        D = {}
        #pr "indexDictionary", identity, dictionary
        for k in dictionary: #dictionary.keys():
            #pr "k", k
            #pr "dictionary[k]", dictionary[k]
            D[k] = [ dictionary[k] ]
        E = entry.Entry(identity, D)
        self.index(E)
        
    def index(self, entry, delete=False, test=False):
        "index the entry"
        stypes = types.StringTypes
        #pr ; pr "INDEXING", (entry,delete,test)
        SV = specialValues.SpecialValue
        _freeTextOnly = self._freeTextOnly
        if self.readOnly:
            raise ValueError, "cannot modify archive via read only session"
        # XXXX should check whether identity is present already?
        # "indexing", entry
        truncation = self.valueTruncation
        identity = entry.identity()
        timestamp = time.time()
        timestamp1 = True # for now...
        attrDict = entry.attrDict(indexable=True)
        attrDict = self.translateDict(attrDict)
        vDict = entry.attrDict(indexable=False)
        vDict = self.translateDict(vDict)
        #self.Identities.put( (identity,), (attrDict, timestamp), delete ) # tupling key for consistency ????
        self.Log.put( (timestamp, identity), (), delete )
        attrIndexDict = {}
        descrIndexDict = {}
        for att in attrDict: #attrDict.keys():
            if not _freeTextOnly:
                # attribute prefix indexing
                for val in attrDict[att]:
                    if type(val) in stypes:
                        # truncate strings
                        #tval = str(val)[:truncation]
                        tval = val[:truncation]
                    else:
                        tval = val
                    attrIndexDict[ (att, tval, identity) ] = timestamp1
            # description indexing/storage
            for val in vDict[att]:
                #pr "attrIndexDict", (identity, att, val)
                # make value marshalable if special
                if isinstance(val, SV):
                    val = val.marshalValue()
                else:
                    val = marshal.dumps(val)
                descrIndexDict[ (identity, att, val) ] = timestamp1
        # also "link" the parent/child relationship if present XXXX should be optional???
        #  XXXX NOTE: THIS CURRENTLY DOES NOT DO INDEXING ON CHILD/PARENT ATTRIBUTE
        parent = self.parent(identity)
        if parent is not None:
            att = self.childAttributeName
            val = identity
            if not _freeTextOnly:
                attrIndexDict[ (att, val, parent) ] = timestamp1
            #descrIndexDict[ (parent, att, val) ] = timestamp1
            mval = marshal.dumps(val)
            descrIndexDict[ (att, mval, parent) ] = timestamp1
            att = self.parentAttributeName
            val = parent
            if not _freeTextOnly:
                attrIndexDict[ (att, val, identity) ] = timestamp1
            descrIndexDict[ (identity, att, val) ] = timestamp1
        (freeWords, attributeWords) = entry.wordStats(self.AttributeTranslations)
        freeWordsDict = {}
        for w in freeWords: #freeWords.keys():
            freeWordsDict[ (w,identity) ] = timestamp1
        AttrWordDict = {}
        if not _freeTextOnly:
            for a in attributeWords: #attributeWords.keys():
                aDict = attributeWords[a]
                for w in aDict: #aDict.keys():
                    AttrWordDict[ (a, w, identity) ] = timestamp1
        if not test:
            #pr "BEFORE loading"
            #pr "  ",self.Description
            #pr "  ",self.AttrIndex
            #pr "  ",self.AttrWord
            #pr "  ",self.WordIndex
            self.Description.putDictionary(descrIndexDict, delete)
            self.AttrIndex.putDictionary(attrIndexDict, delete)
            self.AttrWord.putDictionary(AttrWordDict, delete)
            self.WordIndex.putDictionary(freeWordsDict, delete)
            if delete:
                self.Description.removeKeysWithPrefix( (identity,) ) # should be redundant modulo floating point
            #pr "AFTER loading"
            #pr "  ",self.Description
            #pr "  ",self.AttrIndex
            #pr "  ",self.AttrWord
            #pr "  ",self.WordIndex
            return None
        else:
            return (descrIndexDict, attrIndexDict, AttrWordDict, freeWordsDict)
        
    def remove(self, identity):
        "delete an identity from index"
        ##pr " ********** removing", identity
        entry = self.describe(identity)
        self.index(entry, delete=True)
        
    def firstId(self):
        "find first identity in the index"
        index = self.Description.getExistingBaseIndex()
        KeyValue = index.firstKeyValue()
        if KeyValue:
            (k,v) = KeyValue
            idnt = k[0]
            return idnt
        return None # no id's found
    
    def nextId(self, lastId):
        "find following identity in index"
        index = self.Description.getExistingBaseIndex()
        seekKey = (lastId, pquery.MAXIMUMOBJECT)
        KeyValue = index.nextKeyValueAfter(seekKey)
        if KeyValue:
            (k,v) = KeyValue
            idnt = k[0]
            return idnt
        return None # no id found
    
    def allIds(self, limit=None):
        "return sequence of all ids (strictly for testing purposes!)"
        # XXX this will blow out memory for large data sets if limit=None
        result = []
        index = self.Description.getExistingBaseIndex()
        KeyValue = index.firstKeyValue()
        lastId = None
        while KeyValue and (limit is None or len(result)<limit):
            (k,v) = KeyValue
            ##pr "allid keyvalue", KeyValue
            idnt = k[0]
            if idnt!=lastId:
                lastId = idnt
                result.append(lastId)
            KeyValue = index.nextKeyValueAfter(k)
        return result
    
    def hasIdentity(self, identity):
        "test whether a given identity exists in the index"
        index = self.Description.getExistingBaseIndex()
        probe = (identity,)
        test = index.nextKeyValueAfter( probe )
        if test:
            (k,v) = test
            if k[0]==identity:
                return True
        return False
        
    def describe(self, identity):
        "extract entry description for id (return None if missing)"
        from types import TupleType
        descr = {}
        index = self.Description.getExistingBaseIndex()
        fromKey = (identity,)
        toKey = (identity, pquery.MAXIMUMOBJECT)
        #D = index.rangeDict(fromKey, toKey)
        (keys, values) = index.rangeLists(fromKey, toKey)
        # "for", identity, "got", D
        for k in keys:
            (idt, att, mval) = k
            attL = descr.get(att)
            if attL is None:
                attL = descr[att] = []
            if type(mval) is TupleType:
                # special
                #pr "*** describing special", val
                (flagname, text) = mval
                specialval = self.specialValue(flagname, text)
                val = specialval
            else:
                val = marshal.loads(mval)
            if val not in attL:
                attL.append(val)
        descr = self.untranslateDict(descr)
        result = entry.Entry(identity, descr)
        return result

    def result(self, queryString):
        import booleanQuery
        result = booleanQuery.booleanResult(queryString, self)
        return result

    def entries(self, queryString):
        result = self.result(queryString)
        return result.entries()

    def dictionaries(self, queryString):
        result = self.result(queryString)
        return result.dictionaries()
    
    def Query(self):
        "create a query object associated with this archive"
        return NucularQuery(self, threaded=self.threaded)
    
    def QueryFromXMLText(self, text):
        "create a query object from xml text"
        from findetree import etree
        node = etree.fromstring(text)
        return self.QueryFromXMLNode(node)
    
    def QueryFromXMLNode(self, node):
        "create a query object from ElementTree XML node representation"
        if not (hasattr(node, "tag") and node.tag=="query"):
            return None # bad node type, fail
        threadedAttr = node.attrib.get("threaded")
        threaded = self.threaded
        if threadedAttr:
            if threadedAttr.upper().strip()!="FALSE":
                threaded = True
        Q = NucularQuery(self, threaded)
        for fld in node.getchildren():
            fldtag = None
            if hasattr(fld, "tag"):
                fldtag = fld.tag
            if fldtag=="prefix":
                n = fld.attrib.get("n")
                if not n:
                    raise ValueError, 'prefix requires n="attributeName"'
                p = fld.attrib.get("p")
                if not p:
                    raise ValueError, 'prefix requires p="prefix"'
                Q.prefixAttribute(n, p)
            elif fldtag=="range":
                n = fld.attrib.get("n")
                if not n:
                    raise ValueError, 'prefix requires n="attributeName"'
                low = fld.attrib.get("low")
                if not low:
                    raise ValueError, 'prefix requires low="low value"'
                high = fld.attrib.get("high")
                if not high:
                    raise ValueError, 'prefix requires high="high value"'
                Q.attributeRange(n, low, high)
            elif fldtag=="contains":
                n = fld.attrib.get("n")
                #if not n:
                #    raise ValueError, 'prefix requires n="attributeName"'
                p = fld.attrib.get("p")
                if not p:
                    raise ValueError, 'prefix requires p="prefix"'
                if n:
                    Q.attributeWord(n,p)
                else:
                    Q.anyWord(p)
            elif fldtag=="match":
                n = fld.attrib.get("n")
                if not n:
                    raise ValueError, 'match requires n="attributeName"'
                v = fld.attrib.get("v")
                if not v:
                    raise ValueError, 'prefix requires v="valueToMatch"'
                Q.matchAttribute(n,v)
            elif fldtag=="near":
                limit = fld.attrib.get("limit")
                try:
                    intLimit = int(limit)
                except:
                    raise ValueError, 'near requires limit="INTEGER" '+repr(limit)
                words = fld.attrib.get("words")
                if words:
                    words = words.strip().split()
                if not words:
                    raise ValueError, 'near requires words="WORDS TO MATCH IN ORDER" '+repr(words)
                Q.proximateWords(words, intLimit)
            else:
                pass # ignore bogus tag or comment for now.
        return Q
    
    def store(self, lazy=None):
        "record any changes"
        # system store must not be lazy
        System = self.System
        System.store(False)
        if lazy is None:
            lazy = self.lazyIndexing
        iList = self.indexList()
        for ind in iList:
            if ind is not System:
                ind.store(lazy)
            
    def discard(self):
        "discard changes"
        iList = self.indexList()
        for ind in iList:
            ind.discard()
            
    def aggregateRecent(self, dieOnFailure=True, verbose=False, fast=False, target=None):
        "collect recent changes"
        policy = parameters.gcPolicy() # disable gc, possibly to reenable later
        if verbose:
            print "verbose: entering nucular.aggregateRecent"
            now = time.time()
        iList = self.indexList()
        success = True
        work = False
        total = 0
        clean = self._autoClean
        for ind in iList:
            if target is not None and ind.name!=target:
                continue # skip non-targets if specified
            work = True
            archive = ind.getArchive()
            if verbose:
                print "verbose: aggregateRecent", ind
            try:
                (test, moved) = archive.aggregateRecent(dieOnFailure=dieOnFailure, verbose=verbose, fast=fast)
            finally:
                if clean:
                    if verbose:
                        print "verbose: auto-cleaning", ind
                        archive.cleanUp(complete=True)
            total += moved
            if not test:
                success = False
                break
        if not work:
            raise ValueError, "no such target found "+repr(target)
        if verbose:
            elapsed = time.time()-now
            print "verbose: nucular.aggregateRecent complete", total, "elapsed", elapsed, "rate", total/elapsed
        policy = None
        return (success, total)
    
    def moveTransientToBase(self, dieOnFailure=True, verbose=False, target=None):
        "add transient updates to base storage"
        policy = parameters.gcPolicy()
        if verbose:
            print "verbose: entering nucular.moveTransientToBase"
            now = time.time()
        iList = self.indexList()
        success = True
        work = False
        total = 0
        for ind in iList:
            if target is not None and ind.name!=target:
                continue # skip non-target if specified
            work = True
            archive = ind.getArchive()
            (test,moved) = archive.moveTransientToBase(dieOnFailure=dieOnFailure, verbose=verbose)
            total += moved
            if not test:
                success = False
                break
        if not work:
            raise ValueError, "no such target found "+repr(target)
        if verbose:
            elapsed = time.time()-now
            print "verbose: exiting nucular.moveTransientToBase", total, "elapsed", elapsed, "rate", total/elapsed
        policy = None
        return (success, total)
    
    def cleanUp(self, complete=False, target=None):
        "delete old files"
        iList = self.indexList()
        work = False
        for ind in iList:
            if target is not None and ind.name!=target:
                continue # skip non target if specified
            work = True
            archive = ind.getArchive()
            archive.cleanUp(complete=complete)

def flattenMultiDict(md):
    "from key-->{x:y} dict return [ (key,x)... ] list"
    count = 0
    D = {}
    for k in md: #md.keys():
        D2 = md[k]
        for x in D2: #D2.keys():
            D[count] = (k,x)
            count += 1
    return D.values()

def flattenListDict(md):
    "from key-->[x...] dict return [ (key,x)... ] list"
    count = 0
    D = {}
    for k in md: #md.keys():
        L = md[k]
        for x in L:
            D[count] = (k,x)
            count += 1
    return D.values()

class NucularQuery:
    "Document retrieval query abstraction."

    def __init__(self, collection, threaded=False):
        self.collection = collection
        self.threaded = threaded
        # use a dictionary to automatically eliminate dups
        self.filters = {}
        self.log = []
        
    def toXML(self):
        "display query in xml representation"
        L = [ '<query threaded="%s">' % self.threaded ]
        ap = L.append
        items = self.filters.items()
        items.sort()
        for (k, f) in items:
            ap( "   "+f.toXML() )
        ap( "</query>" )
        return "\n".join(L)
    
    def report(self):
        "generate a string describing the evaluation report"
        logdata = "\n".join(self.log)
        return "Query log:\n"+logdata
    
    def addMiscellaneousFilter(self, filterIdentifier, filter):
        "add an externally defined filter"
        filters = self.filters
        if filters.has_key(filterIdentifier):
            raise KeyError, "filter identifier collision: "+repr(filterIdentifier)
        filters[filterIdentifier] = filter
        
    def matchAttribute(self, attr, value):
        "select entries where attribute has value"
        f = self.collection.AttributeMatchFilter(attr, value)
        self.filters[ ("m",attr,value) ] = f
        
    def prefixAttribute(self, attr, prefix):
        "select entries where attr value starts with prefix"
        f = self.collection.AttributePrefixFilter(attr, prefix)
        self.filters[ ("p",attr,prefix) ] = f
        
    def attributeRange(self, attr, low, high):
        "select entries where attr lies in alpha range low..high"
        f = self.collection.AttributeRangeFilter(attr, low, high)
        self.filters[ ("r",attr,low,high) ] = f

    def proximateWords(self, wordSequence, nearLimit=2):
        "select entries where wordSequence appears in order near eachother in one attribute"
        # normalize the words, just in case
        splitsequence = []
        splitter = self.collection.splitter
        if type(wordSequence) in types.StringTypes:
            splitsequence = splitter(wordSequence)
        else:
            for w in wordSequence:
                words = splitter(w)
                splitsequence.extend(words)
        wordSequence = tuple(splitsequence)
        if len(wordSequence)>1:
            # word sequence of length 1 is an "anyword"
            f = self.collection.ProximateFilter(wordSequence, nearLimit=nearLimit)
            self.filters[ ("x", wordSequence) ] = f
        # also add attributeWords to make use of indices
        for w in wordSequence:
            self.anyWord(w)
        
    def attributeWord(self, attr, word):
        "select entries which match word as a word prefix in a value for attr"
        words = self.collection.splitter(word)
        for w in words:
            f = self.collection.AttributeWordFilter(attr, w)
            self.filters[ ("a",attr,w) ] = f
            
    def anyWord(self, word):
        "select entries which match word as a word-prefix in any attribute"
        words = self.collection.splitter(word)
        for w in words:
            f = self.collection.WordFilter(w)
            self.filters[ ("w",w) ] = f
            
    def resultDictionaries(self):
        "return evaluated result as sequence of dictionaries"
        (result, status) = self.evaluate()
        if not result:
            raise ValueError, "no result returned in evaluation status="+repr(status)
        return result.dictionaries()

    def annotateDictionary(self, entry, delimitPairs=None):
        "mark up entry dictionary with html showing match locations"
        if delimitPairs is None:
            delimitPairs = [ ("<b><em>", "</em></b>") ]
        npairs = len(delimitPairs)
        filters = self.filters.items()
        filters.sort()
        Dict = entry.attrDict(indexable=True)
        for i in xrange(len(filters)):
            (name, f) = filters[i]
            (startmark, endmark) = delimitPairs[ i % npairs ]
            Dict = f.annotate(Dict, startmark, endmark)
        return Dict

    def SuggestionQuery(self):
        "generate a weakened query for generating completion suggestions"
        result = NucularQuery(self.collection, self.threaded)
        for (key, filter) in self.filters.items():
            result.filters[key] = filter.SuggestionFilter()
        return result

    #def suggestionDictionaries(self, entry, allFields, byFields):
    #    "generate a dictionary of suggested attribute completions from entry"
    #    dict = entry.attrDict(indexable=True)
    #    filters = self.filters
    #    for key in self.filters:
    #        f = filters[key]
    #        suggestions = f.suggestion(dict, allFields, byFields)

    def suggestions(self, sampleSize=None, maxBufferLimit=MAXBUFFERDEFAULT,
                 switchFactor=SWITCHFACTORDEFAULT, minResultLength=None):
        """
        return (L, D) suggesting query completions.
        L is a list of suggestions for any word completion suggestions.
        D is a dict of attr->list for list of attribute completion suggestions.
        (heuristic)
        """
        SQ = self.SuggestionQuery()
        return SQ._suggestions(sampleSize, maxBufferLimit, switchFactor, minResultLength)

    def _suggestions(self, sampleSize=None, maxBufferLimit=MAXBUFFERDEFAULT,
                 switchFactor=SWITCHFACTORDEFAULT, minResultLength=None):
        """
        return (L, D) suggesting query completions.
        L is a list of suggestions for any word completion suggestions.
        D is a dict of attr->list for list of attribute completion suggestions.
        (heuristic)
        """
        # XXX for the moment don't offer any suggestions if the query is trivial
        L = []
        D = {}
        filters = self.filters
        if filters:
            (result, status) = self.evaluate(maxBufferLimit, switchFactor, minResultLength)
            if result:
                idList = result.identities()
                # only sample from up to sampleSize if specified
                if sampleSize:
                    idList = idList[:sampleSize]
                # collect filtered suggestions from samples
                allFields = {}
                byFields = {}
                collectFields = {}
                entries = {}
                for identity in idList:
                    entry = result.describe(identity)
                    entries[identity] = entry
                    #self.suggestionDictionaries(entry, allFields, byFields)
                    collectFields.update(entry.attrDict())
                # for attributes with no query suggestions add entry samples
                entryL = entries.values()
                for attr in collectFields:
                    if not byFields.has_key(attr):
                        attrD = {}
                        for entry in entryL:
                            samples = entry.sample(attr)
                            for s in samples:
                                attrD[s] = s
                        byFields[attr] = attrD
                # extract dictionaries as lists
                D = {}
                for a in byFields:
                    fD = byFields[a]
                    D[a] = fD.keys()
                # if allFields is provided, generate L from it.
                collectL = {}
                if allFields:
                    for attrD in allFields.values():
                        #pr attrD
                        collectL.update(attrD)
                else:
                    # otherwise collect all suggestions as L
                    for attrD in byFields.values():
                        collectL.update(attrD)
                L = collectL.keys()
            else:
                # query is trivial: no suggestions
                pass
        return (L, D)
    
    def evaluate(self, maxBufferLimit=MAXBUFFERDEFAULT,
                 switchFactor=SWITCHFACTORDEFAULT, truncateSize=None, materializeSize=None,
                 minResultLength=None, timeLimit=QUERYTIMELIMIT):
        """
        return (result, status)
        If the estimates for all filters exceed maxBufferLimit return (None, OVERFLOWSTATUS).
        If the truncateSize is set truncate the intermediate results to that size.
        If materializeSize is set, do materialization whenever intermediate size falls below that limit.
        If the evaluation was terminated to attempt to provide result exceeding minResultLength status=TRUNCATEDSTATUS.
        Otherwise status=COMPLETESTATUS.
        "result" if not None is a NucularResult.
        """
        now = time.time()
        policy = parameters.gcPolicy()
        # minResultLength not yet supported...
        if minResultLength is not None:
            raise ValueError, "minResultLength support not yet implemented"
        self.log = []
        log = self.log.append
        collection = self.collection
        # sort filters by increasing estimate
        allFilters = self.filters.copy()
        estimatesDict = {}
        for (k,f) in allFilters.items():
            est = f.estimate()
            # if the estimate is None then the filter can only be directly tested, not indexed.
            if est is not None:
                estimatesDict[ (est,k) ] = f
        estimateList = estimatesDict.items()
        estimateList.sort()
        # evaluate filters that are cheap enough
        if self.threaded:
            log("threaded evaluation "+repr(now))
            result = ThreadedNucularResult(collection, self)
        else:
            log("unthreaded evaluation "+repr(now))
            result = NucularResult(collection, self)
        sizeLimit = maxBufferLimit
        status = COMPLETESTATUS # unless decided otherwise later
        newsize = None
        for ( (est, k), f ) in estimateList:
            # "estimate", (est,k,sizeLimit)
            log("estimate %s=%s sizelimit=%s [%s]" %(k, est, sizeLimit, time.time()-now))
            evaluateF = False
            if est<sizeLimit:
                # estimate is okay: use the index
                evaluateF = True
            if truncateSize is not None:
                # ignore the estimate anyway: just use the index and truncate as needed. (heuristic)
                evaluateF = True
            if newsize is not None and materializeSize is not None and newsize<materializeSize:
                # We are below the materialization limit: switch from using indices to using materialized entries.
                evaluateF = False
            if evaluateF:
                idDict = f.evaluateD(truncateSize=truncateSize)
                # "idDict", idDict
                result.intersectDict(idDict)
                del allFilters[k] # done with this filter
                newsize = result.size()
                log("evaluated %s result %s give size=%s [%s]" % (k, len(idDict), newsize, time.time()-now))
                sizeLimit = min(maxBufferLimit, switchFactor*newsize+100)
            else:
                log("estimate %s for %s switches to materialization %s" % (est, newsize, (sizeLimit,truncateSize,materializeSize)))
                break # done looking for cheap filters
        if not result.populated():
            log("aborting evaluation with overflow")
            return (None, OVERFLOWSTATUS) # no small enough filter found
        # XXXX note could add "attribute/value/id" indexing here with additional index
        # XXXX as a possible optimization.
        # otherwise refine by checking descriptions against remaining filters (if any)
        ### filter id by id for remaining filters to allow early truncation on slow evaluation
        remainingFilters = allFilters.items()
        if not remainingFilters:
            return (result, status)
        # XXXX should sort the filters by increasing difficulty
        tests = [ f.test for (k,f) in remainingFilters ]
        ids = result.identities()
        ids.sort()
        keepids = {}
        for identity in ids:
            description = result.describe(identity)
            keep = True
            for test in tests:
                if not test(description):
                    keep = False
                    break
            if keep:
                keepids[identity] = description
            elapsed = time.time()-now
            if timeLimit is not None and elapsed>timeLimit:
                log("terminating final evaluation early at %s with %s elements" % (elapsed, len(keepids)))
                break # terminate early on too slow evaluation
        result.resetDict(keepids, keepids.copy())
        policy = None
        #pr "LOG"
        #pr "\n".join(self.log)
        elapsed = time.time()-now
        log("evaluation complete at %s" % elapsed)
        return (result, status)
    
    def test(self):
        "self test to make sure that filters are working"
        # XXX this will blow out memory for large data sets
        idDicts = {}
        tests = {}
        allFilters = self.filters.copy()
        for (k,f) in allFilters.items():
            idDicts[k] = f.evaluateD()
            tests[k] = f.test
        # iterate over all ids
        collection = self.collection
        allIds = collection.allIds()
        for identity in allIds:
            #pr "*** in test describing", repr(identity)
            descr = collection.describe(identity)
            #pr "description = ", repr(descr)
            for (k,f) in allFilters.items():
                idDict = idDicts[k]
                test = tests[k]
                #pr "test is", test
                testMatch = test(descr)
                inDict = idDict.has_key(identity)
                if inDict:
                    if not testMatch:
                        raise ValueError, "for %s descr %s in dict but fails test" % (k,descr)
                if testMatch:
                    if not inDict:
                        raise ValueError, "for %s descr %s matches test but not in dict" % (k,descr)

class NucularResult:
    "container for the result of a query"
    def __init__(self, collection, query=None):
        self.collection = collection
        self.idDict = None
        self.idToDescription = {}
        self.idToAncestors = {} # used for threading
        self.query = query
        
    def annotateDictionary(self, entry, delimitPairs=None):
        "mark up entry dictionary with html showing match locations"
        query = self.query
        if query is None:
            raise ValueError, "cannot annotate: query not recorded"
        return query.annotateDictionary(entry, delimitPairs)

    def resetDict(self, idDict, idToDescriptions=None):
        if idToDescriptions is None:
            idToDescriptions = {}
        self.idDict = idDict
        self.idToDescriptions = idToDescriptions
        
    def intersectDict(self, idDict):
        "combine intermediate results with new results in idDict"
        if self.idDict is None:
            self.idDict = idDict
        elif self.idDict:
            newDict = dictIntersect(self.idDict, idDict)
            self.idDict = newDict

    def unionDict(self, idDict):
        if self.idDict is None:
            self.idDict = idDict
        else:
            all = self.idDict.copy()
            #all.update(idDict)
            for k in idDict.keys():
                if all.get(k) is None:
                    all[k] = idDict[k]
            # xxxx should do something about cleaning up child matches for matching ancestors...
            self.idDict = all

    def differenceDict(self, idDict):
        # XXXX this is probably incorrect for the case of document inheritance (need to check)
        assert self.idDict is not None, "unrestricted negation is not permitted"
        all = self.idDict.copy()
        for k in idDict.keys():
            if all.has_key(k):
                del all[k]
        self.idDict = all
        
    def populated(self):
        "true iff the result has recorded an entry set"
        return self.idDict!=None
    
    def size(self):
        return len(self.idDict)

    def entries(self):
        policy = parameters.gcPolicy()
        identities = self.identities()
        descriptions = [self.describe(i) for i in identities]
        policy = None
        return entries
    
    def dictionaries(self):
        "return selected entries as dictionaries"
        policy = parameters.gcPolicy()
        identities = self.identities()
        descriptions = [self.describe(i) for i in identities]
        dictionaries = [descr.asDictionary() for descr in descriptions]
        policy = None
        return dictionaries
    
    def identities(self):
        "return all selected identities as a sequence"
        result = self.idDict.keys()
        result.sort()
        return result
    
    def toXML(self):
        L = ["<entries>"]
        ap = L.append
        for identity in self.identities():
            descr = self.describe(identity)
            ap(descr.toXML("   "))
        ap("</entries>")
        return "\n".join(L)
    
    def allIdentities(self):
        "do not eliminate any based on threading or whatever in threaded result"
        return NucularResult.identities(self)
    
    def remove(self, identity):
        "take an identity out of this result set"
        idDict = self.idDict
        i2d = self.idToDescription
        if idDict.has_key(identity):
            del idDict[identity]
        if i2d.has_key(identity):
            del i2d[identity]
            
    def describe(self, identity):
        "get description for identity (and cache it)"
        idDict = self.idDict
        if idDict is None:
            raise ValueError, "result not yet populated"
        if not idDict.has_key(identity):
            raise KeyError, "id not in result set"
        i2d = self.idToDescription
        result = i2d.get(identity)
        if result is None:
            result = self.collection.describe(identity)
            i2d[identity] = result
        return result
    
    def describeThread(self, identity):
        "return list of descriptions [followUp, parent, grandparent,...]"
        ancestorChain = self.collection.ancestorChain(identity)
        result = [ self.describe(anId) for anId in ancestorChain ]
        return result

class ThreadedNucularResult(NucularResult):
    """
    A threaded Id is represented as a delimited string.
    A follow up matches any pattern matched by its parent.
    A result omit the child if a collection matches both the parent and child.
    """
    def identities(self):
        "return identities that don't have ancestors in the same result set"
        idDict = self.idDict
        idToAncestors = self.idToAncestors
        collection = self.collection
        allDict = idDict.copy()
        for identity in idDict:
            ancestors = idToAncestors.get(identity)
            if ancestors is None:
                ancestors = collection.ancestorChain(identity)
            # "for", identity, "ancestors=", ancestors
            keep = True
            # remove any entry with ancestor in same set
            for ancestor in ancestors:
                if identity!=ancestor and idDict.has_key(ancestor):
                    #pr "removing", identity, "because of", ancestor
                    keep = False
                    break
            if not keep:
                del allDict[identity]
        return allDict.keys()
    
    def intersectDict(self, idDict):
        "check in current results whether id or ancestors match new result, remove non-matches"
        if self.idDict is None:
            self.idDict = idDict
        else:
            myDict = self.idDict
            idToAncestors = self.idToAncestors
            collection = self.collection
            currentIds = myDict.keys()
            # see if each identity or any of its ancestors are in the result
            for identity in currentIds: # myDict modified
                discard = True
                ancestors = idToAncestors.get(identity)
                if ancestors is None:
                    ancestors = idToAncestors[identity] = collection.ancestorChain(identity)
                #id1 = identity
                for ancestor in ancestors:
                    #pr id1
                    if idDict.has_key(ancestor):
                        discard = False
                        break
                        #if id1!=identity:
                            #pr "keeping", identity, "based on ancestor match", ancestor
                if discard:
                    del myDict[identity]
            self.idDict = myDict

def dictIntersect(d1, d2):
    "return pairs common to both dictionaries (should be a builtin)"
    if len(d1)>len(d2):
        (d1,d2) = (d2,d1)
    result = {}
    for k in d1: #d1.keys():
        if d2.has_key(k):
            result[k] = d1[k]
    return result

class NucularIndex:
    "Sub-index interface for Nucular indices."
    
    lazyIndexing = True
    
    def __init__(self, name, argumentsOrder, valuesOrder, directory, sessionId, readOnly=False):
        "connect to an index in the directory with the name"
        self.readOnly = readOnly
        self.sessionId = sessionId
        self.name = name
        self.argumentsOrder = argumentsOrder
        self.valuesOrder = valuesOrder
        self.directory = directory
        self.archive = None
        self.session = None
        self.timestamp = None
        self.loadCount = 0

    def __repr__(self):
        return "nucularIndex: "+repr((self.name, self.sessionId))
        
    def archiveDirectory(self):
        "return the path of the subdirectory containing the index implementation files"
        directory = os.path.join(self.directory, self.name)
        return directory
        
    def put(self, key, value, delete=False):
        "set at key/value pair in the base index"
        index = self.getExistingBaseIndex()
        if delete:
            del index[key]
        else:
            index[key] = value

    def removeKeysWithPrefix(self, keyPrefixTuple):
        fromKey = keyPrefixTuple
        toKey = keyPrefixTuple + (pquery.MAXIMUMOBJECT,)
        index = self.getExistingBaseIndex()
        (keys, values) = index.rangeLists(fromKey, toKey)
        D = {}
        #pr "removing keys", keys
        for k in keys:
            D[k] = None
        index.delDictionary(D)
            
    def putDictionary(self, dictionary, delete=False):
        "set entries from dictionary in the base index"
        #pr self.name, "putDictionary", delete
        self.loadCount += len(dictionary)
        #pr dictionary
        index = self.getExistingBaseIndex()
        if delete:
            index.delDictionary(dictionary)
        else:
            index.putDictionary(dictionary)

    def exists(self):
        directory = self.archiveDirectory()
        return os.path.exists(directory)
            
    def getExistingBaseIndex(self):
        "open the base index"
        if self.session:
            return self.session
        directory = self.archiveDirectory()
        self.archive = fltree.LayeredArchive(directory)
        #(self.session, self.timestamp) = self.archive.newSessionMapping()
        self.session = self.archive.sessionMapping(self.sessionId, self.readOnly)
        self.timestamp = self.sessionId
        return self.session
    
    def createBaseIndex(self):
        "make the base index"
        directory = self.archiveDirectory()
        self.archive = fltree.LayeredArchive(directory, create=True)
        #(self.session, self.timestamp) = self.archive.newSessionMapping()
        #return self.session
        self.session = None
        return self.getExistingBaseIndex()
    
    def sync(self):
        "record results (uncommitted) in undecided area"
        if self.session:
            self.session.sync()
            
    def store(self, waiting):
        "make updates permanent"
        if self.session:
            self.session.store(waiting)
        self.session = None
        
    def discard(self):
        "discard updates"
        if self.session:
            self.session.discard()
        self.session = None
        
    def getArchive(self):
        "get the index implementation archive, open it if not already open."
        dummy = self.getExistingBaseIndex()
        return self.archive

