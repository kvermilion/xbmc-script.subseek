"""
A thin wrapper which makes a nucular archive look a little like an SQL table space.
"""

import os
import Nucular
import entry
import dCursor
import shutil

class TableSpace:
    "quick and dirty tables embedded in nucular"

    def __init__(self, session, prefix, tableNameAttribute="TableName", lower=True, metaTableName="metadata"):
        #pr "tablespace init", prefix 
        self.metaDataChanged = False
        self.session = session
        self.prefix = prefix
        self.tableNameToPrefixAndKeyList = {}
        self.prefixToTableName = {}
        if lower:
            tableNameAttribute = tableNameAttribute.lower()
        self.tableNameAttribute = tableNameAttribute
        self.metaTableName = metaTableName
        self.lower = lower
        self.idMaker = dCursor.PrimaryKeyIdMaker(tableNameAttribute, prefix, lower=lower)
        self.loadMetaData()

    def dumpGen(self, rows=True):
        "dump metadata report and optionally row values as well"
        yield "TableSpace dump ts prefix="+repr(self.prefix)
        tableNameToPrefixAndKeyList = self.tableNameToPrefixAndKeyList
        tableNames = tableNameToPrefixAndKeyList.keys()
        tableNames.sort()
        for tableName in tableNames:
            yield "    table: "+repr(tableName)
            (prefix, keylist) = tableNameToPrefixAndKeyList[tableName]
            keylist = list(keylist)
            yield "        prefix="+repr(prefix)
            yield "        keylist="+repr(keylist)
            if rows:
                Q = self.selectByValues(tableName)
                dicts = Q()
                for d in dicts:
                    atts = d.keys()
                    identity = d['i']
                    atts.sort()
                    yield "        "+repr(identity)+":"
                    for att in atts:
                        yield "            "+repr(att)+" : "+repr(d[att])

    def dumpString(self):
        return "\n".join(list(self.dumpGen()))

    def loadMetaData(self):
        """
        Load metadata entry of form:
        i: metaId
        metaId+'K'+tableName : keyAttributes
        metaId+'P'+tableName : tablePrefix
        ...
        """
        metaId = self.metaId()
        keyPrefix = metaId+"K"
        abbrPrefix = metaId+"P"
        lenprefix = len(keyPrefix)
        metaEntry = self.session.describe(metaId)
        entrydata = metaEntry.attrDict()
        metaDataChanged = self.metaDataChanged
        for attr in entrydata.keys():
            if attr.startswith(keyPrefix):
                tableName = attr[lenprefix:]
                if tableName:
                    keysstring = metaEntry[keyPrefix+tableName]
                    keys = keysstring.split()
                    tableAbbrev = metaEntry[abbrPrefix+tableName]
                    self.defTableList(tableName, tableAbbrev, keys)
        # restore the store flag to previous value (no new information loaded).
        self.metaDataChanged = metaDataChanged

    def storeMetaData(self):
        tableNameToPrefixAndKeyList = self.tableNameToPrefixAndKeyList
        metaId = self.metaId()
        keyPrefix = metaId+"K"
        abbrPrefix = metaId+"P"
        metaEntry = entry.Entry(metaId)
        for (tablename, (prefix, keylist)) in tableNameToPrefixAndKeyList.items():
            metaEntry[abbrPrefix+tablename] = prefix
            metaEntry[keyPrefix+tablename] = " ".join(keylist)
        self.session.remove(metaId)
        self.session.index(metaEntry)
        
    def metaId(self):
        idMaker = self.idMaker
        meta = self.metaTableName
        identity = idMaker.fakeId(meta)
        return identity
    
    def store(self, lazy=False):
        #pr self.prefix, "tablespace store"
        if self.metaDataChanged:
            self.storeMetaData()
        self.session.store(lazy=lazy)
        self.metaDataChanged = False

    def cleanUp(self, full=False, fast=True, verbose=False):
        #pr self.prefix, "tablespace cleanup"
        self.session.aggregateRecent(fast=fast, verbose=verbose)
        if full:
            self.session.moveTransientToBase(verbose=verbose)
            
    def getSession(self):
        return self.session
    
    def dropTable(self, tableName):
        self.deleteByValues(tableName)
        ptn = self.prefixToTableName
        tntpk = self.tableNameToPrefixAndKeyList
        (tableAbbrev, keyAttributes) = tntpk[tableName]
        del tntpk[tableName]
        del ptn[tableAbbrev]
        self.storeMetaData()
        self.__init__(self.session, self.prefix, self.tableNameAttribute, self.lower, self.metaTableName)
        
    def dropAll(self, autosave=True, lazy=False):
        """erase all entries relating to this table space including metadata"""
        idprefix = self.idMaker.fakeId("")
        q = self.session.Query()
        q.prefixAttribute('i', idprefix)
        (result, status) = q.evaluate()
        ids = result.identities()
        for identity in ids:
            self.session.remove(identity)
        if autosave:
            self.session.store(lazy=lazy)
            
    def defTableList(self, tableName, tableAbbrev, keyAttributes):
        "define a table, it's unique abbreviation and the key attributes for the table"
        if tableName==self.metaTableName:
            raise ValueError, "cannot use metaTableName as TableName "+repr((tableName, self.metaTableName))
        if tableAbbrev==self.metaTableName:
            raise ValueError, "cannot use metaTableName as TableAbbrev "+repr((tableAbbrev, self.metaTableName))
        for k in keyAttributes:
            if len(k.split())!=1:
                raise ValueError, "key attribute names may not be empty or contain white space "+repr(k)
        ptn = self.prefixToTableName
        tntpk = self.tableNameToPrefixAndKeyList
        if tntpk.has_key(tableName):
            (abbr, keys) = tntpk[tableName]
            if abbr==tableAbbrev and list(keys)==list(keyAttributes):
                return # permit redeclaration of identical table
            raise ValueError, "table name in use "+repr((tableName, abbr, keys))
        if ptn.has_key(tableAbbrev):
            raise ValueError, "abbreviation in use "+repr(tableAbbrev)
        current = ptn.get(tableAbbrev)
        self.idMaker.addPrimaryKeyList(tableName, tableAbbrev, *keyAttributes)
        ptn[tableAbbrev] = tableName
        tntpk[tableName] = (tableAbbrev, keyAttributes)
        self.metaDataChanged = True
    def defTable(self, tableName, tableAbbrev, *keyAttributes):
        "convenience alternative to defTableList"
        return self.defTableList(tableName, tableAbbrev, keyAttributes)
    # foriegn key logic contemplated but deferred for now...
    # metadata archiving contemplated but deferred for now...
    def baseNucularQuery(self, tableName, attributesToValues):
        if self.lower:
            attributesToValues = dCursor.lowerD(attributesToValues)
        query = self.session.Query()
        attributesToValues = attributesToValues.copy()
        attributesToValues[self.tableNameAttribute] = tableName
        idPrefix = self.idMaker(attributesToValues, partial=True)
        for (name, value) in attributesToValues.items():
            query.matchAttribute(name, value)
        query.prefixAttribute("i", idPrefix)
        return query
    def extendedRow(self, tableName, dictionary):
        "add table name and identity information to row"
        if self.lower:
            dictionary = dCursor.lowerD(dictionary)
        tableatt = self.tableNameAttribute
        if tableName is None:
            try:
                tableName = dictionary[tableatt]
            except KeyError:
                raise ValueError, "cannot determine table name to extend row"
        result = dictionary.copy()
        result[tableatt] = tableName
        identity = self.idMaker(result, partial=False) # raises exception if key attributes are missing
        result["i"] = identity
        return (identity, tableName, result)
    def selectByValuesDict(self, tableName, argsDict):
        "return a selection object (over one table only at the moment)"
        #pr "selectByValues", (tableName, argsDict)
        return SelectByValues(self, tableName, argsDict, self.lower)
    def selectByValues(self, tableName, **argsDict):
        "convenience alternative to selectByValuesDict"
        return self.selectByValuesDict(tableName, argsDict)
    def insert(self, tableName, **namesToValues):
        "convenience version of insertDict"
        return self.insertDict(tableName, namesToValues)
    def insertDict(self, tableName, namesToValues):
        "insert dictionary using table name (or look up table name if tableName is None)"
        #pr "insertDict", namesToValues
        (identity, tableName, D) = self.extendedRow(tableName, namesToValues)
        #pr "insertDict before", self.session.describe(identity).asDictionary(strict=True)
        self.session.indexDictionary(identity, D)
        self.store() # for debug!
        self.cleanUp()
        #pr "insertDict after", self.session.describe(identity).asDictionary(strict=True)
    def delete(self, tableName, **namesToValues):
        "convenience version of deleteDict"
        return self.deleteDict(tableName, namesToValues)
    def deleteDict(self, tableName, namesToValues):
        "delete any dictionary with matching identifier using table name (or look up table name if tableName is None)"
        #pr "deleteDict", tableName, namesToValues
        (identity, tableName, D) = self.extendedRow(tableName, namesToValues)
        #pr "deleteDict before", self.session.describe(identity).asDictionary(strict=True)
        self.session.remove(identity)        
        #pr "deleteDict after", self.session.describe(identity).asDictionary(strict=True)
    def match(self, tableName, **argsDict):
        "convenience"
        sel = self.selectByValuesDict(tableName, argsDict)
        return sel()        
    def deleteByValues(self, tableName, **argsDict):
        "convience delete interface"
        sel = self.selectByValuesDict(tableName, argsDict)
        return sel.delete()
    def testQueryGenFunction(self, varname, tableName, **namesToValues):
        def runTest(matchingDictionaries=None):
            query = self.selectByValues(tableName, **namesToValues)
            result = query()
            it = namesToValues.items()
            yield varname+".testQueryGenFunction("+repr(varname)+", "+repr(tableName)+","
            it.sort()
            for (n,v) in it:
                yield n+"="+repr(v)+ ","
            yield ") ( ["
            for dict in result:
                dit = dict.items()
                dit.sort()
                yield "   {"
                for (a,b) in dit:
                    yield repr(a)+" : "+repr(b)+","
                yield "   }"
            yield "] )"
            if matchingDictionaries is not None:
                for dict in matchingDictionaries:
                    if not dict in result:
                        raise ValueError, "expected dict missing in query result "+repr(dict)
                for dict in result:
                    if not dict in matchingDictionaries:
                        raise ValueError, "result dict missing in expected dictionary list "+repr(dict)
        return runTest

class SelectByValues:
    "wrapper for query over one table with update, delete"
    def __init__(self, tableSpace, tableName, argsDict, lower=True):
        #pr "selectByValues init", (tableSpace, tableName, argsDict, lower)
        if lower:
            argsDict = dCursor.lowerD(argsDict)
        self.lower = lower
        self.tableSpace = tableSpace
        self.tableName = tableName
        self.argsDict = argsDict
        # create the query object...
        self.query = self.tableSpace.baseNucularQuery(tableName, argsDict)
    def getQuery(self):
        "return the internal query object for external modification (addition of restrictions)"
        return self.query
    def updateSetDict(self, newValuesDict):
        "update the matching rows, set values as specified in newValuesDict"
        #pr "updateSetDict", self.argsDict, newValuesDict
        if self.lower:
            newValuesDict = dCursor.lowerD(newValuesDict)
        session = self.tableSpace.getSession()
        #matchingDicts = self()
        ids = self.getIds()
        for identity in ids:
            e = session.describe(identity)
            #pr "updating identity", identity
            d = e.asDictionary(strict=True)
            #pr "update before", d
            d.update(newValuesDict)
            session.remove(identity)
            self.tableSpace.insertDict(None, d)
            #pr "update after", session.describe(identity).asDictionary(strict=True)
    def updateSet(self, **newValuesDict):
        "convenience alternative to updateSetDict"
        return self.updateSetDict(newValuesDict)
    def getIds(self):
        "get list of id's matching query"
        (result, status) = self.query.evaluate()
        if result:
            ids = result.identities()
            return ids
        else:
            raise ValueError, "query evaluation failed "+repr(status)
    def getIter(self):
        "iterate matching dictionaries"
        ids = self.getIds()
        session = self.tableSpace.getSession()
        for identity in ids:
            if session.hasIdentity(identity):
                outentity = session.describe(identity)
                outdict = outentity.asDictionary(strict=True)
                yield outdict
    def __call__(self):
        "return matching dictionaries in a list"
        return list(self.getIter())
    def delete(self):
        "delete all matching entries"
        ids = self.getIds()
        session = self.tableSpace.getSession()
        for identity in ids:
            session.remove(identity)

def getTableSpace(archivePath, prefix="", create=False):
    if create:
        if os.path.exists(archivePath):
            shutil.rmtree(archivePath)
        os.mkdir(archivePath)
        session = Nucular.Nucular(archivePath)
        session.create()
    else:
        session = Nucular.Nucular(archivePath)
    space = TableSpace(session, prefix)
    return space

def newTableSpace(archivePath, prefix=""):
    return getTableSpace(archivePath, prefix, create=True)

def test_twice(archive="/tmp/supplier_parts"):
    "test two table spaces in same archive"
    test(prefix="SP1")
    test(prefix="SP2", full=False)
    for prefix in ("SP1", "SP2"):
        ts = getTableSpace(archive, prefix)
        print
        print "dump of", prefix, "tablespace"
        print ts.dumpString()
    ts = getTableSpace(archive, "SP2")
    ts.dropAll()
    print "dropped all SP2"
    print
    ts = getTableSpace(archive, "SP1")
    print "after drop of SP2: SP1 gives"
    print ts.dumpString()

def test(archive="/tmp/supplier_parts", prefix="SP1", full=True):
    print "testing table space implementation"
    if full:
        print "creating new archive and new table space"
        ts = newTableSpace(archive, prefix)
    else:
        print "finding or creating table space in existing archive"
        ts = getTableSpace(archive, prefix)
    ts.defTable("Supplier", "S", "SNO")
    ts.defTable("Sells", "L", "SNO", "PNO")
    ts.insert("Supplier", sno=1, sname="smith", city="london", misc="a very unpleasant person")
    d1 = list(ts.dumpGen())
    for x in d1:
        print x
    ts.store()
    if not full:
        return
    ts = getTableSpace(archive, prefix)
    d2 = list(ts.dumpGen())
    compareDumps(d1, d2)
    ts.insert("Supplier", sno=2, sname="jones", city="paris", misc="likes bunnies boiled broiled or in a stew")
    ts.insert("Supplier", sno=3, sname="adams", city="vienna", misc="owns and sells bunnies as a hobby")
    ts.insert("Supplier", sno=31, sname="adams", city="vienna", misc="this is a typo")
    ts.insert("Supplier", sno=4, sname="blake", city="rome", misc="has no discernible personality")
    ts.insert("Supplier", sno=5, sname="bongo", city="mahwah", misc="erroneous")
    ts.insert("Sells", sno=1, pno=1, misc="always late")
    ts.insert("Sells", sno=1, pno=2)
    ts.insert("Sells", sno=2, pno=4)
    ts.insert("Sells", sno=3, pno=1)
    ts.insert("Sells", sno=3, pno=3)
    ts.insert("Sells", sno=3, pno=2)
    ts.insert("Sells", sno=4, pno=3)
    ts.insert("Sells", sno=4, pno=4)
    test = ts.match("Supplier", sname='bongo')
    if not len(test)==1:
        raise ValueError, "inserted data not found by match"
    ts.deleteByValues("Supplier", sname='bongo')
    test = ts.match("Supplier", sname='bongo')
    if not len(test)==0:
        raise ValueError, "deleted data found by match"    
    test = ts.match("Supplier", city='xxx')
    if not len(test)==0:
        raise ValueError, "bogus data found by match"
    test = ts.match("Supplier", sno=31)
    if len(test)!=1:
        print test
        raise ValueError, "undeleteted 31 not found"
    ts.delete("Supplier", sno=31)
    test = ts.match("Supplier", sno=31)
    if not len(test)==0:
        raise ValueError, "deleted data by id found by match"
    d1 = list(ts.dumpGen())
    for x in d1:
        print x
    ts.store()
    ts = getTableSpace(archive, prefix)
    d2 = list(ts.dumpGen())
    compareDumps(d1, d2)
    ts.defTable("Part", "P", "PNO")
    ts.store()
    ts = getTableSpace(archive, prefix)
    ts.insert("Part", PNO=1, PNAME="Screw", PRICE=10)
    d1 = list(ts.dumpGen())
    for x in d1:
        print x
    ts.store()
    ts = getTableSpace(archive, prefix)
    d2 = list(ts.dumpGen())
    compareDumps(d1, d2)
    ts.insert("Part", PNO=2, PNAME="Nut", PRICE=8)
    t1 = list(ts.testQueryGenFunction("ts", "Part", PRICE=8)())
    for x in t1:
        print x
    ts.store()
    t2 = list(ts.testQueryGenFunction('ts', 'Part',
                                      PRICE=8,
                                      ) ( [
           {
        'i' :  prefix+'P2',
        'pname' : 'Nut',
        'pno' : 2,
        'price' : 8,
        'tablename' : 'Part',
           }
           ] ))
    compareDumps(t1, t2)
    ts.insert("Part", PNO=3, PNAME="Bolt", PRICE=15)
    ts.insert("Part", PNO=4, PNAME="Cam", PRICE=25)
    def partsForSupplier(sname):
        for sdict in ts.match("Supplier", sname=sname):
            snum = sdict["sno"]
            for spdict in ts.match("Sells", sno=snum):
                pnum = spdict["pno"]
                for pdict in ts.match("Part", pno=pnum):
                    yield pdict["pname"]
    pblake = list(partsForSupplier("blake"))
    if pblake!=['Bolt', 'Cam']:
        raise ValueError, "expected ['Bolt', 'Cam'] but got "+repr(pblake)
    print "parts for blake", pblake
    boiledSelect = ts.selectByValues("Supplier")
    boiledSelect.getQuery().anyWord("boil")
    bb = boiledSelect()
    if bb!=[{'city': 'paris', 'sname': 'jones', 'i': prefix+'S2', 'sno': 2, 'misc': 'likes bunnies boiled broiled or in a stew', 'tablename': 'Supplier'}]:
        raise ValueError, "didn't get expected boiled suppliers "+repr(bb)
    print "boiled suppliers", bb
    boiledSelect.updateSet(city="chico")
    print "updated boiled suppliers", boiledSelect()
    boiledSelect.delete()
    print "deleted boiled suppliers", boiledSelect()
    ts.store()
    ts.dropTable("Supplier")
    print
    print "after dropping supplier"
    print ts.dumpString()
    ts.store()
    ts.dropTable("Sells")
    print
    print "after dropping sells"
    print ts.dumpString()
    ts.store()
    
def compareDumps(d1, d2):
    if d1!=d2:
        print "lengths", len(d1), len(d2)
        for (a,b) in zip(d1, d2):
            if a!=b:
                print "diff", (a,b)
        raise ValueError, "dump comparison failed"

if __name__=="__main__":
    test_twice()
