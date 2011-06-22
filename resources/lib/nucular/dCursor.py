
class PrimaryKeyError(KeyError):
    "could not find primary key"

class PrimaryKeyIdMaker:
    """
    Construct identifiers by concatenating primary keys.
    """
    def __init__(self, tableNameAttribute, prefix=None, separator="", lower=True):
        self.prefix=prefix
        self.separator = separator
        self.tableNameAttribute = tableNameAttribute
        self.nameToKeySequence = {}
        self.nameToSuffix = {}
        self.lower = lower
    def addPrimaryKeyList(self, tableName, suffix, *keySequence):
        if self.lower:
            keySequence = [x.lower() for x in keySequence]
        self.nameToKeySequence[tableName] = keySequence
        self.nameToSuffix[tableName] = suffix
    def addPrefix(self, query):
        "specify that identifier must start with prefix, if given"
        if self.prefix:
            query.prefixAttribute("i", self.prefix)
    def id_fragments(self, D, partial=False):
        "get identity matching dictionary (or identity prefix is partial)"
        try:
            tableName = D[self.tableNameAttribute]
        except KeyError:
            if partial:
                if self.prefix:
                    return [self.prefix]
                else:
                    return []
            raise PrimaryKeyError,"table name not provided "+repr(self.tableNameAttribute)
        try:
            keyAttributeSequence = self.nameToKeySequence[tableName]
            suffix = self.nameToSuffix[tableName]
        except KeyError:
            raise PrimaryKeyError, "no such table known "+repr(tableName)
        #pr "D=", D
        #valueSequence = [ D[name] for name in keyAttributeSequence ]
        valueSequence = []
        for name in keyAttributeSequence:
            try:
                v = D[name]
            except KeyError:
                if partial:
                    break
                raise PrimaryKeyError, "key attribute %s not found" % repr(name)
            else:
                valueSequence.append(v)
        useq = map(unicode, valueSequence)
        reprSequence = map(repr, useq)
        reprSequence = [ x[2:-1] for x in reprSequence ] # drop quotes and leading u
        if suffix:
            reprSequence.insert(0, str(suffix))
        if self.prefix:
            reprSequence.insert(0, str(self.prefix))
        return reprSequence
    def fakeId(self, suffix, *stringList):
        "make a fake id (for metadata, etc)"
        L = list(stringList)
        if suffix:
            L.insert(0, str(suffix))
        if self.prefix:
            L.insert(0, str(self.prefix))
        identity = self.separator.join(L)
        return identity
    def __call__(self, D, partial=False):
        reprSequence = self.id_fragments(D, partial)
        identity = self.separator.join(reprSequence)
        #pr "identity=", identity
        return identity

def lowerD(D):
    result = {}
    for k in D.keys():
        result[k.lower()] = D[k]
    return result

class dCursor:
    """
    Simplified Nucular interface for inserting, retrieving, replacing, deleting dictionaries in an archive.
    """
    def __init__(self, session, idMaker, addRestrictions=None, lower=True):
        self.addRestrictions = addRestrictions
        self.session = session
        self.idMaker = idMaker
        self.lower=lower
    def store(self, lazy=True):
        #pr "storing dCursor"
        self.session.store(lazy=lazy)
    def addDict(self, D):
        if self.lower:
            D = lowerD(D)
        identity = self.idMaker(D)
        #pr "cCursor addDict", identity, D
        self.session.indexDictionary(identity, D)
    def add(self, **args):
        self.addDict(args)
    def addDicts(self, sequence):
        for D in sequence:
            self.addDict(D)
    def delDict(self, D):
        "delete entry with identity matching D"
        if self.lower:
            D = lowerD(D)
        #pr "dCursor delDict", D
        identity = self.idMaker(D)
        self.session.remove(identity)
    def delete(self, **args):
        self.delDict(args)
    def replaceDict(self, D):
        self.delDict(D)
        self.addDict(D)
    def replace(self, **args):
        self.replaceDict(args)
    def updateDicts(self, whereDict, setDict):
        for dictionary in self.matchDictIter(whereDict):
            d = dictionary.copy()
            d.update(setDict)
            self.delDict(dictionary)
            self.addDict(d)
    def update(self, **whereDict):
        def update_inner(**setDict):
            return self.updateDicts(whereDict, setDict)
        return update_inner
    def matchDictIds(self, D):
        # check for primary key
        if self.lower:
            D = lowerD(D)
        #pr "matchDictIds", D
        try:
            id = self.idMaker(D)
        except PrimaryKeyError:
            pass
        else:
            # optimized
            #pr "matching id", id
            if self.session.hasIdentity(id):
                #pr "optimized matchDictIds", id
                # must check for value matches outside of the key!
                outDict = self.session.describe(id).asDictionary()
                for (k,v) in D.keys():
                    if not outDict.has_key(k):
                        return [] # missing value is "no match"???
                    if outDict[k]!=v:
                        return [] # differing value is "no match"
                return [id]
            else:
                #pr "optimized matchDictIds (empty)"
                return []
        query = self.session.Query()
        if self.addRestrictions:
            self.addRestrictions(query)
        for (name, value) in D.items():
            query.matchAttribute(name, value)
        (result, status) = query.evaluate()
        if result:
            ids = result.identities()
            #pr "matchDictIds", len(ids), ids[:10], "..."
            return ids
        else:
            raise ValueError, "query evaluation failed "+repr(status)
    def matchDictIter(self, D, startIndex=0):
        ids = self.matchDictIds(D)
        for identity in ids[startIndex:]:
            if self.session.hasIdentity(identity):
                outdict = self.session.describe(identity).asDictionary()
                #pr "matchDictIter yields", outdict
                yield outdict
    def match(self, startIndex=0, **args):
        return matchDictIter(args, startIndex)
    def deleteAllDict(self, D):
        #pr "deleteAllDict", D
        ids = self.matchDictIds(D)
        for identity in ids:
            #pr "deleteAllDict remove", identity
            self.session.remove(identity)
    def deleteAll(self, **args):
        return self.deleteAllDict(args)


