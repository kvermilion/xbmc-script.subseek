
"""
Place a "white" tree (mapping) over a "grey" tree (mapping).
Entries in white tree dominate entries in grey key.
White entries with value None mean "deleted entry".
All inserts/deletes/updates are effected only in the white
tree.

This version is designed specifically to work with ftree/dframe generator objects.
"""

# XXXX ADD "FRAME AFTER" CACHING FOR LEAF TRAVERSAL (clear on any update)

import os
import fTree
import dframe
import frameGenerators

TESTDIR = "../../testdata/"

class ShadowTree:
    def __init__(self, white, grey, caching=True):
        #pr "shadowTree init", hex(id(self))
        self.caching = caching
        self.white = white
        self.grey = grey
        self.lastLeaf = None
    def close(self):
        self.lastLeaf = None
        for t in (self.white, self.grey):
            if t:
                t.close()
        self.white = self.grey = None
    def __setitem__(self, key, value):
        if value is None:
            raise ValueError, "None is a forbidden value for shadow tree"
        #pr "shadowTree setitem", (key,value)
        self.lastLeaf = None
        self.white[key] = value
    def putDictionary(self, dictionary):
        # no value checking
        #pr "shadowTree.putdict", dictionary
        self.lastLeaf = None
        self.white.putDictionary(dictionary)
    def delDictionary(self, dictionary):
        #pr "shadowTree.deldict", dictionary
        # no value checking
        self.lastLeaf = None
        D = {}
        for x in dictionary:
            D[x] = None
        self.white.putDictionary(D)
    def __delitem__(self, key):
        #pr "shadowTree.delitem", key
        self.lastLeaf = None
        self.white[key] = None # saves checking in grey (expensive)
    def get(self, key, default=None):
        LL = self.lastLeaf
        if LL is not None and LL.inRange(key):
            #pr "get from last leaf"
            if LL.has_key(key):
                test = LL.get(key)
                if test is not None:
                    return test
                return default
        for t in (self.white, self.grey):
            if t.has_key(key):
                test = t[key]
                if test is None:
                    # deleted
                    return default
                else:
                    return test
        return default
    def __getitem__(self, key):
        #pr "shadowtree getitem", key, "for", hex(id(self))
        LL = self.lastLeaf
        if LL is not None and LL.inRange(key):
            #pr "getitem from last leaf for", key
            if LL.has_key(key):
                test = LL.get(key)
                if test is not None:
                    #pr "leaf keys", LL.keys
                    #pr "leaf values", LL.values
                    #pr "last leaf returns", test
                    return test
                raise KeyError, "key explicitly deleted in last leaf "+repr(key)
        for t in (self.white, self.grey):
            #pr "getitem from", t
            if t.has_key(key):
                test = t[key]
                if test is None:
                    # deleted
                    raise KeyError, "key explicitly deleted "+repr(key)
                else:
                    #pr t, "returns", test
                    return test
        raise KeyError, "key missing "+repr(key)
    def has_key(self, key, default=None):
        LL = self.lastLeaf
        if LL is not None and LL.inRange(key):
            #pr "has_key from last leaf"
            if LL.has_key(key):
                test = LL.get(key)
                if test is not None:
                    return True
                return False
        for t in (self.white, self.grey):
            if t.has_key(key):
                test = t[key]
                if test is None:
                    # deleted
                    return False
                else:
                    return True
        return False
    def indexOf(self, key):
        "approximate byte position: counts delete marks too"
        return self.white.indexOf(key)+self.grey.indexOf(key)
    def lastIndex(self):
        "approximate last structure position"
        return self.white.lastIndex() + self.grey.lastIndex()
    def finalize(self):
        self.white.finalize()
        if self.grey is not None:
            self.grey.close()
    def _generateFromLeaf(self, leaf):
        "generate leaves starting at leaf"
        #pr "yeilding leaf"
        yield leaf
        (mn, mx) = leaf.getMinMax(check=True) # get true maximum value
        # this should not cause an infinite recursion because atEnd=False
        for frame in self.LeafGenerator(mx, atEnd=False, clean=True):
            if frame is None:
                break
            #pr "yielding frame after leaf", frame.getMinMax(check=True)
            yield frame
        yield None # sentinel
    def LeafGenerator(self, fromKey=None, atEnd=True, clean=False, updateCache=True):
        # XXXX clean should be used when copying tree (only)
        # XXXX need to add some sort of caching mechanism! (how?)
        #pr "leafGenerator", (fromKey, atEnd, clean), self.caching, hex(id(self))
        LL = self.lastLeaf
        if LL is not None and LL.inRange(fromKey, atEnd=atEnd):
            # optimization: start at last leaf
            #pr "   leaf generator choosing leaf", LL.getMinMax(check=True)
            frames = self._generateFromLeaf(LL)
            result = frameGenerators.SortedFrames(frames)
        else:
            #pr "leaf generator merging leaves"#, LL.getMinMax(check=True)
            wgen = self.white.LeafGenerator(fromKey, atEnd=atEnd)
            ggen = self.grey.LeafGenerator(fromKey, atEnd=atEnd)
            result = wgen.Merge(ggen)
            # not atEnd requires checking null removal
            if clean or not atEnd:
                result = result.RemoveNoneValuesIfExploded(fromKey, atEnd=atEnd)
        # if atEnd is False then white/grey frames may be incorrectly aligned: don't cache
        if self.caching and updateCache and atEnd:
            # to guarantee correct range testing for cache in future queries, must trim the first frame
            if fromKey is not None:
                #pr "trimming for fromKey"
                result = result.Range(fromKey)
            #pr "leaf generator caching result"
            result = frameGenerators.SortedFrames(self.cacheGenerator(result))
        return result
    cacheCount = [0] # DEBUG
    def cacheGenerator(self, generator):
        for frame in generator:
            self.cacheCount[0] += 1 # DEBUG
            #pr "caching", frame, hex(id(self)), self.cacheCount # DEBUG
            #if self.cacheCount[0]==226: raise "debug halt"
            if frame is None:
                break
            self.lastLeaf = frame
            yield frame
        yield None # sentinel
    def RangeFrames(self, fromKey=None, toKey=None):
        leaves = self.LeafGenerator(fromKey)
        return leaves.Range(fromKey, toKey)
    def rangeLists(self, fromKey, toKey, truncateSize=None):
        "simple implementation for debugging"
        keys = []
        values = []
        for pair in self.KeyValueGenerator(fromKey):
            if pair is None:
                break
            (k,v) = pair
            if k>toKey:
                break
            keys.append(k)
            values.append(v)
        return (keys, values)
    def rangeLists0(self, fromKey, toKey, truncateSize=None):
        "this is semi-historical: ignoring truncate size for now"
        frames = self.RangeFrames(fromKey, toKey)
        L = []
        keycount = 0
        for f in frames:
            if f is None:
                break
            (keys, values) = f.sortedKeysAndValues()
            if None in values:
                # clean out the Nones
                newKeys = keys[:]
                newValues = values[:]
                outcount = 0
                for i in xrange(len(keys)):
                    k = keys[i]
                    v = values[i]
                    if v is not None:
                        newKeys[outcount] = k
                        newValues[outcount] = v
                        outcount += 1
                keys = newKeys[:outcount]
                values = newValues[:outcount]
            keycount += len(keys)
            L.append( (keys, values) )
        allPair = frameGenerators.getPairFromListOfPairs(L, keycount)
        return allPair
    def KeyValueGenerator(self, fromKey=None, toKey=None):
        #pr "keyvaluegenerator", (fromKey, toKey), hex(id(self))
        frames = self.RangeFrames(fromKey, toKey)
        for f in frames:
            if f is None:
                break
            (keys, values) = f.sortedKeysAndValues()
            for i in xrange(len(keys)):
                v = values[i]
                if v is not None:
                    yield (keys[i], values[i])
        yield None # sentinel
    def firstKeyValue(self):
        g = self.KeyValueGenerator()
        return g.next()
    def findAtOrNextKeyValue(self, fromKey, forceNext=False):
        pairs = self.KeyValueGenerator(fromKey)
        #pr "findAtOrNext", (fromKey, forceNext)
        for p in pairs:
            if p is None:
                #pr "findAtOrNext at end"
                return None
            if forceNext:
                #pr "forceNext", p
                (k,v) = p
                if fromKey!=k:
                    #pr "forceNext returns", p
                    return p
            else:
                #pr "returning", p
                return p
    def cursor(self):
        return ShadowCursor(self)
            
class ShadowCursor:
    "historical: primarily for testing"
    def __init__(self, tree):
        self.tree = tree
        self.lastPair = None
        self.pairGenerator = None
    def current(self):
        return self.lastPair
    def set_range(self, key):
        self.pairGenerator = self.tree.KeyValueGenerator(key)
        self.lastPair = self.next()
    def next(self):
        pairGenerator = self.pairGenerator
        if pairGenerator is None:
            #pr "next not positioned: returning None"
            return None
        test = pairGenerator.next()
        # skip None values
        while test and test[1] is None:
            #pr "next skipping", test
            test = pairGenerator.next()
        if test is None:
            self.pairGenerator = None
        #pr "next returning", test
        return test

########## testing

def test(size=1000, file1=os.path.join(TESTDIR, "shBase.dat"), detail=False):
    for version in range(2):
        print size, version, "preparing test data", file1
        mod1 = max(3, int(size/23))
        mod2 = max(5, int(size/13))
        div1 = max(2, int(size/3701))
        initial = {}
        adds = {}
        dels = {}
        for i in xrange(size):
            k = "k"+repr(i)
            v = "v"+repr(i)
            v2 = "v"+repr(i)
            #print "adding", k
            initial[k] = v
            if (i%mod1)<div1:
                #print "deleting", k
                dels[k] = None
            if (i%mod2)<div1:
                #print "modifying", k
                adds[k] = v2
        final = initial.copy()
        final.update(adds)
        for k in dels:
            del final[k]
        caching = version==0
        caching = True
        print size, version, "loading tree"
        greyTree = fTree.openTreeFromDictionary(initial, file1)
        whiteTree = dframe.DataFrame()
        shadowTree = ShadowTree(whiteTree, greyTree, caching)
        if version<1:
            for (k,v) in adds.items():
                shadowTree[k] = v
            for k in dels:
                del shadowTree[k]
        else:
            shadowTree.putDictionary(adds)
            shadowTree.delDictionary(dels)
        print size, version, "testing tree with caching", caching
        fTree.compareDictAndTree(final, shadowTree, detail=detail)
        print
    print "tests complete"

if __name__=="__main__":
    try:
        from cProfile import run
    except:
        from profile import run
    run("test(100000, detail=True)")
    #for size in (10, 100, 1000, 10000, 1000000):
    #    test(size)
