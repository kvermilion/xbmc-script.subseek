
"""
balanced tree implementation using frame generators.
"""

FAST = True

if __name__ == '__main__' and FAST:
    # Import Psyco if available
    try:
        #raise ImportError # for test
        import psyco
        psyco.full()
        print "WITH PSYCO"
    except ImportError:
        print "NO PSYCO"
        pass
    # ...your code here...


TESTDIR = "../../testdata"
#TESTDIR = "/net/arw/"

import os
import frameGenerators
import bisect
from nucular import parameters

def makeEmptyTree(filename):
    emptyFrames = frameGenerators.SortedFrames( iter([None,]) )
    return TreeFromSortedFrames(emptyFrames, filename)

def openExistingTree(filename, withCaching=True):
    result = fTree()
    result.readOpen(filename, withCaching)
    return result

def TreeFromSortedFrames(sortedFrames, filename, adjustSizes=True, nodeSize=None, verbose=False):
    result = fTree(filename, nodeSize)
    result.create(sortedFrames, filename, adjustSizes=True, verbose=verbose)
    return result

def FastTreeFromDframeFiles(filePaths, filename, adjustSizes=True, nodeSize=None):
    unsortedFrames = frameGenerators.FramesFromDFrameFilePaths(filePaths)
    result = FastTreeFromUnsortedFrames(unsortedFrames, filename,
                                        adjustSizes, nodeSize)
    return result

def FastTreeFromUnsortedFrames(Frames, filename, adjustSizes=True, nodeSize=None):
    if nodeSize is None:
        nodeSize = fTree.nodeSize
    sortedFrames = Frames.SimpleSort()
    return TreeFromSortedFrames(sortedFrames, filename, adjustSizes)

def MergeTrees(T1, T2, filename, adjustSizes=True, verbose=False):
    "merge trees prefering T2 on collision"
    T1Frames = T1.LeafGenerator(None, updateCache=False)
    T2Frames = T2.LeafGenerator(None, updateCache=False)
    # T2Frames should dominate on key collisions
    MFrames = T2Frames.Merge(T1Frames, preferLeft=True, verbose=verbose)
    return TreeFromSortedFrames(MFrames, filename, adjustSizes, verbose=verbose)

class fTree:
    nodeSize = parameters.LTreeNodeSize

    fifoLimit = parameters.LTreeFifoLimit

    def __init__(self, filename=None, nodeSize=None, fifoLimit=None):
        "create or connect to fTree associated with filename"
        if nodeSize is not None:
            self.nodeSize = nodeSize
        if fifoLimit is not None:
            self.fifoLimit = fifoLimit
        self.filename = filename
        self.root = None
        self.file = None
        self.fifo = None
        self.lastLeaf = None

    def create(self, sortedFrames, filename=None, adjustSizes=True, verbose=False):
        "create tree from frame sequence all at once"
        # XXX some day factor this to support pre-made leaf level (using straight cp)
        empty = True
        nodeSize = self.nodeSize
        if filename is None:
            filename = self.filename
        toFile = file(filename, "w+b")
        # first write out the leaf level frames
        if adjustSizes:
            # adjust the sizes of leaves
            toosmall = nodeSize
            toolarge = nodeSize*2
            adjusted = sortedFrames.AdjustDataSizes(toolarge, toosmall)
        else:
            adjusted = sortedFrames
        # truncate keys min/max estimates to save node space in parents
        truncated = adjusted.TruncateKeyStats()
        # store the frames to the file
        leafLevel = 0
        if verbose:
            print "storing leaves"
        (nodeCount, levelSeek, lastFrame) = truncated.ToFileWithStats(toFile)
        if verbose:
            print "stored leaves", (nodeCount, levelSeek)
        if nodeCount>0:
            empty = False
        # add additional levels until nodeCount at the current level goes below 2
        while nodeCount>1:
            # scan child frames from file, but just loading stats, not full data
            childFrames = frameGenerators.FramesFromFileWithStats(toFile, levelSeek, noData=True)
            if verbose:
                print "building index level", leafLevel
            parentFrames = childFrames.MakeInteriorNodes(nodeSize, verbose=verbose)
            (nodeCount, levelSeek, lastFrame) = parentFrames.ToFileWithStats(toFile)
            if verbose:
                print "index built", (leafLevel, nodeCount, levelSeek)
            #pr "built level", (nodeCount, levelSeek)
            leafLevel += 1
        if empty:
            # empty case
            rootSeek = -1
        else:
            # non-empty case
            if nodeCount!=1 or lastFrame is None:
                raise ValueError, "nodeCount should always be 1 for non-empty tree at top level"
            rootSeek = levelSeek
        # store the trailer
        trailer = "\n?%s&%s" % (rootSeek, leafLevel)
        toFile.seek(0,2) # eof
        toFile.write(trailer)
        toFile.close()

    def readOpen(self, filename=None, withCaching=True):
        if filename is None:
            filename = self.filename
        self.file = fromFile = file(filename, "rb")
        # find trailer
        fromFile.seek(0,2) # eof
        trailerSeek = fromFile.tell()
        test = None
        for i in xrange(1000):
            trailerSeek -= 1
            fromFile.seek(trailerSeek)
            test = fromFile.read(1)
            if test=="?":
                break
        if test!="?":
            raise ValueError, "couldn't find start mark for trailer"
        trailerbody = fromFile.read() # read to end
        trailercomponents = trailerbody.split("&")
        trailerInts = [int(x) for x in trailercomponents]
        (rootSeek, leafLevel) = trailerInts
        #pr "open", (rootSeek, leafLevel)
        if leafLevel<0 or leafLevel>100:
            raise ValueError, "unsupported leaf level "+repr(leafLevel)
        self.leafLevel = leafLevel
        if rootSeek<0:
            # empty structure
            root = self.root = None
        else:
            root = self.root = frameGenerators.FramesFromFileWithStats(fromFile, rootSeek).OnlyFrame()
        # set up node caching
        if withCaching:
            self.fifo = []
            self.seekToNode = {}
        else:
            self.fifo = self.seekToNode = None
        return root

    def close(self):
        if self.file:
            self.file.close()
        self.file = None
        self.root = None
        self.fifo = None
        self.seekToNode = None
        self.lastLeaf = None

    def rangeFrames(self, fromKey=None, toKey=None):
        # returns a wrapper for an iterator
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

    def KeyValueGenerator(self, fromKey=None, toKey=None):
        # returns a generator, not a wrapper
        frames = self.rangeFrames(fromKey, toKey)
        for frame in frames.iter():
            if frame is None:
                break
            (keys, values) = frame.sortedKeysAndValues()
            for i in xrange(len(keys)):
                yield ( keys[i], values[i] )
        yield None # sentinel

    def firstKeyValue(self):
        g = self.KeyValueGenerator()
        for p in g:
            return p

    def get(self, key, default=None):
        # try last leaf
        lastLeaf = self.lastLeaf
        if lastLeaf is not None and lastLeaf.inRange(key):
            firstFrame = lastLeaf
        else:
            leaves = self.LeafGenerator(key)
            firstFrame = leaves.iter().next()
            if firstFrame is None:
                return default # empty range
        # this forces a cached dictionary conversion in the frame (optimization)
        dictionary = firstFrame.asDictionary()
        #pr "got dict", dictionary
        return dictionary.get(key, default)

    def has_key(self, key, forbiddenValue=None):
        test = self.get(key, forbiddenValue)
        #pr "got", test
        return test != forbiddenValue

    def __getitem__(self, key, forbiddenValue=None):
        result = self.get(key, forbiddenValue)
        if result==forbiddenValue:
            raise KeyError, "key not found in fTree "+repr((key, forbiddenValue))
        return result

    def findAtOrNextKeyValue(self, fromKey, forceNext=False):
        pairs = self.KeyValueGenerator(fromKey)
        for p in pairs:
            if p is None:
                return None
            if forceNext:
                (k,v) = p
                if fromKey!=k:
                    return p
            else:
                return p

    def LeafGenerator(self, key=None, atEnd=True, updateCache=True):
        "return leaf generator wrapper starting at best guess for leaf containing key"
        #pr "leaf Generator", (key, atEnd, updateCache)
        # if key is None start at first leaf
        fromFile = self.file
        leafLevel = self.leafLevel
        if fromFile is None:
            raise ValueError, "not open for reading"
        root = self.root
        if root is None:
            # empty structure: return empty iterator
            return frameGenerators.SortedFrames( [None,] )
        if leafLevel<1:
            #pr "single node tree: root is only leaf"
            #return frameGenerators.SortedFrames( [root, None] )
            return root.LeafGenerator(key, atEnd=atEnd)
        recordSeek = None
        # optimization: check the last leaf
        if key is not None:
            lastLeaf = self.lastLeaf
            if lastLeaf is not None:
                (llmin, llmax) = lastLeaf.getMinMax()
                if llmin<=key and llmax>key:
                    #pr "multi level tree choosing lastLeaf"
                    recordSeek = lastLeaf.getRecordSeek()
                    currentNode = lastLeaf
        else:
            # key is None: find first leaf
            currentNode = root
            for level in xrange(leafLevel):
                test = currentNode.firstKeyValue()
                if test is None:
                    raise ValueError, "logic problem: scanned to empty first child in non-empty tree"
                (dummyKey, recordSeek) = test
                #pr "  ... seeking first leaf", recordSeek
                currentNode = self._getNodeAtSeek(recordSeek)
                #pr "  ... min/max", currentNode.getMinMax()
        if recordSeek is None:
            # find the recordSeek for the right leaf to start at
            currentNode = root
            for level in xrange(leafLevel):
                test = currentNode.findAtOrNextKeyValue(key)
                if not test:
                    # past end of structure
                    #pr "   key", key, "past end"
                    return frameGenerators.SortedFrames( [None,] ) # empty sequence
                (largerKey, recordSeek) = test
                #pr "  seeking", recordSeek, "for", key
                currentNode = self._getNodeAtSeek(recordSeek)
                #pr "  min/max", currentNode.getMinMax()
        # at this point currentNode/recordSeek is at a leaf which may contain key
        leafGeneratorWrapper = frameGenerators.FramesFromFileWithStats(
            fromFile, recordSeek, self.seekToNode)
        # skip the first entry if key is not in range
        leafGenerator = leafGeneratorWrapper.iter()
        if key is not None and not atEnd and currentNode.atEnd(key): #not currentNode.inRange(key, atEnd=atEnd):
            # it is important to skip at the boundary if key is at end of currentNode!
            currentNode = leafGenerator.next() # XXX logic guarantees that first element will always be there.
            #pr "LEAF GENERATOR SKIPPED CURRENTNODE", currentNode.getMinMax()
        if self.seekToNode is not None and updateCache:
            #pr "returning cached wrapper"
            return self._updateCacheGeneratorWrapper(leafGenerator)
        #pr "returning sortedframe"
        return frameGenerators.SortedFrames(leafGenerator)

    def indexOf(self, key):
        "return estimated byte position of key in linear structure"
        # used for query optimization
        leaves = self.LeafGenerator(key).iter()
        leaf = leaves.next()
        if leaf is None:
            # must be at last index
            return self.lastIndex()
        recordSeek = leaf.getRecordSeek()
        if recordSeek is None:
            raise ValueError, "logic problem: no record seek for leaf"
        (keys, values) = leaf.sortedKeysAndValues()
        leafIndex = bisect.bisect_left(keys, key)
        result = recordSeek+leafIndex
        return result

    def lastIndex(self):
        "return estimated last seek position for last element of structure"
        root = self.root
        if root is None:
            return 0 # empty structure
        currentNode = root
        for level in xrange(self.leafLevel):
            (keys, values) = currentNode.sortedKeysAndValues()
            recordSeek = values[-1]
            currentNode = self._getNodeAtSeek(recordSeek)
        # return last position in last leaf
        return currentNode.getRecordSeek() + currentNode.getSize()
        
    def _getNodeAtSeek(self, recordSeek):
        # check cache
        seekToNode = self.seekToNode
        if seekToNode is not None:
            test = seekToNode.get(recordSeek, None)
            if test is not None:
                return test # cached!
        fromFile = self.file
        generator = frameGenerators.restoreFramesFromFileWithStats(fromFile, recordSeek)
        node = generator.next()
        if seekToNode is not None:
            return self._updateCache(node, recordSeek)
        return node

    def _updateCacheGeneratorWrapper(self, generator, leaves=True):
        if self.seekToNode is not None:
            generator = self._updateCacheGenerator(generator, leaves)
        return frameGenerators.SortedFrames(generator)

    def _updateCacheGenerator(self, generator, leaves=True):
        "pass through nodes in generator, updating cache"
        #if self.seekToNode is None:
        #    return generator # no cache ILLEGAL: can't mix return and yield
        for node in generator:
            if node is None:
                break
            self._updateCache(node)
            if leaves:
                self.lastLeaf = node
            yield node
        yield None # sentinel
    
    def _updateCache(self, node, recordSeek=None):
        seekToNode = self.seekToNode
        if seekToNode is None:
            return node # no cache
        if recordSeek is None:
            recordSeek = node.getRecordSeek()
            if recordSeek is None:
                raise ValueError, "no record seek for caching"
        if seekToNode.has_key(recordSeek):
            return node # already cached
        fifoLimit = self.fifoLimit
        seekToNode[recordSeek] = node
        fifo = self.fifo
        fifo.append(recordSeek)
        while len(fifo)>fifoLimit:
            dump = fifo[0]
            del fifo[0]
            del seekToNode[dump]
        return node


############## testing stuff

def scatterSortTreeFromDictionary(dictionary, filename, scratchFileName):
    scratchFile = file(scratchFileName, "w+b")
    pairs = dictionary.items()
    frames1 = frameGenerators.FramesFromPairs(pairs)
    frames2 = frameGenerators.FramesFromPairs(pairs)
    sortedFrames = frameGenerators.SortedFramesFromDFrames(frames1, frames2, scratchFile, 20000)
    tree = TreeFromSortedFrames(sortedFrames, filename)
    return openExistingTree(filename)

def openTreeFromDictionary(dictionary, filename):
    pairs = dictionary.items()
    frames = frameGenerators.FramesFromPairs(pairs)
    tree = FastTreeFromUnsortedFrames(frames, filename)
    return openExistingTree(filename)

def compareDictAndTree(D, tree, forbidden=None, detail=False):
    from time import time
    count = 0
    now = time()
    print "comparing", len(D), "to tree with detail=", detail
    #pr D
    if detail:
        errors = {}
        for k in D:
            count += 1
            if count%10000==1:
                print "  random access compare at", count, "detail=", detail
            v = D[k]
            try:
                v2 = tree[k]
            except:
                errors[ (k,v) ] = "missing"
            else:
                v3 = tree.get(k, forbidden)
                if v!=v2 or v!=v3 or v3==forbidden:
                    errors[ (k,v) ] = "bad values %s %s" % (repr(v2), repr(v3))
                else:
                    if not tree.has_key(k):
                        errors[ (k,v) ] = "has_key says false"
        elapsed = time()-now+0.000001
        print "elapsed", elapsed, "per second", count/elapsed
        if errors:
            items = errors.items()
            items.sort()
            for (p, r) in items:
                print "error", p, r
            raise ValueError, "errors during random access scan"
    firstpair = None
    lastkey = None
    count = 0
    Dkeys = D.keys()
    Dkeys.sort()
    now = time()
    for pair in tree.KeyValueGenerator():
        #print "pair", pair
        if firstpair is None:
            firstpair = pair
        if pair is None:
            break
        if count%10000==1:
            print "  sequential access compare at", count, "detail=", detail
        (k,v) = pair
        #pr "  k", k
        v2 = D[k]
        if v!=v2:
            raise ValueError, "tree and dict disagree in traversal "+repr((k,v,v2))
        k2 = Dkeys[count]
        #pr "  k2", k2
        if k2!=k:
            raise ValueError, "tree and dict disagree on sort order "+repr((k,k2))
        if detail:
            if lastkey is not None:
                lg = tree.LeafGenerator(lastkey, atEnd=False)
                lgleaf = iter(lg).next()
                if lgleaf is None:
                    raise ValueError, "leaf generator for last leaf produced no leaf"
                if not lgleaf.has_key(k):
                    raise ValueError, "leaf generator exclusive didn't produce leaf containing next key"
                if lastkey>=k:
                    raise ValueError, "tree key order violation "+repr((lastkey, k))
                (k3, v3) = tree.findAtOrNextKeyValue(lastkey, forceNext=True)
                if k3!=k:
                    raise ValueError, "next key doesn't work right "+repr((lastkey, k3, k))
                if v3!=v:
                    raise ValueError, "next key gives bad value "+repr((lastkey, k, v3))
            lastkey = k
        count += 1
    elapsed = time()-now+0.000001
    print "elapsed", elapsed, "per second", count/elapsed
    lenD = len(D)
    if count!=lenD:
        raise ValueError, "bad traversal count "+repr((count, lenD))
    p = tree.firstKeyValue()
    if p!=firstpair:
        raise ValueError, "got bad first pair "+repr((p,firstpair))
    items = D.items()
    items.sort()
    #print "listing items"
    #for x in items: print x
    t1 = int(lenD/3)
    t2 = t1*2
    itemsThird = items[t1:t2]
    print "checking middle third"
    if itemsThird:
        firstitem = itemsThird[0]
        lastitem = itemsThird[-1]
        startKey = firstitem[0]
        endKey = lastitem[0]
        itemsFromTree = list(tree.KeyValueGenerator(startKey, endKey))[:-1] # trim off None
        print "start, end", startKey, endKey
        if len(itemsFromTree)!=len(itemsThird):
            print "itemsFromTree"
            for x in itemsFromTree: print "   ", x
            print "itemsThird"
            for x in itemsThird: print "   ", x
            raise ValueError, "tree and dict third differ on length "+repr(
                (len(itemsFromTree),len(itemsThird)))
        if itemsFromTree!=itemsThird:
            raise ValueError, "tree and dict third differ on content"

def dictForTesting(seed="anything", size=10, valueAppend="ljj"):
    import md5
    result = {}
    valueAdd = (valueAppend,)
    for i in xrange(size):
        value = ("%s%s" % (seed, i), i)
        mix = md5.new(repr(value)).hexdigest()
        key = (mix, value)
        result[key] = value+valueAdd
    return result

CHECKKEYS = []

def test(filename1=os.path.join(TESTDIR, "fTree1.dat"),
         filename2=os.path.join(TESTDIR, "fTree2.dat"),
         filename3=os.path.join(TESTDIR, "fTree3.dat"),
         ):
    print "empty tree test"
    T = openTreeFromDictionary({}, filename1)
    if T.has_key("this"):
        raise ValueError, "found a key in empty tree"
    g = T.KeyValueGenerator()
    x = g.next()
    if x is not None:
        raise ValueError, "generated something in empty tree"
    test = T.get("this", "bogus")
    if test!="bogus":
        raise ValueError, "get returned non-default in empty tree"
    test = T.firstKeyValue()
    if test is not None:
        raise ValueError, "found first key value in empty tree"
    leaves = T.LeafGenerator(None)
    for l in leaves:
        if l is not None:
            raise ValueError, "found a leaf in empty tree"
    test = T.indexOf("anything")
    if test!=0:
        raise ValueError, "index of anything in empty tree should be 0"
    test = T.lastIndex()
    if test!=0:
        raise ValueError, "last index should be 0 in empty tree."
    kv = T.KeyValueGenerator("aaa", "zzz")
    test = kv.next()
    if test is not None:
        raise ValueError, "got something generating pairs from empty"
    T.close()
    print "empty tests ok"
    print
    print "1-elt tree test"
    T = openTreeFromDictionary({"hello": "goodbye"}, filename1)
    if T.has_key("this"):
        raise ValueError, "found a 'this' in 1-elt"
    if not T.has_key("hello"):
        raise ValueError, "didn't find 'hello' in 1-elt"
    g = T.KeyValueGenerator()
    x = g.next()
    if x!=("hello", "goodbye"):
        raise ValueError, "found bad pair in 1-elt: "+repr(x)
    y = g.next()
    if y!=None:
        raise ValueError, "end sentinel not found int 1-elt "+repr(y)
    test = T.get("this", "bogus")
    if test!="bogus":
        raise ValueError, "get returned non-default for 'this' in 1-elt"
    test = T.get("hello", "bogus")
    if test!="goodbye":
        raise ValueError, "get returned bad value for 'hello' in 1-elt"
    test = T.firstKeyValue()
    if test!=("hello", "goodbye"):
        raise ValueError, "bad first pair in 1-elt"
    leaves = T.LeafGenerator(None).iter()
    root = leaves.next()
    if root is None:
        raise ValueError, "leaf generator failed to return root in 1-elt"
    next = leaves.next()
    if next is not None:
        raise ValueError, "leaf generator gave extra leaf in 1-elt"
    index1 = T.indexOf("hello")
    index2 = T.indexOf("a")
    index3 = T.indexOf("z")
    if index1<index2:
        raise ValueError, "bad index order before hello "+repr((index1, index2))
    if index3<index1:
        raise ValueError, "bad index order after hello"
    test = T.lastIndex()
    if test<=index1:
        raise ValueError, "last index should be past hello index in 1-elt."
    kv = T.KeyValueGenerator("aaa", "zzz")
    test = kv.next()
    if test!=("hello", "goodbye"):
        raise ValueError, "unexpected pair from key/value generator"
    test = kv.next()
    if test is not None:
        raise ValueError, "got something generating pairs from 1-elt after first"
    T.close()
    print "1-elt tests ok"
    CHECKKEYS.append(1)
    for size in [0,10,100,1000,10000,100000]:
        print "testing at size", size
        D1 = dictForTesting("alpha", size)
        # this will create key collisions with D1 (50%)
        D2 = dictForTesting("alpha", int(size/2), "xxx")
        D2b = dictForTesting("beta", int(size/2))
        D2.update(D2b)
        print "   got data"
        T1 = openTreeFromDictionary(D1, filename1)
        print "   constructed tree T1"
        compareDictAndTree(D1, T1)
        print "   compared D1/T1"
        T2 = scatterSortTreeFromDictionary(D2, filename2, filename3)
        print "constructed T2"
        compareDictAndTree(D2, T2)
        print "done comparing D2/T2: now merging T1/T2"
        T3 = MergeTrees(T1, T2, filename3)
        T3.readOpen()
        print "done merging"
        D3 = D1.copy()
        D3.update(D2) # D2 pairs should dominate on collisions
        print "prepared dict for comparison"
        compareDictAndTree(D3, T3)
        print "   done testing at size", size
    
if __name__=="__main__":
    try:
        from cProfile import run
    except:
        from profile import run
    #run("test()")
    test()
    
