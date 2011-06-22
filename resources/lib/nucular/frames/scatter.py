"""
Scatter sort.

In go (keys, values) sequences unordered or dictionaries.
Out come (keys, values) sequences in key order as a generator.

Method brief outline:
- pick bucket break points, while constructing a bucket chain
- scatter pairs by break points into smaller bucket chains
- recursively scatter chains that are longer than one.
- generate (keys, values) sequence in key order from unitary chains, or recursively.
"""

import marshal
import cmarshal # "compressed marshal"
from random import randint, random
from bisect import bisect_right, bisect_left
import types
from nucular import parameters

# XXX next step, maybe: collect statistics during construction of subchains...

FAST = False

TESTDIR = "../testdata/"
TESTDIR = "/net/arw/tmp"

BUCKETSIZE = parameters.LTreeBucketSize
SCATTERLIMIT = 100
OVERSAMPLE = 10
RECURSIONMINIMUM = 100
TOOLARGEDATASIZE = parameters.LTreeNodeSize

COMPRESS = False

if COMPRESS:
    marshal = cmarshal # use compressed marshal

# for debug/test
TINYTEST = False
if TINYTEST:
    BUCKETSIZE = 1000
    SCATTERLIMIT = 5
    OVERSAMPLE = 3
    RECURSIONMINIMUM = 2

class ScatterSorter:
    "Collector for scatter sort procedure"

    verbose = False
    
    def __init__(self, scratchFile, youngerToRight=True,
                 bucketBytes=BUCKETSIZE, maxScatter=SCATTERLIMIT, overSample=OVERSAMPLE,
                 recursionMinimum=RECURSIONMINIMUM):
        # if youngerToRight is set then on insert key collisions prefer the value
        # from the later insert (it represents the younger value), otherwise prefer
        # the existing value.
        if type(youngerToRight) is not types.BooleanType:
            raise ValueError, "ytr must be bool"
        self.youngerToRight = youngerToRight
        self.scratchFile = scratchFile
        self.bucketBytes = bucketBytes
        self.maxScatter = maxScatter
        self.recursionMinimum = recursionMinimum
        self.samplesSize = (maxScatter*overSample)
        self.keySamples = [None] * self.samplesSize
        self.entryCount = 0
        self.firstChain = None #ScatterBucket()
        self.totalBytes = 0
        self.overSample = overSample
        self.sampled = False
        
    def putSequences(self, keys, values, byteSize=None):
        "record keys and values and sample keys"
        D = {}
        for i in xrange(len(keys)):
            D[ keys[i] ] = values[i]
        self.putDictionary(D, byteSize)

    def analyseDictionary(self, D, byteSize=None):
        "record statistics about dictionary contents: store byte count and randomly selected keys"
        if self.sampled:
            raise ValueError, "too late, samples computed"
        dumps = marshal.dumps
        startCount = self.entryCount
        lenD = len(D)
        self.entryCount = entryCount = startCount + lenD
        samplesSize = self.samplesSize
        keySamples = self.keySamples
        if entryCount<=samplesSize:
            # sample all the keys (fill in the samples)
            allkeys = D.keys()
            keySamples[startCount:entryCount] = allkeys
        else:
            # sample a subset of the keys
            # fill in any blank part of the samples
            if startCount<samplesSize:
                keys = D.keys()
                remainder = samplesSize-startCount
                for i in xrange(remainder):
                    k = keys[i]
                    keySamples[i+startCount] = k
            # choose some of the keys to sample
            selectionProbability = samplesSize / float(entryCount)
            ss1 = samplesSize-1
            # do contortions to reduce the number of calls to random here since it seems to be expensive
            zeroOrOne = 0
            if random()<selectionProbability:
                zeroOrOne = 1
            nsamples = selectionProbability * lenD + zeroOrOne
            if nsamples:
                skip = max(int(lenD/nsamples),1) # number of keys to skip
                count = 0
                msize = 0
                for k in D:
                    count+=1
                    if count%skip==0:
                        index = int(random()*ss1)
                        keySamples[index] = k
                        if byteSize is None:
                            v = D[k]
                            msize += len(dumps(k))+len(dumps(v))
                    if byteSize is None:
                        byteSize = msize
        if byteSize is None:
            # get byte size
            byteSize = len(dumps(D))
        # finally record bytes
        self.totalBytes += byteSize
        
    def putDictionary(self, D, byteSize=None, level=0, analyse=True):
        "record pairs to sort from dictionary (and collect statististics on samples)"
        # if youngerToRight is set then pairs from D should dominate on collision.
        youngerToRight = self.youngerToRight
        if self.sampled:
            raise ValueError, "too late, samples computed "+repr((level, len(D), byteSize))
        if not D:
            return # do nothing on empty dict
        #pr level, "putDictionary", len(D), byteSize
        #pr D
        bucket = self.firstChain
        if bucket is None:
            bucket = self.firstChain = ScatterBucket()
        bucket.putDictionary(D, byteSize, youngerToRight)
        # ALWAYS push the bucket! (fewer calculations that way)
        #self.totalBytes += byteSize
        self.firstChain = bucket.push(self.scratchFile)
        # after push byteCount will always be valid
        #self.totalBytes += bucket.byteCount
        if analyse:
            byteSize = bucket.byteCount
            self.analyseDictionary(D, byteSize)
            #pr "byteSizes", byteSize, self.totalBytes
                    
    #lcount = 0 # for debug

    def LeafGenerator(self, tooLargeDataSize=TOOLARGEDATASIZE):
        """make pairs into dFrames (None terminated -- compatible with dTree)"""
        import dframe
        import frameGenerators
        for p in self.pairsGenerator():
            if p is not None:
                (keys, values) = p
                for frame in frameGenerators.splitFrameGenerator(keys, values, tooLargeDataSize):
                    if frame is not None:
                        yield frame
                    else:
                        break
            else:
                break
        yield None # sentinel
    
    def pairsGenerator(self, level=0):
        youngerToRight = self.youngerToRight
        if self.verbose:
            print "scatterer at", level, "pairsGenerator", youngerToRight
        nextLevel = level+1
        # if there is only one bucket in chain: just return its content
        bucketChain = self.firstChain
        # mark the firstChain as not available (enable further processing)
        self.firstChain = None
        # if we are working from internal bucket chain only then check if it's too small
        # (if self.sampled is set then the data may be pre-scattered).
        if not self.sampled:
            if bucketChain is not None:
                if bucketChain.chainLength<1:
                    if self.verbose:
                        print level,"trivial unit bucket chain", len(bucketChain.D)
                    # trivial base case
                    D = bucketChain.D
                    if D:
                        keys = D.keys()
                        keys.sort()
                        values = [ D[k] for k in keys ]
                        yield (keys, values)
                    yield None # end
                    return
            else:
                # if there are no samples and there is no bucket chain then there is no data!
                yield None
                return
        elif bucketChain is not None:
            raise ValueError, "cannot process scatter sort when presampled and buckets set"
        #pr level, "nontrivial sort generation"
        scratchFile = self.scratchFile
        bucketBytes = self.bucketBytes
        maxScatter = self.maxScatter
        overSample = self.overSample
        recursionMinimum = self.recursionMinimum
        totalBytes = self.totalBytes
        # special case optimization: if size is too small, just do it the simple way (base case)
        ##pr "TOTALBYTES",  totalBytes, bucketBytes, recursionMinimum
        if bucketChain is not None and not self.sampled and totalBytes<bucketBytes*recursionMinimum:
            # ordering of pairs has been inverted in the bucketChain stack
            # (if younger values were inserted right they are now on the left)
            if self.verbose:
                print "scatter at", level, "collapsing too small bucket chain"
            youngerToRight = not youngerToRight
            pair = SortedPairFromChain(bucketChain, scratchFile, youngerToRight)
            yield pair
            yield None # end of data
            return # all done
        # determine sampling break points (if not done)
        self.setUpSampling()
        scatterBuckets = self.scatterBuckets
        scatterSamples = self.scatterSamples
        # pop buckets from primary list and scatter contents
        if bucketChain is not None:
            # bucket chain stack young/old ordering inverted
            youngerToRight = not youngerToRight
            self.scatterChain(bucketChain, scratchFile, youngerToRight)
        # unload the bucket array (rescatter if needed)
        #pr level, "scatter complete"
        count = 0
        # find the maximum bucketchain length
        maxchainLength = max( [bucket.chainLength for bucket in scatterBuckets] )
        # if the max is too large then hibernate all the buckets (store to disk to save space)
        if maxchainLength>3: #max>min(2,recursionMinimum/2):
            #pr ; #pr
            if self.verbose:
                print "scatter", level, "HIBERNATING ALL BUCKETS", len(scatterBuckets), maxchainLength
            for bucket in scatterBuckets:
                bucket.hibernate(scratchFile)
        for bucket in scatterBuckets:
            # it might be hibernated: wake it
            bucket.wake(scratchFile)
            #pr  level, "unloading bucket", len(bucket.D)
            #pr "   initial content", bucket.D
            count+=1
            if self.verbose:
                print "scatter", level, "processing bucket chain", count, "chain", bucket.chainLength, "D", len(bucket.D)
                #scatterSamples[count: count+1]
            chainLength = bucket.chainLength
            if chainLength<recursionMinimum:
                # base case 2: emit the bucket content if chain is too small
                if self.verbose:
                    print "scatter", level, "chainLength too small", bucket.chainLength, recursionMinimum
                # in the bucket stack, the young/old order is inverted (possibly again)
                pair = SortedPairFromChain(bucket, scratchFile, not youngerToRight)
                if pair:
                    yield pair
            else:
                if self.verbose:
                    print "scatter", level, "recursing for chainLength", bucket.chainLength
                # recursive case: scatter sort the chain
                subScatterer = ScatterSorter(scratchFile, not youngerToRight, bucketBytes, maxScatter, overSample)
                subScatterer.analyseChain(bucket, scratchFile)
                subScatterer.setUpSampling()
                # at the next level reverse the priority for keeping old/new
                # because values were pushed onto a stack and will come out in reverse order.
                # if they went in old first they come out new first
                subScatterer.scatterChain(bucket, scratchFile, not youngerToRight)
                subScatterer.verbose = self.verbose
                generator = subScatterer.pairsGenerator(level+1)
                for x in generator:
                    if x is not None:
                        #(ks,vs) = x
                        #pr " ... subscatter yeilded:"
                        #for (k,v) in zip(ks,vs):
                        #    pr "   ...", (k,v)
                        yield x
        if self.verbose:
            print "scatter", level, "finished"
        yield None # finished

    def getSampleKeys(self):
        if not self.sampled:
            raise ValueError, "sample keys not available until after sampling"
        return self.keySamples
        
    def setUpSampling(self):
        if self.sampled:
            return # already done!
        self.sampled = True
        keySamples = self.keySamples
        ##pr "KEYSAMPLES", keySamples
        entryCount = self.entryCount
        if len(keySamples)>entryCount:
            #pr "   keysamples truncated to", entryCount
            self.keySamples = keySamples = keySamples[:entryCount]
        nSamples = len(keySamples)
        # select break points for scattering (make sure you don't select too few)
        scatterEstimate = max(int(self.totalBytes/self.bucketBytes)*4, 10)
        scatterSize = min(self.maxScatter, scatterEstimate)
        #pr level, "scattering scatterSize", scatterSize
        keySamples.sort()
        if nSamples<=scatterSize:
            # use all the samples (this probably shouldn't happen if numbers are set sane?)
            scatterSamples = keySamples
            scatterSize = len(keySamples)
        else:
            scatterSamples = range(scatterSize)
            offset = int(nSamples/scatterSize/2)
            for i in scatterSamples[:]:
                choiceIndex = offset + int((i*nSamples)/scatterSize )
                scatterSamples[i] = keySamples[choiceIndex]
        #pr "selected scatter samples", scatterSamples
        # create buckets for scattering
        scatterBuckets = range(scatterSize+1) # one extra for too large keys
        for i in scatterBuckets[:]:
            scatterBuckets[i] = ScatterBucket()
        self.scatterSamples = scatterSamples
        self.scatterBuckets = scatterBuckets
        self.scatterSize = scatterSize
        return (scatterSamples, scatterBuckets, scatterSize)

    def analyseChain(self, bucketChain, scratchFile):
        while bucketChain:
            D = bucketChain.D
             #pr "analysing chain", len(D)
            if D:
                self.analyseDictionary(D, bucketChain.byteCount)
            bucketChain = bucketChain.pop(scratchFile)

    def scatterChain(self, bucketChain, scratchFile, youngerToRight):
        while bucketChain:
            D = bucketChain.D
             #pr "scattering chain", len(D)
            if D:
                self.scatterDict(D, youngerToRight)
            bucketChain = bucketChain.pop(scratchFile)
                
    def scatterDict(self, D, youngerToRight=True):
        if self.firstChain is not None:
            raise ValueError, "cannot scatter when firstchain is set"
        # if youngerToRight is set, then on collision pairs in D should dominate
        if not self.sampled:
            raise ValueError, "must set up samples first"
        #pr "scattering dict", len(D)
        if not D:
            return
        keys = D.keys()
        keys.sort()
        firstkey = keys[0]
        scatterSamples = self.scatterSamples
        scatterBuckets = self.scatterBuckets
        scatterSize = self.scatterSize
        scratchFile = self.scratchFile
        bucketBytes = self.bucketBytes
        scatterDict = {}
        scatterIndex = bisect_left(scatterSamples, firstkey)
        for key in keys:
            #pr "key = ", key
            # skip indices until right index for key (key>=scatterSamples[scatterIndex] or final index)
            while scatterIndex<scatterSize and key>scatterSamples[scatterIndex]:
                if scatterDict:
                    # store dict at this index
                     #pr "storing dict", scatterDict
                    scatterBucket = scatterBuckets[scatterIndex]
                    if scatterBucket.byteSize()>bucketBytes:
                        #pr level, "pushing overflowing bucket", scatterIndex, scatterSamples[scatterIndex:scatterIndex+1]
                        scatterBucket = scatterBuckets[scatterIndex] = scatterBucket.push(scratchFile)
                    #pr level, "putting", scatterDict
                    scatterBucket.putDictionary(scatterDict, youngerToRight, measure=True)
                    scatterDict = {}
                scatterIndex+=1
            value = D[key]
            scatterDict[key] = value
        # unload final dict
        if scatterDict:
            scatterBucket = scatterBuckets[scatterIndex]
            if scatterBucket.byteSize()>bucketBytes:
                #pr level, "pushing final overflow",  scatterIndex
                scatterBucket = \
                              scatterBuckets[scatterIndex] = scatterBucket.push(scratchFile)
            #pr "putting Final", scatterDict
            scatterBucket.putDictionary(scatterDict, youngerToRight, measure=True)
            #scatterDict = {}

def updatePlus(dictionary, addDictionary, youngerToRight):
    "generalized dictionary.update()"
    if youngerToRight:
        dictionary.update(addDictionary)
        return dictionary
    else:
        # prefer pairs in dictionary on collision
        ## method 1
        result = addDictionary.copy()
        result.update(dictionary)
        return result
    
        ## method 2, 3
        #for k in addDictionary:
            ## method 2
            #dictionary[k] = dictionary.get(k, addDictionary[k])
            ## method 3
            #if not dictionary.has_key(k):
            #    dictionary[k] = addDictionary[k]
        #return dictionary
        
def SortedPairFromChain(bucketChain, scratchFile, youngerToRight):
    # if youngerToRight is set then in the bucketChain younger values are deeper in the stack
    #pr "sortedpair", youngerToRight
    if bucketChain.chainLength<1:
        allD = bucketChain.D
    else:
        allD = {}
        while bucketChain:
            #allD.update(bucketChain.D)
            allD = updatePlus(allD, bucketChain.D, youngerToRight)
            bucketChain = bucketChain.pop(scratchFile)
    if allD:
        keys = allD.keys()
        keys.sort()
        values = [ allD[k] for k in keys ]
        return (keys, values)
    else:
        return None # no data

class ScatterBucket:
    "container for pairs with statistics and pointer to next container in file"
    bucketCount = 0

    def __init__(self, nextSeek=None, chainLength=0):
        ScatterBucket.bucketCount += 1
        #pr "scatter bucket", ScatterBucket.bucketCount
        self.D = {}
        self.byteCount = 0
        self.marshalData = None
        self.nextSeek = nextSeek
        self.chainLength = chainLength
        
    def byteSize(self):
        bc = self.byteCount
        if bc is not None:
            #pr "cached byte size", bc
            return bc
        #pr "BYTESIZE CALCULATES MARSHAL DATA"
        m = self.marshalData = marshal.dumps((self.nextSeek, self.D))
        result = self.byteCount = len(m)
        #pr "computed byte size", result
        return result
    
    def put(self, key, value):
        self.D[key] = value
        self.byteCount = self.marshalData = None
        
    def putDictionary(self, dictionary, youngerToRight, byteSize=None, measure=False):
        # if youngerToRight is set then dictionary values should dominate on collision
        #pr "putDict", len(dictionary), byteSize, "into", len(self.D)
        if not dictionary:
            return
        bc = self.byteCount
        if measure and bc is not None and byteSize is None:
            # measure the additional data
            # don't store the marshal data, because it will never be useful on average
            m = marshal.dumps(self.D)
            byteSize = len(m)
            #pr "MEASURED", byteSize
        D = self.D
        self.byteCount = None
        self.marshalData = None
        if not D and byteSize:
            self.byteCount = byteSize
            ##pr "put", len(D), "set byteSize", byteSize
        elif bc is not None:
            if byteSize is not None:
                self.byteCount = bc + byteSize
        #pr "  byteSize=", self.byteCount
        if D:
            #D.update(dictionary)
            D = self.D = updatePlus(D, dictionary, youngerToRight)
        else:
            # THIS IS DANGEROUS, BUT AN IMPORTANT OPTIMIZATION
            self.D = dictionary
            
    def putSequences(self, keys, values):
        D = self.D
        for i in xrange(len(keys)):
            D[keys[i]] = values[i]
        self.byteCount = self.marshalData = None
        
    def push(self, toFile):
        # seek to end
        toFile.seek(0,2)
        self.mySeek = seek = toFile.tell()
        ##pr "PUSH dumping at", seek, "size", len(self.D)
        #marshal.dump( (self.nextSeek, self.D), toFile )
        data = self.marshalData
        if data is None:
            #data = self.marshalData = marshal.dumps((self.nextSeek, self.D))
            #self.byteCount = len(data)
            marshal.dump((self.nextSeek, self.D), toFile)
            self.byteCount = toFile.tell()-seek
            #pr "PUSH DUMPED NEW MARSHAL DATA", len(self.D), self.byteCount, toFile.tell()
        else:
            toFile.write(data)
            #pr "PUSH WRITING EXISTING DATA", len(data), toFile.tell()
        newBucket = ScatterBucket(seek, self.chainLength+1)
        return newBucket
    
    def hibernate(self, toFile):
        "place contents in file to free up memory"
        self.push(toFile) # discard extra scatterbucket
        #pr "hibernating", len(self.D), self.byteCount
        self.marshalData = self.D = None # mark as hibernated
        
    def wake(self, fromFile):
        "get contents from file"
        if self.D is None:
            seek = self.mySeek
            fromFile.seek(self.mySeek)
            (dummy, self.D) = marshal.load( fromFile )
            self.byteCount = fromFile.tell() - seek
            #pr "woke", len(self.D), "seek=", self.mySeek, "nextSeek=", self.nextSeek
            
    def pop(self, fromFile):
        "get next bucket in chain"
        if self.chainLength<1:
            return None # end
        seek = self.nextSeek
        #pr "seeking from", fromFile.tell(), "to", seek
        fromFile.seek(seek)
        data = marshal.load( fromFile )
        self.byteCount = fromFile.tell() - seek
        oldBucket = ScatterBucket(None, self.chainLength-1)
        (nextSeek, nextD) = data
        #pr "popped", len(nextD), "nextSeek=",nextSeek
        oldBucket.nextSeek = nextSeek
        oldBucket.D = nextD
        return oldBucket


# testing stuff

def generateKeysWithoutPriorities(scatterer):
    "for testing only"
    dfg = scatterer.LeafGenerator()
    for leaf in dfg:
        if leaf is None:
            yield None
            return
        (keys, values) = leaf.sortedKeysAndValues()
        #pr "leaf", keys
        #pr "values", values
        # zip, it turns out, is expensive
        #for pair in zip(keys, values):
        #    yield pair
        for i in xrange(len(keys)):
            yield (keys[i], values[i])
    yield None

def testpairs(s, size):
    import md5
    result = {}
    for i in xrange(size):
        value = "%s %s" % (s, i)
        key = md5.new(value).hexdigest()
        result[key] = value
        #pr (key, value)
    return result

def test1(filename=TESTDIR+"scatter2.dat"):
    "no verification, just exercise and time with large data"
    import time
    now = time.time()
    print "larger testing scattersort to ", filename
    summ = 0
    f = open(filename, "w+b")
    S = ScatterSorter(f)
    innerSize = 50000
    #innerSize = 300000
    for a in list("zebra dingo"):
        for b in list("zebra dingo"):
            print "putting", (a,b), time.time()-now, sum
            D = testpairs(a+b, innerSize)
            summ += len(D)
            S.putDictionary(D, False)
    print "generating", summ, time.time()-now
    gen = S.pairsGenerator()
    #count = 0
    for test in gen:
        if test is None:
            break
        (keys, values) = test
        print "at", keys[0], len(keys), time.time()-now
    print "done at", time.time()-now

def test0(filename=TESTDIR+"scatter.dat"):
    print "test and verify", filename
    import time
    for scale in (0,1,3,30,300,1000):
        print; print; print "*** TESTING AT SCALE", scale, filename
        now = time.time()
        print "testing priority sort to ", filename
        f = open(filename, "w+b")
        S = ScatterSorter(f)
        maxpriority = 5
        keyrange = 30
        for key1 in range(scale):
            store = {}
            for priority in range(5): #key2 in range(keyrange):
                for key2 in range(keyrange): #priority in range(5):
                    key = (key2, key1)
                    # priority is implicit from insertion order
                    store[key] = priority
                    #store[(key, priority)] = priority
                    #pr "store", (key,priority), priority
                #pr "putting", len(store)
                S.putDictionary(store) # this will do duplicate insertions
        dgen = generateKeysWithoutPriorities(S)
        for key2 in range(keyrange):
            for key1 in range(scale):
                next = dgen.next()
                expect = (key2, key1)
                #pr "comparing", (next, expect)
                if next is None:
                    raise ValueError, "got end of sequence at "+repr(expect)
                (k,v) = next
                if k!=expect:
                    raise ValueError, "got unexpected key "+repr((k,expect))
                if v!=maxpriority-1:
                    raise ValueError, "got unexpected value "+repr((v,maxpriority-1))
        next = dgen.next()
        if next is not None:
            raise ValueError, "expected end of sequence, got "+repr(next)
        print "priority test for", scale, "checks out"
        print
        print "testing scattersort to ", filename
        f = open(filename, "w+b")
        #S = ScatterSorter(f, 100, 10, 10) # purposefully small for test/debug
        S = ScatterSorter(f)
        allD = {}
        for c in list("zebra dingo"):
            print "putting dictionary", repr(c), len(allD)
            D = testpairs(c, ord(c)*scale)
            S.putDictionary(D)
            allD.update(D)
        print; print; print
        print "preparing test structures"
        allkeys = allD.keys()
        allkeys.sort()
        print "making generator"
        gen = S.pairsGenerator()
        then = time.time()
        print "generator made", now-then
        print;print;print
        print "testing", len(allD)
        count = 0
        for pair in gen:
            if pair is None:
                break
            (keys, values) = pair
            #pr "got keys", keys
            #pr "with values", values
            #for (k,v) in zip(keys,values):
            for i in xrange(len(keys)):
                k = keys[i]
                v = values[i]
                skey = allkeys[count]
                if count%10000==0:
                    print "at", (k,v), time.time()-now, count
                count += 1
                sval = allD[skey]
                #pr "checking", (k,v), (skey, sval)
                if skey!=k:
                    raise ValueError, "keys don't match "+repr((skey, k))
                if sval!=v:
                    raise ValueError, "values don't match "+repr((sval,v))
        if count!=len(allkeys):
            raise ValueError, "bad count "+repr((count, len(allkeys)))
        end = time.time()
        elapsed = end-now
        print "test passed", elapsed, end-then
    print "all test0 passed"

if __name__=="__main__":
    #import profile
    #profile.run("test0()")
    if FAST:
        test0()
        #test1()
    else:
        try:
            from cProfile import run
        except:
            from profile import run
        run("test0();test1()")

