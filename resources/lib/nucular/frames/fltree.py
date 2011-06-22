
"""
Layered Shadowed Merge Frame Tree implementation.
See ../../doc/ltree.html for design notes.
"""

import os, time, sys

import fTree
import dframe
import fshadowtree
import frameGenerators
import scatter
import nucular.parameters
import fileSystemSurrogate

largeTree = fTree.fTree
smallTree = dframe.DataFrame
shadowTree = fshadowtree.ShadowTree

# file layout data structures

UNDECIDED = "und"
RECENT = "rec"
TRANSIENT = "trn"
BASE = "bse"

LEVELS = [UNDECIDED, RECENT, TRANSIENT, BASE]

PREPARED = "prep"
WAITING = "wait"
ACTIVE = "actv"
RETIRED = "retr"

COLLECTIONS = [PREPARED, WAITING, ACTIVE, RETIRED]

FSLAYOUT = {
    UNDECIDED: [ACTIVE, RETIRED],
    RECENT: COLLECTIONS,
    TRANSIENT: [PREPARED, ACTIVE, RETIRED],
    BASE: [PREPARED, ACTIVE, RETIRED],
    }

BUCKETSIZE = nucular.parameters.LTreeBucketSize

VERBOSE = False

class LayeredArchive:

    verbose = VERBOSE

    def __init__(self, path, create=False, check=True, fsSurrogate=None):
        "Open or create archive."
        #pr "opening archive", (path, create, check)
        if fsSurrogate is None:
            fsSurrogate = fileSystemSurrogate.chooseSurrogate()
        self.fsSurrogate = fsSurrogate
        self.path = path
        if not fsSurrogate.isdir(path):
            if create:
                fsSurrogate.mkdir(path)
            else:
                raise ValueError, "no such archive directory "+repr(path)
        # check or create file system layout
        if create:
            self.tryDestroy()
            for (level, collections) in FSLAYOUT.items():
                lpath = fsSurrogate.join(path, level)
                try:
                    fsSurrogate.mkdir(lpath)
                except OSError:
                    pass
                for collection in collections:
                    cpath = fsSurrogate.join(lpath, collection)
                    ##pr "making", cpath
                    try:
                        fsSurrogate.mkdir(cpath)
                        ##pr "made"
                    except OSError:
                        pass
            # insert empty active base mapping
            timestamp = self.newTimeStamp()
            # "NEW TIMESTAMP", timestamp
            bname = self.baseName(timestamp)
            kt = fTree.makeEmptyTree(bname)
        if check:
            for (level, collections) in FSLAYOUT.items():
                lpath = fsSurrogate.join(path, level)
                if not fsSurrogate.exists(lpath):
                    raise ValueError, "no such archive/level directory "+repr(lpath)
                for collection in collections:
                    cpath = fsSurrogate.join(lpath, collection)
                    if not fsSurrogate.exists(cpath):
                        raise ValueError, "no such archive/level/collection dir"+(
                            repr(cpath))
            # check for active transient and base mappings
            currentBaseName = self.currentBaseName()
            if currentBaseName is None:
                raise ValueError, "can't find current base mapping file on init"
            #currentTransientName = self.currentTransientName()
        # create these when needed
        self.Base = None
        self.Transient = None
        self.TransientIsEmpty = False
        self.Recent = None
        self.RecentIsEmpty = False

    def baseName(self, timestamp, level=BASE, collection=ACTIVE,
                 prefix="b", suffix=".ktree"):
        "return path to base archive component for this timestamp"
        fsSurrogate = self.fsSurrogate
        result = fsSurrogate.join(self.path, level, collection, prefix+timestamp+suffix)
        # "BASENAME", result
        return result
    
    def transientName(self, timestamp, collection=ACTIVE):
        "return path to transient archive component for this timestamp"
        return self.baseName(timestamp, level=TRANSIENT, collection=collection,
                             prefix = "t")
    
    def sessionFileName(self, sessionname):
        "return temporary file name associated with this session"
        return "s%s.tiny" % sessionname

    def sessionFileNames(self, sessionname, fromList):
        "find elements of fromList that refer to self sessionname"
        #pr "looking for session files in", len(fromList)
        result = []
        lookfor = "_s%s.tiny" % sessionname
        for fn in fromList:
            if fn.endswith(lookfor):
                result.append(fn)
        #pr "found", len(result)
        return result

    def currentBaseName(self, level=BASE, collection=ACTIVE):
        "find most recent (greatest timestamp) base archive"
        fsSurrogate = self.fsSurrogate
        dirname = fsSurrogate.join(self.path, level, collection)
        files = fsSurrogate.listdir(dirname)
        # "looking in", dirname, "found", files
        while files and not files[-1].endswith(".ktree"):
            del files[-1]
        if not files:
            return None
        files.sort()
        lastfilename = files[-1]
        result = fsSurrogate.join(dirname, lastfilename)
        return result

    def currentTransientName(self, collection=ACTIVE):
        "find most recent transient archive"
        return self.currentBaseName(level=TRANSIENT, collection=collection)

    def newTimeStamp(self):
        return newTimeStamp()

    def tryDestroy(self):
        try:
            if self.verbose:
                print "tryDestroy"
            self.destroy()
            if self.verbose:
                print "tryDestroy completed"
        except:
            if self.verbose:
                print "tryDestroy aborted"
                print sys.exc_type, sys.exc_value
            pass
        
    def destroy(self):
        "clear and delete directories for archive"
        fsSurrogate = self.fsSurrogate
        for (level, collections) in FSLAYOUT.items():
            lpath = fsSurrogate.join(self.path, level)
            if fsSurrogate.isdir(lpath):
                for collection in collections:
                    cpath = fsSurrogate.join(lpath, collection)
                    if fsSurrogate.isdir(cpath):
                        for fn in fsSurrogate.listdir(cpath):
                            fpath = fsSurrogate.join(cpath, fn)
                            if self.verbose:
                                print "unlink", fpath
                            try:
                                fsSurrogate.unlink(fpath)
                            except OSError:
                                if self.verbose: print "failed to unlink", fpath
                                pass
                        if self.verbose:
                            print "rmdir", cpath
                            print fsSurrogate.listdir(cpath)
                        try:
                            fsSurrogate.rmdir(cpath)
                        except OSError:
                            if self.verbose: print "failed to rmdir", cpath
                            pass
                if self.verbose:
                    print "rmdir", lpath
                try:
                    fsSurrogate.rmdir(lpath)
                except OSError:
                    if self.verbose: print "failed to rmdir", lpath
                
    lastSessionName = None
    
    def newSessionName(self):
        return newSessionName()
    
    def newSessionMapping(self, readonly=False):
        # XXXX need to find a way to guarantee no collisions in session names.
        # XXXX also need to obfuscate session names (to prevent guessing).
        sessionname = self.newSessionName()
        # "SESSIONNAME GOT", sessionname
        while sessionname == self.lastSessionName:
            time.sleep(0.00111)
            #sessionname = "%s_%s" % (self.newTimeStamp(), pid)
            sessionname = self.newSessionName()
        self.lastSessionName = sessionname
        sessionmapping = self.sessionMapping(sessionname, readonly)
        return (sessionmapping, sessionname)
    
    def sessionMapping(self, sessionname, readonly=False):
        "construct a mapping corresponding to self session name"
        fsSurrogate = self.fsSurrogate
        return SessionMapping(self, sessionname, readonly, fsSurrogate)
    
    def undecidedTree(self, sessionname, readonly=False):
        "find or create the undecided shadow layer for this session"
        # when readonly is false it is assumed that only one thread
        # updates this structure at any given interval. (one thread per session)
        fsSurrogate = self.fsSurrogate
        fn = self.sessionFileName(sessionname)
        directory = fsSurrogate.join(self.path, UNDECIDED, ACTIVE)
        fpath = fsSurrogate.join(directory, fn)
        if fsSurrogate.exists(fpath):
            # "UNDECIDED FOUND AT EXISTING PATH", fpath
            result = smallTree(fpath)
            writeable = not readonly
            result.readOpen(writeable=writeable)
        else:
            # "NO UNDECIDED FOUND AT PATH", fpath
            if readonly:
                # no local updates
                return None
            result = smallTree(fpath)
            result.create()
        return result

    def recentMapping(self, sessionname):
        "find the recent shadow layer for self session"
        fsSurrogate = self.fsSurrogate
        #pr "LOOKING FOR RECENT", sessionname
        result = None
        # union all mappings from Recent/Active
        activeDir = fsSurrogate.join(self.path, RECENT, ACTIVE)
        activeFileNames = fsSurrogate.listdir(activeDir)
        # add in any waiting files for self session
        waitingDir = fsSurrogate.join(self.path, RECENT, WAITING)
        waitingFiles = fsSurrogate.listdir(waitingDir)
        #pr "active files", activeFileNames
        #pr "waiting files", waitingFiles
        sessionFiles = self.sessionFileNames(sessionname, waitingFiles)
        #pr "session files", sessionFiles
        waitingTest = {}
        for x in sessionFiles:
            waitingTest[x] = 1
        allFiles = activeFileNames + sessionFiles
        # process the files in increasing timestamp order
        #  to leave most recent updates last.
        allFiles.sort()
        for fn in allFiles:
            if waitingTest.has_key(fn):
                fpath = fsSurrogate.join(waitingDir, fn)
            else:
                fpath = fsSurrogate.join(activeDir, fn)
            if fpath.endswith(".tiny"):
                if result is None:
                    if self.verbose:
                        print "creating recent", fpath
                    result = smallTree(fpath)
                    result.readOpen()
                else:
                    if self.verbose:
                        print "adding recent", fpath
                    result.readOpen(fpath, update=True)
        #pr "recent mapping", result
        return result
    
    def transientMapping(self, collection=ACTIVE):
        "find the transient mapping shadow layer (if any)"
        name = self.currentTransientName(collection=collection)
        if self.verbose:
            print "transient name is", name
        if name is None:
            return None
        # "opening transient", name
        tree = largeTree(name)
        tree.readOpen()
        #self.Transient = tree
        return tree
    
    def baseMapping(self, collection=ACTIVE):
        "find the base mapping layer"
        #if self.Base:
        #    return self.Base
        name = self.currentBaseName(collection=collection)
        if name is None:
            raise ValueError, "could not find base mapping"
        tree = largeTree(name)
        tree.readOpen()
        #self.Base = tree
        return tree
    
    def emptyCheck(self, directory, lockFileName, verbose=verbose):
        "check that the directory is empty: if it is create lockfile there and return the path, otherwise return None"
        # this might need to be tightened up some day...
        fsSurrogate = self.fsSurrogate
        files = fsSurrogate.listdir(directory)
        #pr "checking", directory
        if files:
            if verbose: print "found files", directory, files
            return None
        path = fsSurrogate.join(directory, lockFileName)
        f = file(path, "w")
        f.write(path)
        f.close()
        #pr "check passes", path
        return path
    
    def lockPrepareDirectories(self, lockFileName, verbose=verbose):
        fsSurrogate = self.fsSurrogate
        TransientPrepareDir = fsSurrogate.join(self.path, TRANSIENT, PREPARED)
        BasePrepareDir = fsSurrogate.join(self.path, BASE, PREPARED)
        transientTestFile = self.emptyCheck(TransientPrepareDir, lockFileName, verbose=verbose)
        baseTestFile = self.emptyCheck(BasePrepareDir, lockFileName, verbose=verbose)
        result = (transientTestFile is not None) and (baseTestFile is not None)
        if not result:
            if transientTestFile and fsSurrogate.exists(transientTestFile):
                fsSurrogate.unlink(transientTestFile)
            if baseTestFile and fsSurrogate.exists(baseTestFile):
                fsSurrogate.unlink(baseTestFile)
        return result
    
    def unlockPrepareDirectories(self, lockFileName):
        fsSurrogate = self.fsSurrogate
        TransientPrepareDir = fsSurrogate.join(self.path, TRANSIENT, PREPARED)
        BasePrepareDir = fsSurrogate.join(self.path, BASE, PREPARED)
        transientTestFile = fsSurrogate.join(TransientPrepareDir, lockFileName)
        baseTestFile = fsSurrogate.join(BasePrepareDir, lockFileName)
        if fsSurrogate.exists(transientTestFile):
            #pr "unlinking", transientTestFile
            fsSurrogate.unlink(transientTestFile)
        if fsSurrogate.exists(baseTestFile):
            #pr "unlinking", baseTestFile
            fsSurrogate.unlink(baseTestFile)
            
    def aggregateRecent(self, dieOnFailure=True, verbose=False, fast=False):
        "aggregate recent commits: return False if failed otherwise (True, numberAggregated)"
        if fast:
            return self.aggregateRecentFast(dieOnFailure=dieOnFailure, verbose=verbose)
        else:
            return self.aggregateRecentSlow(dieOnFailure=dieOnFailure, verbose=verbose)
        
    def aggregateRecentFast(self, dieOnFailure=True, verbose=False):
        "aggregate using core memory for speed (provided memory is not exhausted or swapped)."
        fsSurrogate = self.fsSurrogate
        # faster in memory version of aggregateRecent for small recent data collections
        if verbose:
            print "aggregatingRecentFast", repr(self.path)
        moveCount = 0
        ts = self.newTimeStamp()
        TransientPrepareDir = fsSurrogate.join(self.path, TRANSIENT, PREPARED)
        #BasePrepareDir = fsSurrogate.join(self.path, BASE, PREPARED)
        lockFileName = "%s.lock" % (ts,)
        gotLocks = self.lockPrepareDirectories(lockFileName, verbose=verbose)
        try:
            if not gotLocks:
                if dieOnFailure:
                    raise ValueError, "failed to lock prepare directories on attempted aggregation"
                # silently return otherwise
                return (False, 0) # unable to aggregate...
            RecentWaitingDir = fsSurrogate.join(self.path, RECENT, WAITING)
            RecentActiveDir = fsSurrogate.join(self.path, RECENT, ACTIVE)
            #RecentPrepareDir = fsSurrogate.join(self.path, RECENT, PREPARED)
            #mergeFile1 = fsSurrogate.join(RecentPrepareDir, "m%s_1.merge" % ts)
            activeFiles = fsSurrogate.listdir(RecentActiveDir)
            waitingFiles = fsSurrogate.listdir(RecentWaitingDir)
            recentFilePaths = {}
            for x in waitingFiles:
                recentFilePaths[x] = fsSurrogate.join(RecentWaitingDir, x)
            for x in activeFiles:
                recentFilePaths[x] = fsSurrogate.join(RecentActiveDir, x)
            allfiles = activeFiles + waitingFiles
            # in timestamp order, to give older updates lower priority
            if verbose:
                print "aggregating", len(allfiles), "files"
            allfiles.sort()
            #nfiles = len(allfiles)
            if not allfiles:
                if self.verbose or verbose:
                    print "no recent activity to aggregate"
                return (True, 0)
            # dump contents (ordered by priorities in case of key collisions) into a merge tree
            mergeDict = {}
            priority = 0
            if self.verbose:
                print "allfiles", allfiles
            putcount = 0
            transientActive = self.transientMapping()
            newTransientFileName = fsSurrogate.join(TransientPrepareDir, "t%s.ktree" % ts)
            allFilePaths = [ recentFilePaths[filename] for filename in allfiles ]
            UnsortedFrames = frameGenerators.FramesFromDFrameFilePaths(allFilePaths)
            SortedFrames = UnsortedFrames.SimpleSort()
            if transientActive:
                # transientActive is present: merge
                transientSortedFrames = transientActive.LeafGenerator(updateCache=False)
                allSortedFrames = SortedFrames.Merge(transientSortedFrames)
                tktree = fTree.TreeFromSortedFrames(allSortedFrames, newTransientFileName)
            else:
                # no transient active, just add new keys, values
                tktree = fTree.TreeFromSortedFrames(SortedFrames, newTransientFileName)
            if verbose or self.verbose:
                print "moved", moveCount, "now finalizing tktree", newTransientFileName
            TransientActiveDir = fsSurrogate.join(self.path, TRANSIENT, ACTIVE)
            self.moveFile(newTransientFileName, TransientActiveDir)
        finally:
            self.unlockPrepareDirectories(lockFileName)
        # move old transient/active to transient/retired
        if transientActive:
            TransientRetiredDir = fsSurrogate.join(self.path, TRANSIENT, RETIRED)
            self.moveFile(transientActive.filename, TransientRetiredDir)
        # move combined recent/waiting and recent/active to recent/retired
        RecentRetiredDir = fsSurrogate.join(self.path, RECENT, RETIRED)
        for filename in allfiles:
            filepath = recentFilePaths[filename]
            self.moveFile(filepath, RecentRetiredDir)
        return (True, moveCount)
    
    def aggregateRecentSlow(self, dieOnFailure=True, verbose=False):
        "Aggregate using disk storage to support 'any size' data sets."
        fsSurrogate = self.fsSurrogate
        # scatter sort any recent/waiting and recent/active into recent/prepare
        # -- make sure that more recent files dominate on ambiguous keys!
        ts = self.newTimeStamp()
        lockFileName = "%s.lock" % (ts,)
        gotLocks = self.lockPrepareDirectories(lockFileName)
        try:
            if not gotLocks:
                if dieOnFailure:
                    raise ValueError, "failed to lock prepare directories on attempted aggregation"
                # silently return otherwise
                return (False,0) # unable to aggregate...
            RecentWaitingDir = fsSurrogate.join(self.path, RECENT, WAITING)
            RecentActiveDir = fsSurrogate.join(self.path, RECENT, ACTIVE)
            RecentPrepareDir = fsSurrogate.join(self.path, RECENT, PREPARED)
            activeFiles = fsSurrogate.listdir(RecentActiveDir)
            waitingFiles = fsSurrogate.listdir(RecentWaitingDir)
            recentFilePaths = {}
            for x in waitingFiles:
                recentFilePaths[x] = fsSurrogate.join(RecentWaitingDir, x)
            for x in activeFiles:
                recentFilePaths[x] = fsSurrogate.join(RecentActiveDir, x)
            allfiles = activeFiles + waitingFiles
            # in timestamp order, to give older updates lower priority
            allfiles.sort()
            if not allfiles:
                if self.verbose:
                    print "no recent activity to aggregate"
                return (True, 0)
            # dump contents (marked by priorities in case of key collisions) into a scatter sort
            mergeFile1 = fsSurrogate.join(RecentPrepareDir, "m%s_1.merge" % ts)
            scratchFile = open(mergeFile1, "w+b")
            sSorter = scatter.ScatterSorter(scratchFile)
            priority = 0 # priority is now implicit from insertion order
            nallfiles = len(allfiles)
            if verbose:
                print "allfiles", nallfiles
                #now = time.time()
            #mcount = 0
            moveCount = 0
            for filename in allfiles:
                # larger timestamps get higher priority
                priority += 1
                filepath = recentFilePaths[filename]
                if self.verbose or verbose:
                    print "pre-analysing contents of", filepath, priority, "of", nallfiles 
                tree = smallTree(filepath)
                tree.readOpen()
                D = tree.asDictionary()
                sSorter.analyseDictionary(D, tree.byteSize)
            # now prepare for scattering dictionary contents
            sSorter.setUpSampling()
            sSorter.verbose = verbose
            # scatter the contents:
            priority = 0
            for filename in allfiles:
                # larger timestamps get higher priority
                priority += 1
                filepath = recentFilePaths[filename]
                if self.verbose or verbose:
                    print "scattering contents of", filepath, priority, "of", nallfiles
                tree = smallTree(filepath)
                tree.readOpen()
                D = tree.asDictionary()
                moveCount+=len(D)
                # later values override previous values...
                sSorter.scatterDict(D, youngerToRight=True)
            # dump the mergesort into a recent/prepare Ktree
            if verbose:
                print "dumped", moveCount, "now sorting/collecting"
            RecentPrepareName = fsSurrogate.join(RecentPrepareDir, "t%s.ktree" % ts)
            transientActive = self.transientMapping()
            sSorterLeaves = frameGenerators.SortedFrames(sSorter.LeafGenerator())
            if transientActive:
                # merge sSorter with transientActive into pktree
                ActiveSortedFrames = transientActive.LeafGenerator(updateCache=False)
                AllSortedFrames = sSorterLeaves.Merge(ActiveSortedFrames)
                pktree = fTree.TreeFromSortedFrames(AllSortedFrames, RecentPrepareName)
                # finalize is implicit
            else:
                # copy sSorter into pktree (no existing transient active)
                pktree = fTree.TreeFromSortedFrames(sSorterLeaves, RecentPrepareName)
            newTransientFileName = RecentPrepareName
            if verbose:
                print "cleaning up"
            # move recent/prepare mergesort to retired
            RecentRetiredDir = fsSurrogate.join(self.path, RECENT, RETIRED)
            # DELETE THE MOVEFILE (IT IS NOT OF ANY USE, EVEN FOR DEBUGGING)
            #self.moveFile(mergeFile1, RecentRetiredDir)
            scratchFile.close()
            if verbose:
                print "deleting", mergeFile1
            fsSurrogate.unlink(mergeFile1)
            TransientActiveDir = fsSurrogate.join(self.path, TRANSIENT, ACTIVE)
            self.moveFile(newTransientFileName, TransientActiveDir)
        finally:
            self.unlockPrepareDirectories(lockFileName)
        # move old transient/active to transient/retired
        if transientActive:
            TransientRetiredDir = fsSurrogate.join(self.path, TRANSIENT, RETIRED)
            self.moveFile(transientActive.filename, TransientRetiredDir)
        # move combined recent/waiting and recent/active to recent/retired
        for filename in allfiles:
            filepath = recentFilePaths[filename]
            self.moveFile(filepath, RecentRetiredDir)
        return (True, moveCount)

    def moveTransientToBase(self, dieOnFailure=True, verbose=False):
        "Combine current transient and base archives into new base archive."
        fsSurrogate = self.fsSurrogate
        # merge transient/active with base/active into base/prepare
        if verbose:
            print "moveTransientToBase", self.path
        ts = self.newTimeStamp()
        lockFileName = "%s.lock" % (ts,)
        gotLocks = self.lockPrepareDirectories(lockFileName)
        try:
            if not gotLocks:
                if dieOnFailure:
                    raise ValueError, "failed to lock prepare directories on attempted combination to base"
                # silently return otherwise
                return False # unable to aggregate...
            transientActive = self.transientMapping()
            if not transientActive:
                # nothing to merge
                return (True, 0)
            baseActive = self.baseMapping()
            oldBaseFileName = baseActive.filename
            oldTransientFileName = transientActive.filename
            #combo = shadowTree(transientActive, baseActive)
            #destinationName = self.currentBaseName(collection=PREPARED)
            ts = self.newTimeStamp()
            destinationFileName = "b%s.ktree" % ts
            destinationName = fsSurrogate.join(self.path, BASE, PREPARED, destinationFileName)
            # if the base index is empty just COPY the transient tree into the base position (???)
            baseSize = baseActive.lastIndex()
            moveCount = 0
            if verbose:
                print "base size is", baseSize
            if baseSize<1:
                if verbose:
                    print "copying old transient file"
                self.copyFile(oldTransientFileName, destinationName)
                moveCount = transientActive.lastIndex()
            else:
                if verbose:
                    print "merging active base with transient file"
                destination = fTree.MergeTrees(baseActive, transientActive, destinationName, verbose=verbose)
            # move new base/prepare to base/active
            #oldBaseFileName = self.currentBaseName(collection=ACTIVE)
            baseActiveDir = fsSurrogate.join(self.path, BASE, ACTIVE)
            finalPath = self.moveFile(destinationName, baseActiveDir)
            if finalPath==oldBaseFileName:
                raise ValueError, "new and old names should not match"
        finally:
            self.unlockPrepareDirectories(lockFileName)
        # move old base/active to base/retired
        baseRetiredDir = fsSurrogate.join(self.path, BASE, RETIRED)
        self.moveFile(oldBaseFileName, baseRetiredDir)
        # move old transient/active to transient/retired
        transientRetiredDir = fsSurrogate.join(self.path, TRANSIENT, RETIRED)
        self.moveFile(oldTransientFileName, transientRetiredDir)
        return (True, moveCount)

    #def timeOutSessions(self, seconds):
    #    not implemented
    
    def cleanUp(self, complete=False):
        "remove all retired files -- if complete remove uncommitted transaction stores too."
        for level in [UNDECIDED, RECENT, TRANSIENT, BASE]:
            self.unlinkAll(self.path, level, RETIRED)
        if complete:
            # remove all uncommitted transactions and non-active archives
            for level in FSLAYOUT:
                for sublevel in FSLAYOUT[level]:
                    if level==UNDECIDED or sublevel!=ACTIVE:
                        self.unlinkAll(self.path, level, sublevel)
                        
    def unlinkAll(self, *path):
        "unlink all files in path"
        fsSurrogate = self.fsSurrogate
        path = fsSurrogate.join(*path)
        # fix deferred operations if any
        fsSurrogate.fixFakeOperations(path)
        filenames = fsSurrogate.listdir(path)
        for fn in filenames:
            # hack for nfs filesystems
            if fn.startswith(".nfs"):
                continue
            fpath = fsSurrogate.join(path, fn)
            #print "unlinking", fpath
            fsSurrogate.unlink(fpath)
            
    def copyFile(self, fromPath, toPath):
        "copy file delegate."
        import shutil
        shutil.copyfile(fromPath, toPath)

    def moveFile(self, fromPath, toDirectory, toFileName=None, force=True):
        "move file (even if it is open by some process somewhere)."
        fsSurrogate = self.fsSurrogate
        if self.verbose:
            print "moveFile", (fromPath, toDirectory, toFileName)
        if toFileName is None:
            toFileName = fsSurrogate.split(fromPath)[-1]
        if self.verbose:
            print "toFileName=", toFileName
        toPath = fsSurrogate.join(toDirectory, toFileName)
        if fsSurrogate.exists(toPath):
            if force:
                fsSurrogate.unlink(toPath)
            else:
                raise ValueError, "cannot rename to existing file "+repr(toPath)
        if self.verbose:
            print "renaming", fromPath, toPath
        fsSurrogate.rename(fromPath, toPath)
        return toPath

# XXXX: as a possible optimization make this a subclass of ShadowTree
#   rather than a wrapper.

class SessionMapping:
    "Session interface for interacting with a layered archive."

    verbose = VERBOSE
    
    def __init__(self, archive, sessionname, readonly=False, fsSurrogate=None):
        "construct a mapping corresponding to self session name"
        if fsSurrogate is None:
            fsSurrogate = fileSystemSurrogate.chooseSurrogate()
        self.fsSurrogate = fsSurrogate
        self.archive = archive
        self.sessionname = sessionname
        self.readonly = readonly
        # building the shadow stack from the bottom up:
        # get the base/active mapping
        result = archive.baseMapping()
        # shadow with the Transient Active if present
        transient = archive.transientMapping()
        if transient is not None:
            if self.verbose:
                print "transient found", transient.lastIndex(), transient.filename
            result = shadowTree(transient, result)
        else:
            if self.verbose:
                print "no transient found"
        # shadow with the union of all recent/active
        #  and any recent/waiting for self transaction if present.
        recent = archive.recentMapping(sessionname)
        if recent is not None:
            if self.verbose:
                print "recent found", recent.filename
            result = shadowTree(recent, result)
        # shadow with undecided/active for self transaction (create it unless readonly)
        self.undecided = undecided = archive.undecidedTree(sessionname, readonly)
        if undecided is not None:
            if self.verbose:
                print "undecided found", undecided.filename
            result = shadowTree(undecided, result)
        self.tree = result

    def KeyValueGenerator(self):
        return self.tree.KeyValueGenerator()
    
    def __repr__(self):
        return "SessionMapping(id=%s, name=%s, tree=%s)" % (id(self), self.sessionname, self.tree)

    def __getitem__(self, key):
        result = self.tree[key]
        # "DLTREESESSIONMAPPING", self.tree.__class__, "on", repr(key), "give", result
        return result
    
    def rangeDict(self, fromKey, toKey, truncateSize=None):
        return self.tree.rangeDict(fromKey, toKey, truncateSize=truncateSize)
    
    def rangeLists(self, fromKey, toKey, truncateSize=None):
        return self.tree.rangeLists(fromKey, toKey, truncateSize=truncateSize)
    
    def __setitem__(self, key, value):  # aggregate!
        # "DLTREESESSIONMAPPING", id(self), repr(self.sessionname), "setitem", (key,value)
        self.tree[key] = value
    def putDictionary(self, dictionary):
        self.tree.putDictionary(dictionary)
        
    def delDictionary(self, dictionary):
        self.tree.delDictionary(dictionary)
        
    def __delitem__(self, key):
        del self.tree[key]
        
    def has_key(self, key):
        return self.tree.has_key(key)
    
    def firstKeyValue(self):
        return self.tree.firstKeyValue()
    
    def nextKeyValueAfter(self, key):
        tree = self.tree
        kv = tree.findAtOrNextKeyValue(key, forceNext=True)
        if kv:
            (k,v) = kv
            if k==key:
                raise ValueError, "not advancing "+repr((k,key))
        return kv
    
    def findAtOrNextKeyValue(self, key, forceNext=False):
        return self.tree.findAtOrNextKeyValue(key, forceNext=forceNext)
    
    def indexOf(self, key):
        return self.tree.indexOf(key)

    def lastIndex(self):
        return self.tree.lastIndex()
    
    def keysBetweenDict(self, startKey, pastEndKey, giveValues=False):
        tree = self.tree
        cursor = tree.cursor()
        cursor.set_range(startKey)
        resultD = {}
        currentpair = cursor.current()
        while currentpair is not None and currentpair[0]<pastEndKey:
            (k,v) = currentpair
            if giveValues:
                resultD[k] = v
            else:
                resultD[k] = 1
            currentpair = cursor.next()
        return resultD

    def getAllValues(self, keys, isSorted=False):
        # primarily for testing
        "returns values associated with keys (not necessarily in the same order)"
        if not isSorted:
            keys = list(keys)
            keys.sort()
        tree = self.tree
        result = [ tree[key] for key in keys ]
        return result

    def store(self, waiting=False):
        "store undecided values permanently.  if waiting then defer visibility to others sessions until aggregation."
        # "STORING"
        # self
        fsSurrogate = self.fsSurrogate
        self.sync() # MIGHT NOT BE NEEDED!
        archive = self.archive
        undecided = self.undecided
        if not undecided:
            # no changes to save (readonly)
            return
        if undecided.lastIndex()<1:
            # empty structure: discard instead
            self.discard()
            return
        destination = ACTIVE
        if waiting:
            destination = WAITING
        ts = archive.newTimeStamp()
        frompath = undecided.filename
        tofilename = "r%s_s%s.tiny" % (ts, self.sessionname)
        todir = fsSurrogate.join(archive.path, RECENT, destination)
        archive.moveFile(frompath, todir, tofilename)
        
    def sync(self):
        "save undecided information to disk for possible later resumption"
        undecided = self.undecided
        if not undecided:
            return # nothing to sync
        undecided.finalize()
        
    def discard(self):
        "throw away any undecided changes"
        # "DISCARDING", self
        fsSurrogate = self.fsSurrogate
        archive = self.archive
        undecided = self.undecided
        if not undecided:
            # no changes to discard
            return
        undecided.finalize()
        filename = undecided.filename
        UndecidedRetiredDir = fsSurrogate.join(archive.path, UNDECIDED, RETIRED)
        archive.moveFile(filename, UndecidedRetiredDir)

def newTimeStamp():
    "invent a new timestamp string for the current instant (doesn't check for collision!)"
    now = time.time()
    snow = "%016.4f" % now # this will cause problems long after le deluge
    return snow.replace(".", "_")

def newSessionName():
    pid = str(os.getpid())
    sessionname = "%s_%s" % (newTimeStamp(), pid)
    return sessionname

