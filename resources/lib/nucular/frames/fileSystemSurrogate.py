"""
File system surrogate intended to fake out POSIX
like behaviour on non-POSIX file systems like NT.
On posix systems this is not needed, but I think
the added overhead is "negligible" (I hope anyway).

Basically we want to be able to unlink
or rename a file that
is currently open by this or some other process.

We implement this by attempting the operation, and
if it fails adding "marker" files which effectively
delays the operation.

RENAME:
======

Rename is only supported from one directory to another
(not within the same directory), and only for
"regular files" (not directories).

If Rename(fromDirectory, toDirectory, filename) fails
create FIND marker file

    toDirectory/"__FIND__"+filename
        content: fromDirectory/filename toDirectory/filename

and another MOVE marker file

    fromDirectory/"__MOVE__"+filename
        content: fromDirectory/filename toDirectory/filename

UNLINK
======

Unlink special support is only supported for regular files.

If Unlink(fromDirectory, filename) fails
create marker file

    fromDirectory/"__DEL__"+filename
        content: fromDirectory/filename

All required file system accesses are modified
to detect and understand these file markers as needed.

A cleanup operation attempts to remove all markers
from a directory (and related markers in other directories)
after "finishing" the failed actions.

THIS IS NOT INTENDED FOR GENERAL USE: IT MAKES
USE OF SPECIAL PROPERTIES OF THE FILE HANDLING BY NUCULAR.
"""

# XXX possible, unlikely, multiprocess race conditions for non-atomic operations.

# XXX possible infinite file reference loops (but not for the intended use in nucular)

import os
import sys
import time
import marshal

# sleep interval used for repeating operations on marker files
SLEEPINTERVAL = 0.1

FINDPREFIX = "__FIND__"
MOVEPREFIX = "__MOVE__"
DELPREFIX = "__DEL__"
PREFIXES = (FINDPREFIX, MOVEPREFIX, DELPREFIX)

def unlinkMarker(path, countLimit=100):
    count = 0
    while os.path.exists(path):
        count+=1
        if countLimit<count:
            raise ValueError, "sanity check limit on unlinkMarker polling loop exceeded "+repr(path)
        try:
            os.unlink(path)
        except:
            (typ, value, tb) = sys.exc_info()
            print "type", typ
            print "val", value
            time.sleep(SLEEPINTERVAL)

def chooseSurrogate():
    if os.name=="posix":
        #pr "choosing posix"
        return POSIX_INSTANCE
    #pr "choosing surrogate"
    return SURROGATE_INSTANCE

class PosixSurrogate:
    "the way it should be"
    def __init__(self, osInterface=None):
        if osInterface is None:
            osInterface = os
        self.osInterface = osInterface
        self.translatedPath = osInterface.path.join
        self.isdir = osInterface.path.isdir
        self.rmdir = osInterface.rmdir
        self.mkdir = osInterface.mkdir
        self.join = osInterface.path.join
        self.exists = osInterface.path.exists
        self.listdir = osInterface.listdir
        self.unlink = osInterface.unlink
        self.rename = osInterface.rename
        self.split = osInterface.path.split
    def fixFakeOperations(self, *args):
        pass # don't need to do anything in this case

POSIX_INSTANCE = PosixSurrogate()

class Surrogate:
    "use this if you don't have posix"
    def __init__(self, osInterface=None):
        if osInterface is None:
            osInterface = os
        self.osInterface = osInterface
    def translatedPath(self, *paths):
        "translate path for existing or non-existing file"
        osInterface = self.osInterface
        path = osInterface.path.join(*paths)
        test = osInterface.path.exists(path)
        (f,m,d) = self._markerNames(path)
        if test:
            # it's there: is it a deferred move or delete?
            if osInterface.path.exists(d):
                #raise IOError, "cannot access deferred deleted file "+repr(d)
                return None
            if osInterface.path.exists(m):
                #raise IOError, "cannot access deferred moved file "+repr(m)
                return None
            return path
        else:
            # it's not there, is there a find for it?
            if osInterface.path.exists(f):
                # make sure the other location is still existant
                realpath = self._findTest(f)
                if realpath is not None:
                    return realpath
                raise IOError, "find file marker points to non-existant file "+repr(f)
        return path # otherwise return real path
    def isdir(self, path):
        osInterface = self.osInterface
        return osInterface.path.isdir(path) # no difference
    def _markerNames(self, path):
        osInterface = self.osInterface
        "return filenames for marker files associated with path (find, move, delete)"
        (directory, filename) = osInterface.path.split(path)
        f = osInterface.path.join(directory, FINDPREFIX+filename)
        m = osInterface.path.join(directory, MOVEPREFIX+filename)
        d = osInterface.path.join(directory, DELPREFIX+filename)
        return (f,m,d)
    def _splitMarker(self, filename):
        "split 'real' filename from marker file name, return (prefix, filename)"
        for prefix in PREFIXES:
            if filename.startswith(prefix):
                remainder = filename[len(prefix):]
                return (prefix, remainder)
        return (None, filename)
    def split(self, path):
        # assume path has already been translated (?)
        osInterface = self.osInterface
        return osInterface.path.split(path)
    def rmdir(self, path):
        "attempt to remove a directory after clearing up deferred operations"
        # try to clear up deferred operations
        osInterface = self.osInterface
        self.fixFakeOperations(path)
        return osInterface.rmdir(path)
    def mkdir(self, path):
        "make directory"
        osInterface = self.osInterface
        return osInterface.mkdir(path)
    def join(self, *components):
        "join path components"
        return self.translatedPath(*components)
    def exists(self, path):
        "test whether a file 'exists' according to surrogate conventions"
        # ignore deferred deletes and moves, add finds
        osInterface = self.osInterface
        test = osInterface.path.exists(path)
        (f,m,d) = self._markerNames(path)
        if test:
            # it's there: is it a deferred move or delete?
            if osInterface.path.exists(d):
                return False
            if osInterface.path.exists(m):
                return False
            return True
        else:
            # it's not there, is there a find for it?
            if osInterface.path.exists(f):
                # make sure the other location is still existant
                if self._findTest(f) is not None:
                    return True
            return False
    def listdir(self, path):
        "list a directory including files that haven't been moved into the directory yet"
        # remove deferred deletes and moves, add finds
        osInterface = self.osInterface
        result0 = osInterface.listdir(path)
        resultD = {}
        removes = {}
        # add find files, remove deferred unlinks and moves
        for filename in result0:
            (prefix, remainder) = self._splitMarker(filename)
            if prefix is None:
                # the actual file exists (but may be a move or delete: check later)
                resultD[filename] = filename
            elif prefix==FINDPREFIX:
                if self._findTest(filename):
                    # the file "exists" as a deferred move to this directory
                    resultD[remainder] = filename
            elif prefix==DELPREFIX:
                # the fail is awaiting deletion
                removes[remainder] = remainder
            elif prefix==MOVEPREFIX:
                # the file is awaiting a move
                removes[remainder] = remainder
            else:
                raise ValueError, "bad prefix "+repr(prefix)
        # expunge deferred moves and unlinks
        for r in removes:
            if resultD.has_key(r):
                del resultD[r]
        result = resultD.keys()
        return result
    def _findTest(self, findFilePath):
        "iteratively find actual file path in a deferred move (or return None)"
        osInterface = self.osInterface
        visited = {}
        while 1:
            if not osInterface.path.exists(findFilePath):
                return None
            if visited.has_key(findFilePath):
                raise ValueError, "looped while searching "+repr(findFilePath)
            visited[findFilePath] = True
            fFile = file(findFilePath)
            (fromFilePath, toFilePath) = marshal.load(fFile) #fFile.read()
            fFile.close()
            # if the file exists, that's the actual file
            if osInterface.path.exists(fromFilePath):
                return fromFilePath
            # otherwise look for a find marker file for fromFilePath
            findFilePath = fromFilePath
    def unlink(self, path):
        "unlink a file or add an deletion marker file if you can't"
        osInterface = self.osInterface
        test = osInterface.path.exists(path)
        (f,m,d) = self._markerNames(path)
        # remove any mark
        unlinkMarker(m)
        if test:
            try:
                osInterface.unlink(path)
            except:  # is unlink consistent in it's errors?
                # apparently it is open and locked add a delete mark
                delfile = file(d, "w")
                delfile.write(path)
                delfile.close()
            else:
                unlinkMarker(d)
            unlinkMarker(f)
            return True # virtual unlink "succeeded"
        else:
            # it's not there, is there a find for it?
            if osInterface.path.exists(f):
                # make sure the other location is still existant
                realFile = self._findTest(f)
                # keep trying to delete the find file until success
                unlinkMarker(f)
                if realFile is not None:
                    return self.unlink(realFile)
            # almost silently fail if the file doesn't exist and there is no find
            return False

    def rename(self, fromPath, toPath):
        osInterface = self.osInterface
        fromExists = osInterface.path.exists(fromPath)
        toExists = self.exists(toPath)
        if toExists:
            raise IOError, "file system surrogate will not rename a file to an existing filename "+repr(toPath)
        if not fromExists:
            (f,m,d) = self._markerNames(fromPath)
            unlinkMarker(m)
            unlinkMarker(d)
            realFile = self._findTest(f)
            unlinkMarker(f)
            if realFile is None:
                raise ValueError, "cannot locate file to move "+repr(fromPath)
            fromFile = realFile
        try:
            osInterface.rename(fromPath, toPath)
        except:
            # fromPath is open and locked: add find and move marks
            (fromF, fromM, fromD) = self._markerNames(fromPath)
            (toF, toM, toD) = self._markerNames(toPath)
            payload = (fromPath, toPath)
            # write Find file for toPath
            findFile = file(toF, "wb")
            marshal.dump(payload, findFile)
            findFile.close()
            # write move file for fromPath
            moveFile = file(fromM, "wb")
            marshal.dump(payload, moveFile)
            moveFile.close()
            # for fun, erase bogus marker files
            for othermarker in (toM, toD, fromF, fromD):
                unlinkMarker(othermarker)
            
    def fixFakeOperations(self, directory, ignoreExceptions=True):
        "fix fake operations rooted at this directory: move any deferred moves and delete any deferred deletes"
        osInterface = self.osInterface
        realList = osInterface.listdir(directory)
        for fn in realList:
            (prefix, remainder) = self._splitMarker(fn)
            if prefix is None:
                pass # do nothing for 'regular' files
            elif prefix==FINDPREFIX:
                pass # finds are not rooted at this directory -- it's the other directory's responsibility
            elif prefix==DELPREFIX:
                # perform the deletion now
                path = osInterface.path.join(directory, fn)
                delfile = file(path)
                fileToDelete = delfile.read()
                delfile.close()
                if osInterface.path.exists(fileToDelete):
                    try:
                        osInterface.unlink(fileToDelete)
                    except:
                        if not ignoreExceptions:
                            raise
                    else:
                        # remove the delete mark too
                        unlinkMarker(path)
                else:
                    # just delete bogus marker
                    unlinkMarker(path)
            elif prefix==MOVEPREFIX:
                # perform the move now
                path = osInterface.path.join(directory, fn)
                moveFile = file(path)
                (fromPath, toPath) = marshal.load(moveFile)
                moveFile.close()
                if osInterface.path.exists(fromPath):
                    try:
                        osInterface.rename(fromPath, toPath)
                    except:
                        if not ignoreExceptions:
                            raise
                    else:
                        # remove the move mark
                        unlinkMarker(path)
                else:
                    # bogus move mark: just delete it
                    unlinkMarker(path)
                markers = self._markerNames(toPath)
                for m in markers:
                    unlinkMarker(m)
            else:
                raise ValueError, "bad prefix "+repr(prefix)

SURROGATE_INSTANCE = Surrogate()

# for testing

class bogusOs:
    "an os interface that refuses to do renames or unlinks"
    def __init__(self):
        self.path = os.path
        self.rmdir = os.rmdir
        self.mkdir = os.mkdir
        self.listdir = os.listdir
    def unlink(self, path):
        print "bogusOs refusing to unlink "+repr(path)
        raise IOError, "unlink refused "+repr(path)
    def rename(self, fromPath, toPath):
        print "bogusOs refusing to rename", (fromPath, toPath)
        raise IOError, "rename refused "+repr((fromPath, toPath))

def test(testDir="../testdata"):
    print "running file system surrogate test with bogus/test surrogate"
    b = bogusOs()
    Sbogus = Surrogate(b)
    Sposix = PosixSurrogate()
    for S in (Sbogus, Sposix):
        print "creating directories for", S
        Adir = S.join(testDir, "Adir")
        Bdir = S.join(testDir, "Bdir")
        S.mkdir(Adir)
        S.mkdir(Bdir)
        print "populating", Adir
        filesAndContent = { "file1": "content of file1", "file2": "file2 content", "file3": "file3's content" }
        for (f,c) in filesAndContent.items():
            path = Adir+"/"+f
            print "creating file", repr(path), "with content", repr(c)
            file(path, "w").write(str(c))
        checkFiles(S, filesAndContent, Adir)
        print "moving file1 to", Bdir
        fromPath = Adir+"/file1" #S.translatedPath(Adir, "file1")
        toPath = Bdir+"/file1" #S.transletedPath(Bdir, "file1")
        S.rename(fromPath, toPath)
        Bfiles = { "file1": "content of file1" }
        Afiles = filesAndContent.copy()
        del Afiles["file1"]
        checkFiles(S, Afiles, Adir)
        checkFiles(S, Bfiles, Bdir)
        print "deleting file2 from", Adir
        delPath = Adir+"/file2" #S.translatedPath(Adir, "file2")
        S.unlink(delPath)
        del Afiles["file2"]
        checkFiles(S, Afiles, Adir)
        print "moving file3 to", Bdir
        fromPath = Adir+"/file3" #S.translatedPath(Adir, "file3")
        toPath = Bdir+"/file3" #S.transletedPath(Bdir, "file3")
        S.rename(fromPath, toPath)
        checkFiles(S, {}, Adir)
        print "deleting file3 in", Bdir
        S.unlink(toPath)
        print "switching to real surrogate"
        S = Surrogate()
        print "cleaning up", Bdir
        S.fixFakeOperations(Bdir)
        checkFiles(S, {}, Adir)
        checkFiles(S, Bfiles, Bdir)
        print "removing file1 from", Bdir
        delPath = Bdir+"/file1" #S.translatedPath(Bdir, "file1")
        S.unlink(delPath)
        checkFiles(S, {}, Bdir)
        print "final clean up and removing directories", Adir, Bdir
        S.fixFakeOperations(Adir)
        S.rmdir(Adir)
        S.rmdir(Bdir)

def checkFiles(S, dict, directory):
    for (f,c) in dict.items():
        path = S.translatedPath(directory, f)
        text = file(path).read()
        if text!=c:
            raise ValueError, "text %s for %s didn't match %s" (
                repr(text), repr(f), repr(c))

if __name__=="__main__":
    test()
