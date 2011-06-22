"""
plug replacement for marshal supporting automatic compression
"""

import marshal
import zlib

def dump(obj, toFile):
    # could inline
    toFile.write(dumps(obj))

def loads(s):
    "avoid this one if possible"
    lc = marshal.loads(s)
    llc = len(marshal.dumps(lc))
    compressedObject = s[llc:]
    marshalledObject = zlib.decompress(compressedObject)
    return marshal.loads(marshalledObject)

def dumps(obj):
    marshalledObject = marshal.dumps(obj)
    compressedObject = zlib.compress(marshalledObject)
    lc = len(compressedObject)
    data = marshal.dumps(lc)+compressedObject
    return data

def load(fromFile):
    lc = marshal.load(fromFile)
    compressedObject = fromFile.read(lc)
    marshalledObject = zlib.decompress(compressedObject)
    return marshal.loads(marshalledObject)

def test(filename = "cmarshalTestFile.dat"):
    import os
    objects = [123, None, "a string", ["a", "list"], (False, "tuple")]
    f = file(filename, "wb")
    for o in objects:
        print "dumping", repr(o)
        d = dumps(o)
        l = loads(d)
        if o!=l:
            raise ValueError, "no match "+repr((o,l))
        dump(o, f)
    f.close()
    f = file(filename, "rb")
    for o in objects:
        print "loading", repr(o)
        l = load(f)
        if o!=l:
            raise ValueError, "no match "+repr((o,l))
    test = f.read()
    if test!="":
        raise ValueError, "didn't read to end"
    os.unlink(filename)
    print "tests ok"
    

if __name__=="__main__":
    test()

