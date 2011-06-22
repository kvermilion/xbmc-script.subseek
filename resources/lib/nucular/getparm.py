
"helper for parsing command line parameters"

def getparm(L, name, default=None, getValue=True):
    "get a parameter from the command line L like --this or --name value"
    v = default
    if name in L:
        i = L.index(name)
        v = True
        if getValue:
            try:
                v = L[i+1]
            except IndexError:
                raise ValueError, "parameter %s requires a value" % repr(name)
            del L[i+1]
        del L[i]
    return v
