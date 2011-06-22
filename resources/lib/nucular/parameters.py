
"""
This file defines magic numbers used by other components.

Generally speaking larger numbers mean greater use of resources
but faster execution times.  However too large numbers may result
in memory thrashing, out of memory exceptions, or other degradations
-- so configuring the values is a bit of a dark art.
"""

# By default queries will truncate results that take more than this number of seconds to evaluate
DefaultQueryTimeLimit = 20

# Queries will refuse to fully evaluate
# query plans that do not have an initial estimate
# QueryEvalMax or smaller.
QueryEvalMax = 10000000

# Queries will switch from intersecting Id lists to
# examining values when
#   #currentIds * switchfactor < #subqueryEstimate
QuerySwitchFactor = 5000

# This is the estimated desired size for buckets of
# information to pass around when building the ltree indices
# (if memory is thrashing lower this number.)
# (if memory is available and builds are slow, raise this number).
LTreeBucketSize = 400000

# the approximate node size in every balanced LTree node
LTreeNodeSize = 10000

# the number of nodes to keep cached per LTree tree
LTreeFifoLimit = 100
 
# truncate value strings to this length in value index
ValueIndexLengthTruncation = 80

import gc

class gcPolicy:
    "magic object which on allocation sets up gc, on deallocation resets gc"
    # this relies on reference counting and auto deallocation of locals.
    def __init__(self):
        test = self.test = gc.isenabled()
        if test:
            pass
            gc.disable()
    def __del__(self):
        if self.test:
            pass
            gc.enable()
