#!/usr/bin/python

# cd proj/ && find -print0 | wc -l --files0-from -

# ignore the protobuffer-generated files (they're fucking huge!)
IGNORE = ['proj/proto/RPCProtos.java',
          'proj/paxos/Messages.java',
          'proj/transactions/Messages.java']

import os, sys
out = open('linecount.html', 'w')
out.write('<html><head><title>Line Counts</title></head><body>')
totalcount = 0
def count_lines(fname):
    with open(fname, 'r') as f:
        return len(f.readlines())

linecounts = {}
for dirpath, _, files in os.walk('proj/'):
    for fname in files:
        filepath = os.path.join(dirpath, fname)
        if filepath in IGNORE:
            continue
        linecounts[filepath] = count_lines(filepath)
        totalcount += linecounts[filepath]

max_linecount = max(linecounts.values())
out.write('<table>')
barwidth = 800
def itersorteditems(mapping):
    ks = sorted(mapping.keys())
    return [(k, mapping[k]) for k in ks]
import itertools
colors = itertools.cycle(['#DDDDDD', '#AAAAAA'])
for color, (filepath, count) in zip(colors, itersorteditems(linecounts)):
    out.write('<tr><td><tt>%s</tt></td><td><span style="background-color: %s; padding-right: %dpx">%s</span></td></tr>' % (filepath, color, int((float(count) / float(max_linecount) * barwidth)), count))
out.write('</table>')
out.write('</html>')

print 'total: %d' % totalcount
