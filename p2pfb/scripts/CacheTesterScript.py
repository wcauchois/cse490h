#!/usr/bin/python
import argparse, time, sys

parser = argparse.ArgumentParser(description='Generate a run script for CacheNodeTester')
parser.add_argument('-n', '--nodes', dest='num_nodes', type=int, default=4, help='The number of nodes to spawn')
parser.add_argument('-t', '--time', dest='time_span', type=int, default=10, help='How many timesteps to run the simulation for')
parser.add_argument('-d', '--delay', dest='delay', type=float, default=0, help='A delay, in seconds, to sleep() between outputting timesteps')
parser.add_argument('-o', '--only-node', dest='only_node', type=int, default=-1, help='Only send commands to the specified node (other nodes will be started)')
parser.add_argument('-i', '--infinite', dest='infinite', default=False, action='store_true', help='Continue indefinitely (causes the time span option to be ignored)')

args = parser.parse_args()

def do_cmd(cmd_template, force_every=False):
    if args.only_node != -1 and not force_every:
        print cmd_template % args.only_node
    else:
        for i in xrange(args.num_nodes):
            print cmd_template % i
flush = sys.stdout.flush

do_cmd('start %i', True)
print 'time'
flush()
def time_span_generator():
    if not args.infinite:
        for t in xrange(args.time_span):
            yield t
    else:
        t = 0
        while True:
            yield t
            t += 1
try:
    for t in time_span_generator():
        do_cmd('%i think')
        print 'time'
        flush()
        if args.delay != 0:
            time.sleep(args.delay)
except KeyboardInterrupt:
    sys.exit(1)
do_cmd('%i report')
print 'time'
print 'exit'
flush()
