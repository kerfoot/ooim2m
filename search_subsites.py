#!/usr/bin/env python

import logging
import argparse
import json
import os
import sys
import datetime 
from m2m.M2mClient import M2mClient

def main(args):
    '''Return the list of all registered subsites in the UFrame instance or
    subsites matching the specified subsite'''
    
    status = 0
    
    # Set up the logger
    log_level = getattr(logging, args.loglevel.upper())
    log_format = '%(asctime)s:%(module)s:%(levelname)s:%(message)s [line %(lineno)d]'
    m2m_logger = logging.getLogger('m2m.M2mClient')
    m2m_logger.setLevel(log_level)
    ch = logging.StreamHandler()
    formatter = logging.Formatter(log_format)
    ch.setFormatter(formatter)
    m2m_logger.addHandler(ch)

    base_url = args.base_url
    if not base_url:
        base_url = os.getenv('UFRAME_BASE_URL')
        
    if not base_url:
        sys.stderr.write('No UFrame instance specified')
        sys.stderr.flush()
        return 1
    
    # Create the M2mClient instance
    uframe = M2mClient(base_url, timeout=args.timeout)
    
    if (args.subsite):
        subsites = uframe.search_subsites(args.subsite)
    else:
        subsites = uframe.subsites
        
    if args.json:
        sys.stdout.write('{:s}\n'.format(json.dumps(subsites)))
        return status
    
    for subsite in subsites:
        sys.stdout.write('{:s}\n'.format(subsite))
    
    return status
    
if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('subsite',
        nargs='?',
        help='Name of the array to search')
    arg_parser.add_argument('-j', '--json',
        dest='json',
        action='store_true',
        help='Return response as json.  Default is ascii text.')
    arg_parser.add_argument('-b', '--baseurl',
        dest='base_url',
        help='Specify an alternate uFrame server URL. Must start with \'http://\'.  Value is taken from the UFRAME_BASE_URL environment variable, if set')
    arg_parser.add_argument('-t', '--timeout',
        type=int,
        default=120,
        help='Specify the timeout, in seconds (Default is 120 seconds).')
    arg_parser.add_argument('-l', '--loglevel',
        help='Verbosity level',
        choices=['debug', 'info', 'warning', 'error', 'critical'],
        default='info')

    parsed_args = arg_parser.parse_args()
#    print parsed_args
#    sys.exit(0)

    sys.exit(main(parsed_args))
