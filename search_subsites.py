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
    
    # Translate the logging level string to numeric value
    log_level = getattr(logging, args.loglevel.upper())
    log_format = '%(asctime)s:%(module)s:%(levelname)s:%(message)s [line %(lineno)d]'
    logging.basicConfig(level=log_level,format=log_format)

    base_url = args.base_url
    if not base_url:
            
        base_url = os.getenv('UFRAME_BASE_URL')
        
    if not base_url:
        sys.stderr.write('No UFrame instance specified')
        sys.stderr.flush()
        return 1
    
    # Create a UFrame instance   
    
    uframe = M2mClient(base_url,
        timeout=args.timeout)
    
    # Fetch the table of contents from UFrame
#    if args.verbose:
#        t0 = datetime.datetime.utcnow()
#        sys.stderr.write('Fetching and creating UFrame table of contents...')

    # Automatically called on instantiation of the instance
#    uframe.fetch_toc()
    
    
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
#    arg_parser.add_argument('--syslogging',
#        action='store_true',
#        help='Verbose display')

    parsed_args = arg_parser.parse_args()
#    print parsed_args
#    sys.exit(0)

    sys.exit(main(parsed_args))
