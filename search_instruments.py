#!/usr/bin/env python

import logging
import argparse
import json
import os
import sys
import csv
from m2m.M2mClient import M2mClient

def main(args):
    '''Return the fully qualified reference designator list for all instruments
    contained in asset management.  If a partial or fully-qualified reference designator 
    is specified, matching instruments are returned.'''
    
    # Set up the m2m.M2mClient logger
    log_level = getattr(logging, args.loglevel.upper())
    log_format = '%(asctime)s:%(module)s:%(levelname)s:%(message)s [line %(lineno)d]'
    m2m_logger = logging.getLogger('m2m.M2mClient')
    m2m_logger.setLevel(log_level)
    ch = logging.StreamHandler()
    formatter = logging.Formatter(log_format)
    ch.setFormatter(formatter)
    m2m_logger.addHandler(ch)
    
    # Set up the stream logger
    logger = logging.getLogger(__name__)
    logger.setLevel(log_level)
    sh = logging.StreamHandler()
    sh_formatter = logging.Formatter('%(module)s:%(levelname)s:%(message)s [line %(lineno)d]')
    sh.setFormatter(sh_formatter)
    logger.addHandler(sh)

    base_url = args.base_url
    if not base_url:
        base_url = os.getenv('UFRAME_BASE_URL')
        
    if not base_url:
        logger.error('No UFrame instance specified')
        return 1
    
    # Create the M2mClient instance
    toc = None
    if args.tocfile:
        if not os.path.isfile(args.tocfile):
            logger.error('Invalid TOC json file ({:s})'.format(args.tocfile))
            return 1
        try:
            with open(args.tocfile) as fid:
                toc = json.load(fid)
        except (OSError, ValueError) as e:
            logger.error(e)
            return 1
            
    uframe = M2mClient(base_url, timeout=args.timeout, toc=toc)
    
    if args.reference_designator:
        if args.streams:
            instruments = uframe.instrument_to_streams(args.reference_designator)
        else:
            instruments = uframe.search_instruments(args.reference_designator)
    else:
        instruments = uframe.instruments
        
    # Dump as csv records if specified via -c/--csv
    if args.csv:
        if not instruments:
            return 0
            
        # Create a csv writer
        csv_writer = csv.writer(sys.stdout)
        if args.streams:
            cols = ['reference_designator',
                'stream']
            if args.metadata:
                cols = instruments[0].keys()
                cols.sort()
                
            csv_writer.writerow(cols)
            for instrument in instruments:
                csv_writer.writerow([instrument[k] for k in cols])
                
        return 0
        
    # Otherwise, dump as json
    sys.stdout.write(json.dumps(instruments))
    
    return 0
    
if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('reference_designator',
        nargs='?',
        help='Name of the instrument to search')
    arg_parser.add_argument('-s', '--streams',
        action='store_true',
        help='Include streams produced by the instrument')
    arg_parser.add_argument('-m', '--metadata',
        action='store_true',
        help='Display metadata for all streams produced by the instrument')
    arg_parser.add_argument('-c', '--csv',
        dest='csv',
        action='store_true',
        help='Print results as comma-separated value records')
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
    arg_parser.add_argument('--tocfile',
        help='JSON file containing a full copy of the UI table of contents.  If specified, this file is used instead of fetching it from the system (MUCH faster!)')

    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))
