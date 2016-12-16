#!/usr/bin/env python

import logging
import argparse
import json
import os
import sys
import datetime
import csv
from m2m.M2mClient import M2mClient

def main(args):
    '''Return the fully qualified reference designator list for all instruments
    if no partial or fully-qualified reference_designator is specified.  Specify
    the -s or --streams option to include metadata for all streams produced by the
    instrument(s)'''
    
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
            return status
            
        # Create a csv writer
        csv_writer = csv.writer(sys.stdout)
        if args.streams:
            cols = ['reference_designator',
                'stream']
            if args.metadata:
                cols = instruments[0].keys()
                
            csv_writer.writerow(cols)
            for instrument in instruments:
                csv_writer.writerow([instrument[k] for k in cols])
                
        return status
        
    # Otherwise, dump as json
    sys.stdout.write(json.dumps(instruments))
    
    return status
    
if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('reference_designator',
        nargs='?',
        help='Name of the instrument to search')
    arg_parser.add_argument('-s', '--streams',
        action='store_true',
        help='Display metadata for all streams produced by the instrument')
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

    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))
