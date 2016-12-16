#!/usr/bin/env python

import logging
import os
import sys
import argparse
import json
import csv
from m2m.M2mClient import M2mClient

def main(args):
    '''Display all deployment events for the full or partially qualified
    reference designator.  A reference designator uniquely identifies an
    instrument.  Results are printed as valid JSON.'''
    
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
        
    events = uframe.query_instrument_deployments(args.reference_designator,
        ref_des_search_string=args.filter,
        status=args.status)
       
    if args.csv:
        if events:
            csv_writer = csv.writer(sys.stdout)
            cols = ['reference_designator',
                'deployment_number',
                'active',
                'event_start_ts',
                'event_stop_ts',
                'event_start_ms',
                'event_stop_ms',
                'valid_event']
            csv_writer.writerow(cols)
            for event in events:
                csv_writer.writerow([event['instrument']['reference_designator'],
                    event['deployment_number'],
                    event['active'],
                    event['event_start_ts'],
                    event['event_stop_ts'],
                    event['event_start_ms'],
                    event['event_stop_ms'],
                    event['valid']])

        return 0

    if args.raw:
        sys.stdout.write('{:s}\n'.format(json.dumps(uframe.selected_raw_deployment_events)))
    else:
        sys.stdout.write('{:s}\n'.format(json.dumps(events)))
        
    return 0
    
if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('reference_designator',
        help='Partial or fully-qualified reference designator identifying one or more instruments')
    arg_parser.add_argument('-s', '--status',
        dest='status',
        type=str,
        default='all',
        choices=['active', 'inactive', 'all'],
        help='Specify the status of the deployment <default=\'all\'>')
    arg_parser.add_argument('-f', '--filter',
        dest='filter',
        type=str,
        help='A string used to filter reference designators')
    arg_parser.add_argument('-c', '--csv',
        dest='csv',
        action='store_true',
        help='Print results as comma-separated value records')
    arg_parser.add_argument('-r', '--raw',
        action='store_true',
        help='Dump the selected raw deployment events.  Events are dumped as valid JSON only')
    arg_parser.add_argument('-b', '--baseurl',
        dest='base_url',
        help='UFrame instance URL. Must begin with \'http://\'.  Default is taken from the UFRAME_BASE_URL environment variable, provided it is set.  If not set, the URL must be specified using this option')
    arg_parser.add_argument('-t', '--timeout',
        type=int,
        default=120,
        help='Request timeout, in seconds <Default=120>.')
    arg_parser.add_argument('-l', '--loglevel',
        help='Verbosity level',
        choices=['debug', 'info', 'warning', 'error', 'critical'],
        default='info')
    arg_parser.add_argument('--tocfile',
        help='JSON file containing a full copy of the UI table of contents.  If specified, this file is used instead of fetching it from the system (MUCH faster!)')
            
    parsed_args = arg_parser.parse_args()
    #print parsed_args
    #sys.exit(13)

    sys.exit(main(parsed_args))
