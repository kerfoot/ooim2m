#!/usr/bin/env python

import logging
import argparse
import os
import sys
import json
from m2m.M2mClient import M2mClient

def main(args):
    '''Return the list of request urls that conform to the UFrame API for the 
        partial or fully-qualified reference_designator and all telemetry types.  
        The URLs request all stream L0, L1 and L2 dataset parameters over the entire 
        time-coverage.  The urls are printed to STDOUT.
    '''
    
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
    
    if (args.reference_designator):
        instruments = uframe.search_instruments(args.reference_designator)
    else:
        instruments = uframe.instruments
        
    if not instruments:
        sys.stderr.write('No instruments found for reference designator: {:s}\n'.format(args.reference_designator))
        sys.stderr.flush()

    urls = []
    for instrument in instruments:
        request_urls = uframe.build_instrument_m2m_queries(instrument,
            stream=args.stream,
            telemetry=args.telemetry,
            time_delta_type=args.time_delta_type,
            time_delta_value=args.time_delta_value,
            begin_ts=args.start_date,
            end_ts=args.end_date,
            time_check=args.time_check,
            exec_dpa=args.no_dpa,
            application_type=args.format,
            provenance=args.no_provenance,
            limit=args.limit,
            annotations=args.no_annotations,
            user=args.user,
            email=args.email)
#            selogging=args.selogging)
           
        if request_urls:
            urls = urls + request_urls

    for url in urls:
        sys.stdout.write('{:s}\n'.format(url))
        
    return 0
    
if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('reference_designator',
        help='Partial or fully-qualified reference designator identifying one or more instruments')
    arg_parser.add_argument('--stream',
        help='Restricts urls to the specified stream name, if it is produced by the instrument')
    arg_parser.add_argument('--telemetry',
        help='Restricts urls to the specified telemetry type')
    arg_parser.add_argument('-s', '--start_date',
        help='An ISO-8601 formatted string specifying the start time/date for the data set')
    arg_parser.add_argument('-e', '--end_date',
        help='An ISO-8601 formatted string specifying the end time/data for the data set')
    arg_parser.add_argument('--time_delta_type',
        help='Type for calculating the subset start time, i.e.: years, months, weeks, days.  Must be a type kwarg accepted by dateutil.relativedelta')
    arg_parser.add_argument('--time_delta_value',
        type=int,
        help='Positive integer value to subtract from the end time to get the start time for subsetting.')
    arg_parser.add_argument('--no_time_check',
        dest='time_check',
        default=True,
        action='store_false',
        help='Do not replace invalid request start and end times with stream metadata values if they fall out of the stream time coverage')
    arg_parser.add_argument('--no_dpa',
        action='store_false',
        default=True,
        help='Execute all data product algorithms to return L1/L2 parameters <Default:False>')
    arg_parser.add_argument('--no_provenance',
        action='store_false',
        default=True,
        help='Include provenance information in the data sets <Default:False>')
    arg_parser.add_argument('-f', '--format',
        dest='format',
        default='netcdf',
        help='Specify the download format (<Default:netcdf> or json)')
    arg_parser.add_argument('--no_annotations',
        action='store_false',
        default=False,
        help='Include all annotations in the data sets <Default>:False')
    arg_parser.add_argument('-l', '--limit',
        type=int,
        default=-1,
        help='Integer ranging from -1 to 10000.  <Default:-1> results in a non-decimated dataset')
    arg_parser.add_argument('-b', '--baseurl',
        dest='base_url',
        help='Specify an alternate uFrame server URL. Must start with \'https://\' or \'http://\'.  Must be specified if UFRAME_BASE_URL environment variable is not set')
    arg_parser.add_argument('-t', '--timeout',
        type=int,
        default=120,
        help='Specify the timeout, in seconds <Default:120>')
    arg_parser.add_argument('-u', '--user',
        dest='user',
        default='_nouser',
        type=str,
        help='Add a user name to the query')
    arg_parser.add_argument('--email',
        dest='email',
        type=str,
        help='Add an email address for emailing UFrame responses to the request once sent')
    arg_parser.add_argument('--loglevel',
        help='Verbosity level',
        choices=['debug', 'info', 'warning', 'error', 'critical'],
        default='info')

    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))
