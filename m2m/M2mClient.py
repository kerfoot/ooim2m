import logging
import requests
import json
import time
import datetime
from dateutil import parser
from dateutil.relativedelta import relativedelta as tdelta
from pytz import timezone

# Disables SSL warnings
import requests.packages.urllib3
requests.packages.urllib3.disable_warnings()

HTTP_STATUS_OK = 200

_valid_relativedeltatypes = ('years',
    'months',
    'weeks',
    'days',
    'hours',
    'minutes',
    'seconds')
        
class M2mClient(object):
    '''Class for interacting with the OOI UFrame data-services API
    
    Parameters:
        base_url: Base url of the UFrame instance, beginning with http or https.
        timeout: timeout duration (Default is 120 seconds)
    '''
    
    def __init__(self, base_url=None, timeout=120, user=None, token=None, toc=None):
        
        self._base_url = None
        self._m2m_base_url = None
        self._timeout = timeout
        self._api_username = user
        self._api_token = token
    
        self._logger = logging.getLogger(__name__)
        
        # properties for last m2m request
        self._last_m2m_request = None
        self._last_m2m_response = None
        self._last_m2m_status_code = None
        
        # Table of contents
        self._toc = []
        self._subsites = []
        self._instruments = []
        self._parameters = []
        self._streams = []
        self._toc_response = toc
        self._static_toc = False
        if self._toc_response:
            self._static_toc = True
        
        # Deployment events
        self._selected_raw_events = []
        self._filtered_raw_events = []
        self._instrument_deployment_events = []
        
        # Set the base url
        self.base_url = base_url
        self._static_toc = False
        
    @property
    def last_m2m_request(self):
        return self._last_m2m_request
        
    @property
    def last_m2m_response(self):
        return self._last_m2m_response
        
    @property
    def base_url(self):
        return self._base_url
    @base_url.setter
    def base_url(self, url):
        
        if not url:
            self._logger.warning('No UFrame base_url specified')
            return
        if not url.startswith('http'):
            self._logger.warning('base_url must start with http')
            return
            
        self._base_url = url.strip('/')
        self._m2m_base_url = '{:s}/api/m2m'.format(self._base_url)
        
        # Fetch the table of contents and build the internal data structures 
        if not self._static_toc:
            self._build_toc()
        
    @property
    def m2m_base_url(self):
        return self._m2m_base_url
        
    @property
    def timeout(self):
        return self._timeout
    @timeout.setter
    def timeout(self, seconds):
        if type(seconds) != int:
            self._logger.warning('timeout must be an integer')
            return
            
        self._timeout = seconds
    
    @property
    def toc(self):
        return self._toc_response
            
    @property
    def instruments(self):
        return self._instruments
        
    @property
    def parameters(self):
        return self._parameters
        
    @property
    def streams(self):
        return self._streams
        
    @property
    def subsites(self):
        return self._subsites
        
    @property
    def instrument_deployment_events(self):
        return self._instrument_deployment_events
        
    @property
    def selected_raw_deployment_events(self):
        return self._filtered_raw_events
        
    @property
    def deployed_instruments(self):
        return self._active_deployment_events
        
    def search_instruments(self, target_string, metadata=False):
        '''Return the list of all instrument reference designators containing the 
        target_string from the current UFrame table of contents.
        
        Parameters:
            target_string: partial or fully-qualified reference designator
            metadata: set to True to return an array of dictionaries containing the
                instrument metadata.'''
        
        if not self._toc:
            self._logger.warning('No table of contents found')
            return []
            
        if metadata:
            return [self._toc[r] for r in self._instruments if r.find(target_string) >= 0]
        else:
            return [r for r in self._instruments if r.find(target_string) >= 0]
        
    def search_parameters(self, target_string, metadata=False):
        '''Return the list of all stream parameters containing the target_string
        from the current UFrame table of contents.
        
        Parameters:
            target_string: partial or fully-qualified parameter name
            metadata: set to True to return an array of dictionaries containing the
                parameter metadata.'''
        
        if not self._toc:
            self._logger.warning('No table of contents found')
            return []
            
        if metadata:
            return [p for p in self._parameters if p['particleKey'].find(target_string) >= 0]
        else:
            #return [p['particleKey'] for p in self._parameters if p['particleKey'].find(target_string) >= 0]
            return [p for p in self._parameters if p.find(target_string) >= 0]
    
    def search_streams(self, target_stream):
        '''Returns a the list of all streams containing the target_stream fragment
        
        Parameters:
            target_stream: partial or full stream name'''
        
        if not self._toc:
            self._logger.warning('No table of contents found')
            return []
            
        return [s for s in self._streams if s.find(target_stream) >= 0]
        
    def search_subsites(self, target_subsite):
        '''Returns a the list of all subsites containing the target_subsite fragment
        
        Parameters:
            target_subsite: partial or full subsite
            name'''
            
        arrays = []
        
        if not self._toc:
            self._logger.warning('No table of contents found')
            return arrays
            
        # Create a dict of unique array names
        arrays = [a for a in self._subsites if a.find(target_subsite) >= 0]
        arrays.sort()
        
        return arrays
        
    def stream_to_instrument(self, target_stream):
        '''Returns a the list of all instrument reference designators producing
        the specified stream
        
        Parameters:
            target_stream: partial or full stream name'''
        
        instruments = []
        
        if not self._toc:
            self._logger.warning('No table of contents found')
            return instruments
            
        for r in self._toc.keys():
            streams = [s for s in self._toc[r]['streams'] if s['stream'].find(target_stream) >= 0]
            if not streams:
                continue
            for stream in streams:
                if stream['reference_designator'] in instruments:
                    continue
                instruments.append(stream['reference_designator'])
                
        instruments.sort()
        
        return instruments
        
    def instrument_to_streams(self, reference_designator):
        '''Return the list of all streams produced by the partial or fully-qualified
        reference designator.
        
        Parameters:
            reference_designator: partial or fully-qualified reference designator to search
        '''
        
        ref_des_streams = []
        
        instruments = self.search_instruments(reference_designator)
        if not instruments:
            return ref_des_streams
        
        for instrument in instruments:
            
            streams = self._toc[instrument]['streams']
            
            for stream in streams:
                
                # Add the reference designator
                stream['reference_designator'] = instrument
                
                # Parse stream beginTime and endTime to create a unix timestamp, in milliseconds
                try:
                    stream_dt0 = parser.parse(stream['beginTime'])
	        except ValueError as e:
                    self._logger.error('{:s}: {:s} ({:s})\n'.format(stream['stream'], stream['beginTime'], e.message))
	            continue

                try:
                    stream_dt1 = parser.parse(stream['endTime'])
	        except ValueError as e:
                    self._logger.error('{:s}: {:s} ({:s})\n'.format(stream['stream'], stream['endTime'], e.message))
	            continue
                
                # Format the endDT and beginDT values for the query
                stream['beginTimeEpochMs'] = None
                stream['endTimeEpochMs'] = None
                try:
                    stream['endTimeEpochMs'] = int(time.mktime(stream_dt1.timetuple()))*1000
                except ValueError as e:
                    self._logger.error('endTime conversion error: {:s}-{:s}: {:s}\n'.format(instrument, stream['stream'], e.message))

                try:
                    stream['beginTimeEpochMs'] = int(time.mktime(stream_dt0.timetuple()))*1000
                except ValueError as e:
                    self._logger.error('beginTime conversion error: {:s}-{:s}: {:s}\n'.format(instrument, stream['stream'], e.message))

                ref_des_streams.append(stream)
                
        return ref_des_streams
        
    def get_instrument_metadata(self, reference_designator):
        '''Returns the full metadata listing for all instruments matching the
        partial or fully qualified reference designator.
        
        Parameters:
            reference_designator: partial or fully-qualified reference designator to search
        '''
        
        metadata = {}
        
        instruments = self.search_instruments(reference_designator)
        if not instruments:
            return metadata
            
        for instrument in instruments:
            
            metadata[instrument] = self._toc[instrument]
            
        return metadata
        
    def _build_toc(self):
        '''Fetch the UFrame table of contents and build the internal data structures'''
        
        if self._toc_response:
            toc = self._toc_response
        else:
            self._logger.debug('Fetching table of contents')
            toc = self._build_and_send_m2m_request(12576, '/sensor/inv/toc')
        
        if not toc:
            return
            
        self._toc_response = toc
        
        # Map the instrument metadata response to the reference designator
        self._toc = {i['reference_designator']:i for i in toc['instruments']}
        
        # Create the sorted list of reference designators
        ref_des = self._toc.keys()
        ref_des.sort()
        self._instruments = ref_des
        
        # Create a dictionary mapping parameter id (pdId) to the parameter metadata
        param_defs = {p['pdId']:p for p in toc['parameter_definitions']}
        # Loop through the toc_response['parameters_by_stream'] and create
        # an array of dictionaries containing all paramters for the specified stream
        stream_defs = {}
        for s in toc['parameters_by_stream'].keys():
            stream_params = [param_defs[pdId] for pdId in toc['parameters_by_stream'][s]]
            for p in stream_params:
                p[u'stream'] = s
                
            stream_defs[s] = stream_params
                
        # Loop through self._toc (instruments) and add the stream_params
        for i in self._toc.keys():
            self._toc[i]['instrument_parameters'] = []
            for s in self._toc[i]['streams']:
                s['reference_designator'] = i
                self._toc[i]['instrument_parameters'] = self._toc[i]['instrument_parameters'] + stream_defs[s['stream']]
                
        # Create the full list of parameter names
        parameters = [p['particle_key'] for p in toc['parameter_definitions']]
        # Create the full list of streams
        streams = stream_defs.keys()
        
        # Sort parameters
        parameters.sort()
        # Sort streams
        streams.sort()        
        self._parameters = parameters
        self._streams = streams
        
        # Create a dict of unique array names
        subsites = {t.split('-')[0]:True for t in self._toc.keys()}.keys()
        subsites.sort()
        self._subsites = subsites
        
        # Search for all actively deployed instruments
        # 2016-12-15: m2m api can't handle this volume of deployments, so wait until
        # it's fixed to create the active deployments catalog
        #self._get_active_deployments()
            
    def query_instrument_deployments(self, ref_des, ref_des_search_string=None, status=None, raw=False):
        '''Return the list of all deployment events for the specified reference
        designator, which may be partial or fully-qualified reference designator
        identifying the subsite, node or sensor.  An optional keyword argument
        (status) may be set to all, active or inactive to return all <default>,
        active or inactive deployment events'''
        
        end_point = '/events/deployment/query?refdes={:s}'.format(ref_des)
        deployment_events = self._build_and_send_m2m_request(12587, end_point)
        if not deployment_events:
            self._logger.debug('No deployment events for {:s}'.format(ref_des))
            return
            
        self._selected_raw_events = []
        self._filtered_raw_events = []
        self._instrument_deployment_events = []
         
        self._selected_raw_events = deployment_events
            
        for event in self._selected_raw_events:
            
            # Event must have a fully qualified reference designator
            if not event['referenceDesignator']['full']:
                self._logger.warning('{:s}: Invalid instrument for event id={:0.0f}\n'.format(event['eventName'], event['eventId']))
                continue
             
            # Create the fully qualified reference designator
            reference_designator = '{:s}-{:s}-{:s}'.format(
                event['referenceDesignator']['subsite'],
                event['referenceDesignator']['node'],
                event['referenceDesignator']['sensor'])   
                
            # Events must have a eventStartTime to be considered valid
            if not event['eventStartTime']:
                self._logger.warning('{:s}: Deployment event (id={:0.0f}) has no eventStartTime\n'.format(event['eventName'], event['eventId']))
                continue
            
            # Create the concise instrument deployment event object    
            deployment_event = {'instrument' : None,
                'event_start_ms' : event['eventStartTime'],
                'event_stop_ms' : event['eventStopTime'],
                'deployment_number' : event['deploymentNumber'],
                'event_start_ts' : None,
                'event_stop_ts' : None,
                'active' : False,
                'valid' : False}
            instrument = {'reference_designator' : reference_designator,
                'node' : event['referenceDesignator']['node'],
                'full' : event['referenceDesignator']['full'],
                'subsite' : event['referenceDesignator']['subsite'],
                'sensor' : event['referenceDesignator']['sensor']}
            # Add the instrument info to the event
            deployment_event['instrument'] = instrument
    
            # Parse the deployment event start time
            try:
                deployment_event['event_start_ts'] = datetime.datetime.utcfromtimestamp(deployment_event['event_start_ms']/1000).strftime('%Y-%m-%dT%H:%M:%S.%sZ')
            except ValueError as e:
                self._logger.error('Error parsing event_start_ms: {:s}\n'.format(e))
                continue
    
            # Parse the deployment event end time, if there is one
            if deployment_event['event_stop_ms']:
                try:
                    deployment_event['event_stop_ts'] = datetime.datetime.utcfromtimestamp(deployment_event['event_stop_ms']/1000).strftime('%Y-%m-%dT%H:%M:%S.%sZ')
                except ValueError as e:
                    self._logger.error('Error parsing event_start_ms: {:s}\n'.format(e))
                    continue
            else:
                # If the event does not have an end time, mark the deployment as active
                deployment_event['active'] = True
                
            # Deployment is valid
            deployment_event['valid'] = True
        
            # Optionally filter the event based on it's status (None, 'all', 'active', 'inactive')
            if status:
                if status.lower() == 'active' and not deployment_event['active']:
                    continue
                elif status.lower() == 'inactive' and deployment_event['active']:
                    continue
                    
            # Search the reference_designator for ref_des_search_string if specified
            if ref_des_search_string:
                if reference_designator.find(ref_des_search_string) == -1:
                    continue
                    
            # If we've made it here, add the event and deployment_event
            self._filtered_raw_events.append(event)
            self._instrument_deployment_events.append(deployment_event)
            
        return self._instrument_deployment_events
        
    def build_instrument_m2m_queries(self, ref_des, stream=None, telemetry=None, time_delta_type=None, time_delta_value=None, begin_ts=None, end_ts=None, time_check=True, exec_dpa=True, application_type='netcdf', provenance=True, limit=-1, annotations=False, user='_nouser', email=None, selogging=False):
        '''Return the list of request urls that conform to the UFrame m2m API for the specified
        reference_designator.
        
        Parameters:
            ref_des: partial or fully-qualified reference designator
            telemetry: telemetry type (Default is all telemetry types
            time_delta_type: Type for calculating the subset start time, i.e.: years, months, weeks, days.  Must be a type kwarg accepted by dateutil.relativedelta'
            time_delta_value: Positive integer value to subtract from the end time to get the start time for subsetting.
            begin_dt: ISO-8601 formatted datestring specifying the dataset start time
            end_dt: ISO-8601 formatted datestring specifying the dataset end time
            exec_dpa: boolean value specifying whether to execute all data product algorithms to return L1/L2 parameters (Default is True)
            application_type: 'netcdf' or 'json' (Default is 'netcdf')
            provenance: boolean value specifying whether provenance information should be included in the data set (Default is True)
            limit: integer value ranging from -1 to 10000.  A value of -1 (default) results in a non-decimated dataset
            annotations: boolean value (True or False) specifying whether to include all dataset annotations
        '''
        
        #self._last_async_request_urls = []
        #self._last_async_request_responses = []
        
        m2m_urls = []
        
        instruments = self.search_instruments(ref_des)
        if not instruments:
            return []    
        
        if time_delta_type and time_delta_value:
            if time_delta_type not in _valid_relativedeltatypes:
                self._logger.error('Invalid dateutil.relativedelta type: {:s}'.format(time_delta_type))
                return []
        
        begin_dt = None
        end_dt = None
        if begin_ts:
            try:
                begin_dt = parser.parse(begin_ts)
            except ValueError as e:
                self._logger.error('Invalid begin_dt: {:s} ({:s})'.format(begin_ts, e.message))
                return []    
                
        if end_ts:
            try:
                end_dt = parser.parse(end_ts)
            except ValueError as e:
                self._logger.error('Invalid end_dt: {:s} ({:s})'.format(end_ts, e.message))
                return []
                
        for instrument in instruments:
                
            # Get the streams produced by this instrument
            instrument_streams = self.instrument_to_streams(instrument)
            if stream:
                stream_names = [s['stream'] for s in instrument_streams]
                if stream not in stream_names:
                    self._logger.warning('{:s}: Invalid stream specified: {:s}'.format(instrument, stream))
                    continue
                    
                i = stream_names.index(stream)
                instrument_streams = [instrument_streams[i]]
                
            if not instrument_streams:
                self._logger.warning('{:s}: No valid streams found'.format(instrument))
                continue
                
            # Break the reference designator up
            r_tokens = instrument.split('-')
            
            for instrument_stream in instrument_streams:
                
                if telemetry and instrument_stream['method'].find(telemetry) == -1:
                    continue
                    
                #Figure out what we're doing for time
                dt0 = None
                dt1 = None
               
                try:
                    stream_dt0 = parser.parse(instrument_stream['beginTime'])
                except ValueError:
                    self._logger.warning('{:s}-{:s}: Invalid beginTime ({:s})'.format(instrument, instrument_stream['stream'], instrument_stream['beginTime']))
                    continue

                try:
                    stream_dt1 = parser.parse(instrument_stream['endTime'])
                except ValueError:
                    self._logger.warning('{:s}-{:s}: Invalid endTime ({:s})'.format('instrument', instrument_stream['stream'], instrument_stream['endTime']))
                    continue

                if time_delta_type and time_delta_value:
                    dt1 = stream_dt1
                    dt0 = dt1 - tdelta(**dict({time_delta_type : time_delta_value})) 
                else:
                    if begin_dt:
                        dt0 = begin_dt
                    else:
                        dt0 = stream_dt0
                        
                    if end_dt:
                        dt1 = end_dt
                    else:
                        dt1 = stream_dt1
                
                # Format the endDT and beginDT values for the query
                try:
                    ts1 = dt1.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                except ValueError as e:
                    self._logger.error('{:s}-{:s}: {:s}'.format(instrument, instrument_stream['stream'], e.message))
                    continue

                try:
                    ts0 = dt0.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                except ValueError as e:
                    self._logger.error('{:s}-{:s}: {:s}'.format(instrument, instrument_stream['stream'], e.message))
                    continue
                        
                # Make sure the specified or calculated start and end time are within
                # the stream metadata times if time_check=True
                if time_check:
                    if dt1 > stream_dt1:
                        self._logger.warning('time_check ({:s}-{:s}): End time exceeds stream endTime ({:s} > {:s})'.format(ref_des, instrument_stream['stream'], ts1, instrument_stream['endTime']))
                        self._logger.warning('time_check ({:s}-{:s}): Setting request end time to stream endTime'.format(ref_des, instrument_stream['stream']))
                        ts1 = instrument_stream['endTime']
                    
                    if dt0 < stream_dt0:
                        self._logger.warning('time_check ({:s}-{:s}): Start time is earlier than stream beginTime ({:s} < {:s})'.format(ref_des, instrument_stream['stream'], ts0, instrument_stream['beginTime']))
                        self._logger.warning('time_check ({:s}-{:s}): Setting request begin time to stream beginTime'.format(ref_des, instrument_stream['stream']))
                        ts0 = instrument_stream['beginTime']
                       
                    # Check that ts0 < ts1
                    dt0 = parser.parse(ts0)
                    dt1 = parser.parse(ts1)
                    if dt0 >= dt1:
                        self._logger.warning('{:s}: Invalid time range specified ({:s} >= {:s})'.format(instrument_stream['stream'], ts0, ts1))
                        continue

                # Create the url
                stream_url = '{:s}/12576/sensor/inv/{:s}/{:s}/{:s}-{:s}/{:s}/{:s}?beginDT={:s}&endDT={:s}&format=application/{:s}&limit={:d}&execDPA={:s}&include_provenance={:s}&selogging={:s}&user={:s}'.format(
                    self.m2m_base_url,
                    r_tokens[0],
                    r_tokens[1],
                    r_tokens[2],
                    r_tokens[3],
                    instrument_stream['method'],
                    instrument_stream['stream'],
                    ts0,
                    ts1,
                    application_type,
                    limit,
                    str(exec_dpa).lower(),
                    str(provenance).lower(),
                    str(selogging).lower(),
                    user)
                    
                if email:
                    stream_url = '{:s}&email={:s}'.format(stream_url, email)
                    
                m2m_urls.append(stream_url)
                            
        return m2m_urls
        
    def send_m2m_request(self, url):
        '''Validate and send the request url directly to the UFrame instance.  The 
        request response is returned and also stored in UFrame.last_async_response'''
        
        
        # Remove leading and trailing whitespace from the url
        request_url = url.strip()
        
        # The url must be sent to the UFrame.base_url UFrame instance
        if not request_url.startswith(self.m2m_base_url):
            return
            
        # Make sure the url begins with self.m2m_base_url
        m2m_endpoint = url[len(self.m2m_base_url)+1:]
        try:
            m2m_tokens = m2m_endpoint.split('/')
            m2m_port = int(m2m_tokens[0])
            m2m_endpoint = '{:s}'.format('/'.join(m2m_tokens[1:]))
        except ValueError as e:
            self._logger.error(e)
            return
            
        # Send the request
        self._build_and_send_m2m_request(m2m_port, m2m_endpoint)
        
        response = {'requestUrl' : self._last_m2m_request,
            'status' : False,
            'status_code' : self._last_m2m_status_code,
            'response' : self._last_m2m_response}
            
        if self._last_m2m_status_code == HTTP_STATUS_OK:
            response['status'] = True
            
        return response
        
    def _build_and_send_m2m_request(self, port, end_point):
        '''Send a UFrame API request through the m2m interface to the specified port and end_point'''
        
        if not self._base_url:
            self._logger.warning('base_url has not been specified')
            return
            
        m2m_url = '{:s}/{:0.0f}/{:s}'.format(self.m2m_base_url, port, end_point.strip('/'))
            
        try:
            if self._api_username and self._api_token:
                r = requests.get(m2m_url, auth=(self._api_username, self._api_token))
            else:
                r = requests.get(m2m_url)
        except (requests.exceptions.MissingSchema, requests.exceptions.ConnectionError) as e:
            self._logger.error('{:s}: {:s}'.format(e, m2m_url))
            return
           
        self._last_m2m_request = m2m_url
        self._last_m2m_status_code = r.status_code
        
        if r.status_code != HTTP_STATUS_OK:
            self._last_m2m_response = r.json()
            self._logger.warning(self._last_m2m_response['message'])
            return
            
        try:
            self._last_m2m_response = r.json()
            return self._last_m2m_response
        except ValueError as e:
            self._logger.error('{:s}: {:s}'.format(e, m2m_url))
            self._last_m2m_response = r.text
            return
        
    def _get_active_deployments(self, ref_des=None, ref_des_search_string=None):
        '''Retrieve the list of actively deployed instruments from the entire UFrame
        asset management schema.  A reference designator may be specified to retrieve
        only active deployment events for that instrument or array.  Resulting
        events may also be filtered by specifying a ref_des_search_string'''
        
        events = []

        if ref_des:
            # Get the list of fully-qualified instrument reference designators for 
            # the specified partial or fully qualified ref_des
            instruments = self.search_instruments(ref_des)
        else:
            instruments = self.instruments
            
        x = 0
        for i in instruments:
            x = x + 1
            new_events = self.query_instrument_deployments(i, status='active', ref_des_search_string=ref_des_search_string)
            if not new_events:
                continue
            events = events + new_events
            
        self._logger.critical('Sent {:0.0f} requests'.format(x))
            
        self._active_deployment_events = events
            
    def __repr__(self):
        if self._base_url:
            return '<M2mClient(url={:s})>'.format(self.m2m_base_url)
        else:
            return '<M2mClient(url=None)>'

