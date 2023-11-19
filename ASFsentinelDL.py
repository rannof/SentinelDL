#! /usr/bin/env python
# by Ran Novitsky Nof (ran.nof@gmail.com), 2015 @ BSL
# update Nov 9, 2023 R.N.N @ GSI
# ***********************************************************************************
# *    Copyright (C) by Ran Novitsky Nof                                            *
# *                                                                                 *
# *    sentinelDL.py is free software: you can redistribute it and/or modify      *
# *    it under the terms of the GNU Lesser General Public License as published by  *
# *    the Free Software Foundation, either version 3 of the License, or            *
# *    (at your option) any later version.                                          *
# *                                                                                 *
# *    This program is distributed in the hope that it will be useful,              *
# *    but WITHOUT ANY WARRANTY; without even the implied warranty of               *
# *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the                *
# *    GNU Lesser General Public License for more details.                          *
# *                                                                                 *
# *    You should have received a copy of the GNU Lesser General Public License     *
# *    along with this program.  If not, see <http://www.gnu.org/licenses/>.        *
# ***********************************************************************************
import os
import sys
import getpass
import geopandas as gpd
import asf_search as asf
import argparse
import logging
from logging.handlers import TimedRotatingFileHandler

import pandas as pd

_LOG_LEVEL_STRINGS = ['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG']
formatter = logging.Formatter(fmt='%(asctime)s.%(msecs)03d | %(name)s | %(levelname)s | %(message)s',
                              datefmt='%Y-%m-%dT%H:%M:%S')

DATAPATH = 'data'  # where to save the data
CREDFILE= '.credentials'  # user:password for ASF (NASA Earthdata Alaska Facility)
VERBOSE = False
LOG_FILE = None
LOG_LEVEL = 'INFO'
log = logging.getLogger('SentinelDL')
log.setLevel(LOG_LEVEL)  # set at default log level

parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description='''Search and Download Sentinel 1 data from ASF''',
    epilog='''Created by Ran Novitsky Nof (ran.nof@gmail.com), 2023 @ GSI''')
parser.add_argument('-v', help='verbose - print messages to screen?', action='store_true', default=VERBOSE)
parser.add_argument('-l', '--log_level', choices=_LOG_LEVEL_STRINGS, default=LOG_LEVEL,
                    help="Log level (Default: {LOG_LEVEL}). see Python's Logging module for more details".format(LOG_LEVEL=LOG_LEVEL))
parser.add_argument('--logfile', metavar='log file name', help='log to file', default=LOG_FILE)
parser.add_argument('-s', '--datapath', metavar='data folder', help=f'Path for saving data, default: {DATAPATH}', default=DATAPATH)
parser.add_argument('-c', '--credfile', metavar='credentials file', help=f'Path for Scihub credentials, default: {CREDFILE}', default=CREDFILE)
parser.add_argument('-t', '--track', metavar='Track Number', help=f'Track number. default: None', type=int, default=None)
parser.add_argument('-d', '--direction', help=f'Direction, default: Both', choices=['ASCENDING', 'DESCENDING'], default=None)
parser.add_argument('--start', metavar='YYYY-MM-DD', help=f'Start date', required=True)
parser.add_argument('--end', metavar='YYYY-MM-DD', help=f'End date', required=True)
parser.add_argument('--geometry', metavar='geojson/shapefile', help=f'Region of interest polygon (WGS84)', required=True)

def get_auth(credfile='.credentials') -> dict:
    try:
        with open(credfile, 'r') as f:
            username, password = f.readline().strip().split(':')
    except Exception as ex:
        print(f'Error with redential file: {credfile}. Please provide manually.')
        username = getpass.getpass("Enter your username")
        password = getpass.getpass("Enter your password")
    creds = {
        "username": username,
        "password": password,
    }
    return creds


class ASFClient(object):
    'A scihub client class'
    def __init__(self, credfile=CREDFILE, datapath=DATAPATH):
        """Create the url opener and site authentication.
        """
        self.credfile = credfile
        self.datapath = datapath
        if not os.path.exists(self.datapath):
            log.debug(f'Creating {self.datapath}')
            os.makedirs(self.datapath, exist_ok=True)
        log.debug(f'Saving file at {self.datapath}')
        try:
            creds = get_auth(self.credfile)
            self.session = asf.ASFSession().auth_with_creds(creds['username'], creds['password'])
        except asf.ASFAuthenticationError as e:
            log.error(f'Auth failed: {e}')
            sys.exit('Auth Failed')

    def search_S1_SLC_data(self, start_date="2011-01-01", end_date="2024-01-01", aoifile=None, direction=None,
                          track=None):
        log.debug(f'Searching for {direction} data from {start_date} to {end_date} within geometry in {aoifile}' + (
            f' from track {track}' if track else ''))
        params = {}
        # data type
        params['platform'] = asf.PLATFORM.SENTINEL1
        params['processingLevel'] = asf.PRODUCT_TYPE.SLC
        # time window
        params['start'] = pd.to_datetime(start_date).isoformat() + 'Z'
        params['end'] = pd.to_datetime(end_date).isoformat() + 'Z'
        # geometry
        if aoifile is not None:
            aoi = str(gpd.read_file(aoifile).geometry.values[0])
            params['intersectsWith'] = aoi
        # direction
        if direction in ['ASCENDING', 'DESCENDING']:
            params['flightDirection'] = direction
        if track is not None:
            params['relativeOrbit'] = track
        results = asf.search(**params)
        try:
            results.raise_if_incomplete()
        except Exception as EX:
            log.error(f'Incomplete search: {EX}')
            return None
        log.info(f"Found {len(results)} records.")
        return results

    def download_all(self, results):
        """Download search results data from ASF server"""
        results.download(
            path=self.datapath,
            session=self.session,
            processes=50)


def set_logger(log, verbose=VERBOSE, log_level=LOG_LEVEL, logfile=LOG_FILE):
    log.setLevel(log_level)
    if verbose:
        # create console handler
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(log_level)
        ch.setFormatter(formatter)
        if logging.StreamHandler not in [h.__class__ for h in log.handlers]:
            log.addHandler(ch)
        else:
            log.warning('log Stream handler already applied.')
    if logfile:
        # create file handler
        fh = TimedRotatingFileHandler(logfile,
                                      when='midnight',
                                      utc=True)
        fh.setLevel(log_level)
        fh.setFormatter(formatter)
        if TimedRotatingFileHandler not in [h.__class__ for h in log.handlers]:
            log.addHandler(fh)
        else:
            log.warning('Log file handler already applied.')
        log.info(f'Log file is: {logfile}')
    else:
        log.debug(f'No Log file was set')

if __name__=='__main__':
    args = parser.parse_args()
    set_logger(log, args.v, args.log_level, args.logfile)
    client = ASFClient(credfile=args.credfile, datapath=args.datapath)  # create a client
    results = client.search_S1_SLC_data(start_date=args.start, end_date=args.end, aoifile=args.geometry,
                                        direction=args.direction, track=args.track)
    client.download_all(results)

"""
example:
sentinelDL.py --start 2023-09-01 --end 2023-11-01 --geometry NOVA.geojson -d ASCENDING -t 87 --online

Test:
args = parser.parse_args("--start 2023-09-01 --end 2023-11-01 --geometry NOVA.geojson -d ASCENDING -t 87".split())
set_logger(log, True, 'DEBUG', args.logfile)
self = client = ASFClient() # create a client
records = client.search_S1_SLC_data(start_date=args.start, end_date=args.end, aoifile=args.geometry, direction=args.direction, track=args.track)

"""
