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
import os, sys, requests, time, datetime, subprocess, getpass
import geopandas as gpd
import argparse
import logging
from logging.handlers import TimedRotatingFileHandler


_LOG_LEVEL_STRINGS = ['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG']
formatter = logging.Formatter(fmt='%(asctime)s.%(msecs)03d | %(name)s | %(levelname)s | %(message)s',
                              datefmt='%Y-%m-%dT%H:%M:%S')

DATAPATH = 'data'  # where to save the data
CREDFILE= '.credentials'  # user:password for scihub
VERBOSE = False
LOG_FILE = None
LOG_LEVEL = 'INFO'
log = logging.getLogger('SentinelDL')
log.setLevel(LOG_LEVEL)  # set at default log level

parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description='''Search and Download Sentinel 1 data from dataspace.copernicus.eu''',
    epilog='''Created by Ran Novitsky Nof (ran.nof@gmail.com), 2023 @ GSI''')
parser.add_argument('-v', help='verbose - print messages to screen?', action='store_true', default=VERBOSE)
parser.add_argument('-l', '--log_level', choices=_LOG_LEVEL_STRINGS, default=LOG_LEVEL,
                    help="Log level (Default: {LOG_LEVEL}). see Python's Logging module for more details".format(LOG_LEVEL=LOG_LEVEL))
parser.add_argument('--logfile', metavar='log file name', help='log to file', default=LOG_FILE)
parser.add_argument('-s', '--datapath', metavar='data folder', help=f'Path for saving data, default: {DATAPATH}', default=DATAPATH)
parser.add_argument('-c', '--credfile', metavar='credentials file', help=f'Path for Scihub credentials, default: {CREDFILE}', default=CREDFILE)
parser.add_argument('-t', '--track', metavar='Track Number', help=f'Track number. default: None', type=int, default=None)
parser.add_argument('-d', '--direction', help=f'Direction, default: Both', choices=['ASCENDING', 'DESCENDING'], default=None)
parser.add_argument('-o', '--online', action='store_true', help=f'Only get data available online, not from archive', default=False)
parser.add_argument('--start', metavar='YYYY-MM-DD', help=f'Start date', required=True)
parser.add_argument('--end', metavar='YYYY-MM-DD', help=f'End date', required=True)
parser.add_argument('--geometry', metavar='geojson/shapefile', help=f'Region of interest polygon (WGS84)', required=True)

def get_keycloak(credfile='.credentials') -> str:
    try:
        with open(credfile, 'r') as f:
            username, password = f.readline().strip().split(':')
    except Exception as ex:
        print(f'Error with redential file: {credfile}. Please provide manually.')
        username = getpass.getpass("Enter your username")
        password = getpass.getpass("Enter your password")
    data = {
        "client_id": "cdse-public",
        "username": username,
        "password": password,
        "grant_type": "password",
    }
    try:
        r = requests.post(
        "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
        data=data,
        )
        r.raise_for_status()
    except Exception as e:
        raise Exception(
        f"Keycloak token creation failed. Reponse from the server was: {r.json()}"
        )
    return r.json(), datetime.datetime.utcnow()

def isTTY():
    if os.isatty(sys.stdin.fileno()):
        return True
    else:
        return False

def message(msg, newline=False, x=1000):
    """Print messages to terminal"""
    if not isTTY():
        return
    try: # this will only work on Linux xos and cygwin
        rows, columns = [int(i) for i in subprocess.check_output(['stty', 'size']).split()] # get the size of the terminal
    except:
        rows = x # just use the last line of the terminal
        columns = 80 # and assume default line length
    if not newline: # clear last line and print message. might be buggy when on windows and terminal is smaller than default (80 columns)
        msg = "\x1b7\x1b[%d;%df\033[2K%s\x1b8" % (rows+100, 0, msg[-columns+1:]) # use terminal ascii escape codes
    sys.stderr.write(msg[-columns+1:]) # write to standard error
    sys.stderr.flush() # flush stderr - physically print to terminal.


class SciHubClient(object):
    'A scihub client class'
    def __init__(self, credfile=CREDFILE, datapath=DATAPATH):
        """Create the url opener and site authentication.
        """
        self.credfile = credfile
        self.datapath = datapath
        self.get_token()

    def get_token(self):
        self.token, self.tokentime = get_keycloak(self.credfile)
        self.headers = {"Authorization": f"Bearer {self.token['access_token']}"}
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {self.token['access_token']}"}) # session for the scihub web server

    @property
    def renew(self):
        if datetime.datetime.utcnow() > self.tokentime + datetime.timedelta(seconds=600):
            log.debug('Token refresh')
            self.get_token()
            return True
        else:
            return False

    def search_S1_SLC_data(self, start_date="2023-09-01", end_date="2023-11-01", aoifile='NOVA.geojson', direction=None,
                          track=None, online=False):
        aoi = str(gpd.read_file(aoifile).geometry.values[0])
        if online:
            online = " and Online eq true"
        if direction in ['ASCENDING', 'DESCENDING']:
            direction = f" and Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'orbitDirection' and att/OData.CSC.StringAttribute/Value eq '{direction}')"
        if track is not None:
            track = f" and Attributes/OData.CSC.IntegerAttribute/any(att:att/Name eq 'relativeOrbitNumber' and att/OData.CSC.IntegerAttribute/Value eq {track})"
        log.debug(f'Searching for {direction} data from {start_date} to {end_date} within geometry in {aoifile}' + (f'from track {track}' if track else ''))
        resp = self.session.get(
           f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=(startswith(Name,'S1') and ((Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'instrumentShortName' and att/OData.CSC.StringAttribute/Value eq 'SAR') and (contains(Name,'SLC') and OData.CSC.Intersects(area=geography'SRID=4326;{aoi}'))){online}{direction}{track})) and ContentDate/Start gt {start_date}T00:00:00.000Z and ContentDate/Start lt {end_date}T00:00:00.000Z&$expand=Attributes&$count=True&$expand=Assets&$skip=0"
        ).json()
        log.info(f"Found {resp['@odata.count']} records.")
        return resp

    def download(self, Id):
        """Download data from scihub server"""
        url = f"https://zipper.dataspace.copernicus.eu/odata/v1/Products({Id})/$value"
        try:
            self.renew
            DLf = self.session.get(url, headers=self.headers, stream=True, verify=False, allow_redirects=True, timeout=600) # open url, get the data file
            DLf.raise_for_status()
        except Exception as Ex:
            logging.error(f'Error opening URL {url}\n{Ex}')
            return 0
        if not DLf: return 0 # make sure we have a connection to a file
        DLname = self.datapath + os.sep + DLf.headers.get('Content-Disposition').split("=")[-1].strip().replace('"','')  # get file name
        DLsize = int(DLf.headers.get('Content-Length'))  # get file size
        log.info(f'{DLname}: {DLsize/1048576.:.2f} MB')  # sent name and size to terminal
        if os.path.exists(DLname): # check if same name file exists on current location
            fsize = os.path.getsize(DLname) # if so, what is its size
            DLf.close()
            if fsize==DLsize: # make sure we did't download the file before
                logging.info('Already downloaded. skipping.')
                return 1
            log.info("Starting form {}".format(fsize))
            try:
                self.renew
                self.session.headers.update({"Range": f"bytes={fsize}-"})  # set opener to start from current point
                DLf = self.session.get(url, headers=self.session.headers, stream=True, verify=False, allow_redirects=True, timeout=600) # reopen url, from last point
                DLf.raise_for_status()
            except Exception as Ex:
                logging.error(f'Error opening URL {url}\n{Ex}')
                return 0
        starttime = time.time() # get download start time
        tryouts = 0
        while tryouts < 5 and ((not os.path.exists(DLname)) or (os.path.getsize(DLname) < DLsize)):
            try:
                if tryouts > 0:
                    log.info(f'Retry ({tryouts}/5)...')
                DLt = 0
                for data in DLf.iter_content(chunk_size=1048576):  # read a 1 MB piece of data
                    steptime = time.time()
                    with open(DLname,'ab') as outfile: # open the output file for writing
                        if data:
                            outfile.write(data)
                            fsize = os.path.getsize(DLname)
                            NOW = time.time()
                            DLt = NOW - starttime  # calculate time since starting to download
                            DLstep = NOW - steptime  # calculate time to download segment
                            if DLt and DLstep:
                                DLrate = (len(data) / 1048576.) / DLstep  # calculate current download rate
                                ETA = (DLsize - fsize) / (DLrate * 1048576)  # Estimate Arrival Time in seconds
                                ETA = str(datetime.datetime.fromtimestamp(ETA) - datetime.datetime.fromtimestamp(0))[
                                      :-3]  # reformat ETA for humans.
                            else:
                                DLrate = 0
                                ETA = 'N/A'
                            message('%s| %d%% @ %.2f sec (%.2f MB/sec) ETA: %s' \
                                    % (datetime.datetime.now().strftime("%Y%m%dT%H:%M:%S"),
                                       fsize / float(DLsize) * 100, DLt, DLrate,
                                       ETA))  # print some statistics to terminal
                message('%s| %d%% @ %.2f sec\n' \
                                % (datetime.datetime.now().strftime("%Y%m%dT%H:%M:%S"),
                                fsize / float(DLsize) * 100, DLt))  # print final statistics to terminal
            except Exception as Ex:
                log.error(f'{Ex}')
                tryouts += 1
                time.sleep(30)  # have a short resting time
                self.renew
                fsize = os.path.getsize(DLname)
                self.session.headers.update({"Range": f"bytes={fsize}-"})  # set opener to start from current point
                DLf = self.session.get(url, headers=self.session.headers, stream=True,
                                       verify=False, allow_redirects=True, timeout=600)  # reopen url, from last point
        if os.path.getsize(DLname) == DLsize:
            return 1
        else:
            log.error('Failed to download all the file')
            return 0


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
    client = SciHubClient(credfile=args.credfile, datapath=args.datapath) # create a client
    records = client.search_S1_SLC_data(start_date=args.start, end_date=args.end, aoifile=args.geometry, direction=args.direction, track=args.track, online=args.online)
    for record in records['value']:
        client.download(record['Id'])


"""
example:
sentinelDL.py --start 2023-09-01 --end 2023-11-01 --geometry NOVA.geojson -d ASCENDING -t 87 --online

Test:
args = parser.parse_args("--start 2023-09-01 --end 2023-11-01 --geometry NOVA.geojson -d ASCENDING -t 87 --online".split())
set_logger(log, True, 'DEBUG', args.logfile)
self = client = SciHubClient() # create a client
Id = 'a535c346-0212-42ae-a9ab-0a491f320d3b'
records = client.search_S1_SLC_data(start_date=args.start, end_date=args.end, aoifile=args.geometry, direction=args.direction, track=args.track, online=args.online)
Id = records['value'][0]['Id']

"""
