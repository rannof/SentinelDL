#! /usr/bin/env python
# by Ran Novitsky Nof (ran.nof@gmail.com), 2015 @ BSL

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
import socket
socket.setdefaulttimeout(600)
import os,sys,urllib2,time,re,datetime,subprocess
from xml.dom import minidom

BASE_URL = 'https://scihub.copernicus.eu/dhus/' # base URL of the Sentinel scihub

def usage():
  sys.exit('''SentinelDL - A Python download client for Sentinel Data via scihub.esa.int
  USAGE: SentinelDL.py [-h|-H|--help] URI
    URI - a scihub URL for searching products
          or
          a scihub URL for downloading product
          or
          a file containing metalinks of products.

  INSTRUCTIONS:
    1. Go to https://scihub.esa.int/ and register.
    2. create a file .credentials with a single line containing
       [USER]:[PASSWORD].
       where USER and PASSWORD are the scihub credentials.
    3. Enter the hub at https://scihub.esa.int/dhus/ and log in.
    4. Search for data.
    5. Add data to your cart.
    6. Save cart to a file (by default products.meta4).
    7. Run the code with the meta4 file name as a parameter.
    8. Instead of steps 5-7, provide product link as a parameter.

    More data and also how to build a search URL are available at:
      https://scihub.esa.int/userguide
      and
      https://scihub.esa.int/userguide/BatchScripting

By Ran Novitsky Nof (ran.nof@gmail.com) @ BSL, 2015
  ''')

class SciHubClient(object):
  'A scihub client class'
  def __init__(self,credfile='.credentials',BASE_URL = BASE_URL):
    'Create the url opener and site authentication.'
    self.BASE_URL = BASE_URL # scihub base url
    try:
      USERNAME,PASSWORD = open(credfile,'r').readlines()[0].strip().split(':') # get the user credentials form a file
    except Exception as Ex:
      sys.exit("Can't read credential file: %s\n file should contain one line with [username]:[password]\n\n%s"%(credfile,str(Ex)))
    self.passman = urllib2.HTTPPasswordMgrWithDefaultRealm() # password manager
    self.passman.add_password(None, BASE_URL, USERNAME, PASSWORD) # add the credentials
    self.authhandler = urllib2.HTTPBasicAuthHandler(self.passman) # handler of authentication
    self.proxyhandler = urllib2.ProxyHandler({}) # no proxy handler
    self.opener = urllib2.build_opener(self.authhandler,self.proxyhandler) # opener for the scihub web server
    self.headers = self.opener.addheaders[:] # keep original headers
    

  def message(self,msg,newline=False,x=1000):
    'Print messages to terminal'
    try: # this will only work on Linux xos and cygwin
      rows, columns = [int(i) for i in subprocess.check_output(['stty', 'size']).split()] # get the size of the terminal
    except:
      rows = x # just use the last line of the terminal
      columns = 80 # and assume default line length
    if not newline: # clear last line and print message. might be buggy when on windows and terminal is smaller than default (80 columns)
      msg = "\x1b7\x1b[%d;%df\033[2K%s\x1b8" % (rows+100, 0,msg[-columns+1:]) # use terminal ascii escape codes
    sys.stderr.write(msg[-columns+1:]) # write to standard error
    sys.stderr.flush() # flush stderr - physically print to terminal.

  def procURLs(self,urls):
    'Process url to check if its a search or a download url'
    if type(urls)==str: urls = [urls] # make sure we use a list of urls
    for url in urls:
      if re.match(self.BASE_URL+"odata/v1/Products\('.+'\)/\$value", url): # its a download pattern
        self.download(url) # download data
      if re.match(self.BASE_URL+"odata/v1/Products\?",url): # its a search pattern
        retval = self.search(url)  # get json xml returned from server for search
        self.parseXML(retval) # parse the json xml

  def procMetalinkXMLs(self,xmls,download=True):
    'Process a metalink4 xml to extract data urls'
    urls = []
    for xml in xmls:
      [urls.append(el.childNodes[0].data) for el in xml.getElementsByTagName('url')] # extract urls in the metalink
    if download: self.procURLs(urls[::-1]) # send urls for processing
    return urls

  def downloadFromMetalink4(self,files,download=True):
    'Process metalink4 files'
    if type(files)==str: files = [files] # make sure we use a list of files
    metalinks = [minidom.parse(f) for f in files] # parse an xml object from each file
    return self.procMetalinkXMLs(metalinks,download) # process the metalink xmls

  def search(self,url,download=True):
    'Get a search json xml response from server'
    self.message('Searching...')
    try:
      XMLf = self.opener.open(url,timeout=300) # open url - do the search, might take time
    except Exception as Ex:
      print >> sys.stderr,'Error opening URL %s\n%s'%(url,str(Ex))
      return
    if not XMLf: return # make sure we got a response
    XML = minidom.parse(XMLf) # convert text to xml object
    XMLf.close() # close connection
    metalinks = [minidom.parseString(el.childNodes[0].data) for el in XML.getElementsByTagName('d:Metalink')] # extract only the metalinks
    return self.procMetalinkXMLs(metalinks, download) # process the metalinks xmls

  def download(self,url):
    'Download data from scihub server'
    if not re.match(self.BASE_URL+"odata/v1/Products\('.+'\)/\$value", url): # make sure its a download pattern
      self.message("Can't Download %s. Not a valid URL.\n", True)
      return
    try:
      #self.opener.addheaders = self.headers[:] # reset opener headers
      DLf = self.opener.open(url,timeout=600) # open url, get the data file
    except Exception as Ex:
      print >> sys.stderr,'Error opening URL %s\n%s'%(url,str(Ex))
      return 0
    if not DLf: return 0 # make sure we have a connection to a file
    DLname = DLf.info()['Content-Disposition'].split("=")[-1].strip().replace('"','') # get file name
    DLsize = int(DLf.info()['Content-Length']) # get file size
    self.message('%s: %.2f MB\n'%(DLname,DLsize/131072.),True) # sent name and size to terminal
    if os.path.exists(DLname): # check if same name file exists on current location
      fsize = os.path.getsize(DLname) # if so, what is its size
      DLf.close()
      if fsize==DLsize: # make sure we did't download the file before
        self.message('\n\talready downloaded. skipping.\n',True)
        return 1
      self.message("\n\tStarting form %d\n"%fsize,True)
      self.opener.addheaders.append(("Range","bytes=%s-" % (fsize))) # set opener to start from current point
      DLf = self.opener.open(url,timeout=600) # reopen url, from last point
      self.opener.addheaders = self.headers[:] # reset opener headers 
    starttime = time.time() # get download start time
    tryouts = 0
    with open(DLname,'ab') as outfile: # open the output file for writing
      while True: # just keep going, inside checks will break the loop as needed
        steptime = time.time()  # download interval start
        try:
          data = DLf.read(131072) # read a 1 MB piece of data
        except Exception as Ex:
          self.message('\n%s\n'%str(Ex),True)
          if tryouts>5:
            DLf.close() # close connection to server
            self.message('\nOops.\n',True)
            return 0
          else:
            data = 0
        if not data:
          if os.path.getsize(DLname)==DLsize: 
            self.message('\n%s: Done.\n'%DLname,True)
            break # we got to the end of the file so break the loop
          else:
            tryouts = tryouts+1
            self.message('\nRetry (%d/5)...\n'%tryouts,True)
            fsize = os.path.getsize(DLname) # get current point of saved data
            DLf.close() # colse old handler
            time.sleep(30) # have a short resting time
            self.opener.addheaders.append(("Range","bytes=%s-" % (fsize))) # set opener to start from current point
            DLf = self.opener.open(url,timeout=600) # reopen url, from last point
            self.opener.addheaders = self.headers[:] # reset opener headers
            continue # keep trying
        outfile.write(data) # write the data to the output file
        NOW = time.time()
        DLt = NOW-starttime # calculate time since starting to download
        if DLt:
          DLrate = (len(data)/131072.)/(NOW-steptime) # calculate current download rate
        else:
          DLrate = 0
        ETA = (DLsize-outfile.tell())/(DLrate*131072) # Estimate Arrival Time in seconds
        ETA = str(datetime.datetime.fromtimestamp(ETA)-datetime.datetime.fromtimestamp(0))[:-3] # reformat ETA for humans.
        self.message('%s| %d%% @ %.2f sec (%.2f MB/sec) ETA: %s'\
                     %(datetime.datetime.now().strftime("%Y%m%dT%H:%M:%S"),outfile.tell()/float(DLsize)*100,DLt,DLrate,ETA)) # print some statistics to terminal
      self.message('%s| %d%% @ %.2f sec\n'\
                   %(datetime.datetime.now().strftime("%Y%m%dT%H:%M:%S"),outfile.tell()/float(DLsize)*100,DLt)) # print final statistics to terminal
    DLf.close() # close connection to server
    return 1

if __name__=='__main__':
  if not len(sys.argv)==2 or\
         sys.argv[1] in ["-h","-H","--help"] : usage() # print usage statement if needed
  arg = sys.argv[1] # get the argument
  client = SciHubClient() # create a client
  if 'https' in arg: # if its a url
    client.procURLs(arg) # process url
  elif os.path.exists(arg): # if its a valid metalink4 file
    client.downloadFromMetalink4(arg) # parse file and download data
  else:
    print >> sys.stderr,"Sorry, can't find file or link %s"%arg
