##################################################
#         crawler.py - SelfSearchCrawler         #
# https://github.com/rainier39/SelfSearchCrawler #
##################################################

# Standard library imports.
import os
import sys
import requests
# Third-party imports.
import mariadb # pip3 install mariadb
from bs4 import BeautifulSoup # pip3 install beautifulsoup4

# Constants.
VERSION = "2.0"
# Links starting with any of these are skipped. Source: https://www.iana.org/assignments/uri-schemes/uri-schemes.xhtml
SCHEMEBLACKLIST = ['aaa:', 'aaas:', 'about:', 'acap:', 'acct:', 'acd:', 'acr:', 'adiumxtra:', 'adt:', 'afp:', 'afs:', 'aim:', 'amss:', 'android:', 'appdata:', 'apt:', 'ar:', 'ari:', 'ark:', 'at:', 'attachment:', 'aw:', 'barion:', 'bb:', 'beshare:', 'bitcoin:', 'bitcoincash:', 'bl:', 'blob:', 'bluetooth:', 'bolo:', 'brid:', 'browserext:', 'cabal:', 'calculator:', 'callto:', 'cap:', 'caip:', 'cast:', 'casts:', 'chrome:', 'chrome-extension:', 'cid:', 'coap:', 'coap+tcp:', 'coap+ws:', 'coaps:', 'coaps+tcp:', 'coaps+ws:', 'com-eventbrite-attendee:', 'content:', 'content-type:', 'crid:', 'cstr:', 'cvs:', 'dab:', 'dat:', 'data:', 'dav:', 'dhttp:', 'diaspora:', 'dict:', 'did:', 'dis:', 'dlna-playcontainer:', 'dlna-playsingle:', 'dnp:', 'dns:', 'dntp:', 'doi:', 'donau:', 'dpp:', 'drm:', 'drop:', 'dtmi:', 'dtn:', 'dvb:', 'dvx:', 'dweb:', 'ed2k:', 'eid:', 'elsi:', 'embedded:', 'ens:', 'esim:', 'ethereum:', 'example:', 'facetime:', 'fax:', 'feed:', 'feedready:', 'fido:', 'file:', 'filesystem:', 'finger:', 'first-run-pen-experience:', 'fish:', 'fm:', 'ftp:', 'fuchsia-pkg:', 'geo:', 'gg:', 'git:', 'gitoid:', 'gizmoproject:', 'go:', 'gopher:', 'graph:', 'grd:', 'gtalk:', 'h323:', 'ham:', 'hcap:', 'hcp:', 'hs20:', 'hxxp:', 'hxxps:', 'hydrazone:', 'hyper:', 'i0:', 'iax:', 'ibi:', 'ibi-:', 'ilstring:', 'icap:', 'icon:', 'ilstring:', 'im:', 'imap:', 'info:', 'iotdisco:', 'ipfs:', 'ipn:', 'ipns:', 'ipp:', 'ipps:', 'irc:', 'irc6:', 'ircs:', 'iris:', 'iris.beep:', 'iris.lwz:', 'iris.xpc:', 'iris.xpcs:', 'isostore:', 'itms:', 'jabber:', 'jar:', 'jms:', 'keyparc:', 'lastfm:', 'lbry:', 'ldap:', 'ldaps:', 'leaptofrogans:', 'lid:', 'linkid:', 'lorawan:', 'lpa:', 'lvlt:', 'machineProvisioningProgressReporter:', 'magnet:', 'mailserver:', 'mailto:', 'maps:', 'market:', 'matrix:', 'message:', 'microsoft.windows.camera:', 'microsoft.windows.camera.multipicker:', 'microsoft.windows.camera.picker:', 'mid:', 'mms:', 'modem:', 'mongodb:', 'moz:', 'mqtt:', 'mqtts:', 'ms-access:', 'ms-appinstaller:', 'ms-browser-extension:', 'ms-calculator:', 'ms-drive-to:', 'ms-enrollment:', 'ms-excel:', 'ms-eyecontrolspeech:', 'ms-gamebarservices:', 'ms-gamingoverlay:', 'ms-getoffice:', 'ms-help:', 'ms-infopath:', 'ms-inputapp:', 'ms-launchremotedesktop:', 'ms-lockscreencomponent-config:', 'ms-media-stream-id:', 'ms-meetnow:', 'ms-mixedrealitycapture:', 'ms-mobileplans:', 'ms-newsandinterests:', 'ms-officeapp:', 'ms-people:', 'ms-personacard:', 'ms-powerpoint:', 'ms-project:', 'ms-publisher:', 'ms-recall:', 'ms-remotedesktop:', 'ms-remotedesktop-launch:', 'ms-restoretabcompanion:', 'ms-screenclip:', 'ms-screensketch:', 'ms-search:', 'ms-search-repair:', 'ms-secondary-screen-controller:', 'ms-secondary-screen-setup:', 'ms-settings:', 'ms-settings-airplanemode:', 'ms-settings-bluetooth:', 'ms-settings-camera:', 'ms-settings-cellular:', 'ms-settings-cloudstorage:', 'ms-settings-connectabledevices:', 'ms-settings-displays-topology:', 'ms-settings-emailandaccounts:', 'ms-settings-language:', 'ms-settings-location:', 'ms-settings-lock:', 'ms-settings-nfctransactions:', 'ms-settings-notifications:', 'ms-settings-power:', 'ms-settings-privacy:', 'ms-settings-proximity:', 'ms-settings-screenrotation:', 'ms-settings-wifi:', 'ms-settings-workplace:', 'ms-spd:', 'ms-stickers:', 'ms-sttoverlay:', 'ms-transit-to:', 'ms-useractivityset:', 'ms-uup:', 'ms-virtualtouchpad:', 'ms-visio:', 'ms-walk-to:', 'ms-whiteboard:', 'ms-whiteboard-cmd:', 'ms-widgetboard:', 'ms-widgets:', 'ms-word:', 'msnim:', 'msrp:', 'msrps:', 'mss:', 'mt:', 'mtqp:', 'mtrust:', 'mumble:', 'mupdate:', 'mvn:', 'mvrp:', 'mvrps:', 'news:', 'nfs:', 'ni:', 'nih:', 'nntp:', 'notes:', 'num:', 'ocf:', 'oid:', 'onenote:', 'onenote-cmd:', 'opaquelocktoken:', 'openid:', 'openpgp4fpr:', 'otpauth:', 'p1:', 'pack:', 'palm:', 'paparazzi:', 'payment:', 'payto:', 'pkcs11:', 'platform:', 'pop:', 'pres:', 'prospero:', 'proxy:', 'psyc:', 'pttp:', 'pwid:', 'qb:', 'query:', 'quic-transport:', 'redis:', 'rediss:', 'reload:', 'res:', 'resource:', 'rmi:', 'rsync:', 'rtmfp:', 'rtmp:', 'rtsp:', 'rtsps:', 'rtspu:', 'sarif:', 'secondlife:', 'secret-token:', 'service:', 'session:', 'sftp:', 'sgn:', 'shc:', 'shelter:', 'shttp (OBSOLETE):', 'sieve:', 'simpleledger:', 'simplex:', 'sip:', 'sips:', 'skype:', 'smb:', 'smp:', 'sms:', 'smtp:', 'snews:', 'snmp:', 'soap.beep:', 'soap.beeps:', 'soldat:', 'spacify:', 'spiffe:', 'spotify:', 'ssb:', 'ssh:', 'starknet:', 'steam:', 'stun:', 'stuns:', 'submit:', 'svn:', 'swh:', 'swid:', 'swidpath:', 'tag:', 'taler:', 'teamspeak:', 'teapot:', 'teapots:', 'tel:', 'teliaeid:', 'telnet:', 'tftp:', 'things:', 'thismessage:', 'thzp:', 'tip:', 'tn3270:', 'tool:', 'turn:', 'turns:', 'tv:', 'udp:', 'unreal:', 'upn:', 'upt:', 'urn:', 'ut2004:', 'uuid-in-package:', 'v-event:', 'vemmi:', 'ventrilo:', 'ves:', 'videotex:', 'view-source:', 'vnc:', 'vscode:', 'vscode-insiders:', 'vsls:', 'w3:', 'wais:', 'wasm:', 'wasm-js:', 'wcr:', 'web+ap:', 'web3:', 'webcal:', 'wifi:', 'wpid:', 'ws:', 'wss:', 'wtai:', 'wyciwyg:', 'xcompute:', 'xcon:', 'xcon-userid:', 'xfire:', 'xftp:', 'xmlrpc.beep:', 'xmlrpc.beeps:', 'xmpp:', 'xrcp:', 'xri:', 'ymsgr:', 'z39.50:', 'z39.50r:', 'z39.50s:', '#']

def regenUserAgent():
  cfg["useragent"] = cfg["botname"] + "/" + cfg["botversion"] + " (+" + cfg["boturl"] + ")"
      
def cleanup():
  # Close the database connection.
  db.close()
  conn.close()

# Check whether a string is a valid IP address.
def isIP(ip):
  valid = True
  # IPv4
  if (ip.count(".") == 3):
    temp = ip.split(".")
    if (len(temp) == 4):
      for t in temp:
        if not t.isdigit():
          valid = False
        else:
          i = int(t)
          if (i < 0) or (i > 255):
            valid = False
  # IPv6 TODO
  
  return valid
      

# Auxiliary function for parsing robots.txt rules.
def parseRobotsRules(rules, target):
  longest = -1
  # Default behavior: assume we are allowed.
  allowed = True
  
  # Remove trailing slashes.
  target = target.rstrip("/")
  target = target.split("/")
  
  for rule in rules:
    if rule.lower().startswith("allow:"):
      rule = rule[6:].strip()
      # Remove trailing dollar signs (end of match pattern character)
      rule = rule.rstrip("$")
      # Remove trailing wildcards.
      while rule.endswith("/*"):
        rule = rule[:-2]
      # Special case: allow nothing (ignored).
      if (rule == ""):
        continue
      # Special case: allow /.
      if (rule == "/"):
        if longest <= 1:
          longest = 1
          allowed = True
        continue
      # Remove trailing slashes.
      rule = rule.rstrip("/")
      rule = rule.split("/")
      
      if (len(target) < len(rule)):
        break
      else:
        end = len(rule)
      
      match = True
      
      for r in range(0, end):
        wildcardmatch = False
        
        if "*" in rule[r]:
          if (rule[r] == "*"):
            wildcardmatch = True
          elif (rule[r].startswith("*")):
            if (target[r].endswith(rule[r][1:])):
              wildcardmatch = True
          elif (rule[r].endswith("*")):
            if (target[r].startswith(rule[r][:rule[r][-1]])):
              wildcardmatch = True
        
        if (rule[r] != target[r]) and not wildcardmatch:
          match = False
          break
      
      # If this is a match.
      if match and (end >= longest):
        allowed = True
        longest = end
    elif rule.lower().startswith("disallow:"):
      rule = rule[9:].strip()
      # Remove trailing dollar signs (end of match pattern character)
      rule = rule.rstrip("$")
      # Remove trailing wildcards.
      while rule.endswith("/*"):
        rule = rule[:-2]
      # Special case: disallow nothing.
      if (rule == ""):
        if longest < 0:
          longest = 0
          allowed = True
        continue
      # Special case: disallow /.
      if (rule == "/"):
        if longest < 1:
          longest = 1
          allowed = False
        continue
      # Remove trailing slashes.
      rule = rule.rstrip("/")
      rule = rule.split("/")
      
      if (len(target) < len(rule)):
        break
      else:
        end = len(rule)
      
      match = True
      
      for r in range(0, end):
        wildcardmatch = False
        
        if "*" in rule[r]:
          if (rule[r] == "*"):
            wildcardmatch = True
          elif (rule[r].startswith("*")):
            if (target[r].endswith(rule[r][1:])):
              wildcardmatch = True
          elif (rule[r].endswith("*")):
            if (target[r].startswith(rule[r][:rule[r][-1]])):
              wildcardmatch = True
        
        if (rule[r] != target[r]) and not wildcardmatch:
          match = False
          break
      
      # If this is a match.
      if match and (end > longest):
        allowed = False
        longest = end
    #elif rule.lower().startswith("crawl-delay:"):
    # Ignore everything else.

  return allowed

def parseRobotsFile(r, u):
  # Remove the protocol part of the URL.
  if u.startswith("https://"):
    tempurl = u[8:]
  elif u.startswith("http://"):
    tempurl = u[7:]
  tempdirs = None
  # Get the URL's path.
  if "/" in tempurl:
    tempurl = tempurl[tempurl.find("/"):]
  else:
    tempurl = "/"

  robo = r.split("\n")
  
  usrules = []
  starrules = []

  us = False
  star = False
  for line in robo:
    # Skip full line comments.
    if line.startswith("#"):
      continue
    if " #" in line:
      line = line[:line.find(" #")]
    line = line.strip()
    # Skip blank lines.
    if line == "":
      continue
    if line.lower().startswith("user-agent:"):
      # This is case-insensitive.
      if (cfg["botname"].lower() == line.lower()[11:].strip()):
        us = True
        star = False
      elif (line.lower()[11:].strip() == "*"):
        star = True
        us = False
      else:
        us = False
        star = False
    elif us:
      usrules.append(line)
    elif star:
      starrules.append(line)
    # We ignore everything else.
  
  # If there are rules set for this specific crawler, abide by them.
  if (len(usrules) > 0):
    return parseRobotsRules(usrules, tempurl)
  # Otherwise abide by the generic rules.
  else:
    return parseRobotsRules(starrules, tempurl)
  
  # Default behavior: assume we are allowed.
  return True

# Default configuration.
cfg = {"botname": "SelfSearchbot",
"boturl": "https://github.com/rainier39/SelfSearchCrawler",
"botversion": VERSION,
"regenuseragent": "no",
"flushinterval": 10,
"debug": "no",
"mhost": None,
"muser": None,
"mpassword": None,
"mdatabase": None,
"mport": None}

regenUserAgent()

# Load the configuration file.
if not os.path.isfile("config.cfg"):
  print("Warning: config file not found.")
else:
  with open("config.cfg", "rt") as f:
    for line in f:
      # Ignoring comments.
      if not line.startswith("#"):
        for c in cfg:
          if line.startswith(c + "="):
            # Overwrite the default config with any new values.
            cfg[line[:line.find("=")]] = line.strip()[line.find("=")+1:]

if (cfg["regenuseragent"] == "yes"):
  regenUserAgent()

# Connect to the MariaDB database.
dbconfig = {
  'host': cfg["mhost"],
  'port': int(cfg["mport"]),
  'user': cfg["muser"],
  'password': cfg["mpassword"],
  'database': cfg["mdatabase"]
}
conn = mariadb.connect(**dbconfig)
db = conn.cursor()

print("Creating DB table if it doesn't exist already...")
try:
  db.execute("""CREATE TABLE IF NOT EXISTS `pages` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `url` varchar(2048) NOT NULL,
  `timescraped` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `content` mediumtext NOT NULL,
  `title` varchar(60) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `url` (`url`)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;""")
  db.execute("""CREATE TABLE IF NOT EXISTS `robots` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `url` varchar(2048) NOT NULL,
  `timescraped` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `content` mediumtext NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `url` (`url`)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;""")
  db.execute("""CREATE TABLE IF NOT EXISTS `tocrawl` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `url` varchar(2048) NOT NULL,
  `timeadded` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `url` (`url`)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;""")
  conn.commit()
  print("Done!")
except mariadb.Error as e:
  print("Database Error: " + str(e))
  conn.rollback()

headers = {"User-Agent": cfg["useragent"]}

tlds = []

# Get the list of TLDs.
if not os.path.isfile("tlds-alpha-by-domain.txt"):
  # TODO: attempt to download the TLD list.
  print("Error: TLD list file not found.")
  exit()
with open("tlds-alpha-by-domain.txt", "rt") as f:
  for line in f:
    # Ignoring comments.
    if not line.startswith("#"):
      tlds.append(line.strip().lower())

tocrawl = []

# Get the list of URLs we need to crawl.
if not os.path.isfile("seed.txt"):
  print("Warning: seed file not found.")
else:
  with open("seed.txt", "rt") as f:
    for line in f:
      # Ignoring comments.
      if not line.startswith("#"):
        tocrawl.append(line.strip())

if (len(tocrawl) < 1):
  print("Failed to get any sites to crawl from seed file, trying database...")
  selectquery = "SELECT url FROM tocrawl ORDER BY timeadded LIMIT 1"
  db.execute(selectquery)
  if (db.rowcount > 0):
    tocrawl.append(db.fetchone()[0])
  else:
    print("Failed to get any sites to crawl from the database.")
    exit()

blacklist = []

# Get the blacklist (sites we never crawl).
if not os.path.isfile("blacklist.txt"):
  print("Warning: blacklist file not found.")
else:
  with open("blacklist.txt", "rt") as f:
    for line in f:
      # Ignoring comments.
      if not line.startswith("#"):
        temp = line.strip().replace("https://", "").replace("http://", "")
        if "/" in temp:
          temp = temp[:temp.find("/")]
        blacklist.append(temp)

counter = 0

# We need this because there are a few things to do when discarding a URL.
def customContinue():
  global tocrawl
  # Remove this URL from the list.
  tocrawl.pop(0)
  # Remove it from the database too.
  deletequery = "DELETE FROM tocrawl WHERE url='?'"
  db.execute(deletequery, (originalurl))
  
  # Get a new URL from the database if needed.
  if (len(tocrawl) < 1):
    selectquery = "SELECT url FROM tocrawl ORDER BY timeadded LIMIT 1"
    db.execute(selectquery)
    if (db.rowcount > 0):
      tocrawl.append(db.fetchone()[0])

# Main loop.
print("Starting crawl...")
while (len(tocrawl) > 0):
  originalurl = tocrawl[0]
  url = originalurl
  
  protocol = "https://"
  
  # If the protocol is not specified, assume HTTPS.
  if not (url.startswith("http://") or (url.startswith("https://"))):
    url = protocol + url
  else:
    # Use HTTP if that's what the URL specifies.
    if url.startswith("http://"):
      protocol = "http://"
  
  # Get the domain name/TLD/subdomain part.
  temp = url.replace("https://", "").replace("http://", "")
  if "/" in temp:
    temp = temp[:temp.find("/")]
  if "#" in temp:
    temp = temp[:temp.find("#")]
  # Get the TLD.
  matches = []
  for t in tlds:
    if temp.endswith(t):
      matches.append(t)
  if (len(matches) == 0):
    # Check if this is a raw IP address rather than a domain name.
    if isIP(temp):
      matches.append("")
      if (cfg["debug"] == "yes"):
        print("Raw IP address detected: " + temp + ".")
    else:
      if (cfg["debug"] == "yes"):
        print("Error: invalid TLD. (" + url + ")")
      customContinue()
  # The longest matching TLD will be the correct one.
  tld = max(matches, key=len)
  temp = temp[:temp.rfind(tld)-1]
  domain = temp[temp.rfind(".")+1:]
  subdomain = temp.replace(domain, "").strip(".")
  if (tld != ""):
    full = temp + "." + tld
  else:
    full = temp
  
  # Don't crawl blacklisted pages.
  if full in blacklist:
    if (cfg["debug"] == "yes"):
      print("Skipping blacklisted page.")
    customContinue()
  
  # Don't crawl pages we've already crawled.
  selectquery = "SELECT url FROM pages WHERE url=?"
  db.execute(selectquery, (url,))
  if (db.rowcount > 0):
    if (cfg["debug"] == "yes"):
      print("Skipping duplicate page.")
    customContinue()
  
  firstvisit = True
  
  selectquery = "SELECT url FROM pages WHERE (url LIKE ? OR url LIKE ?)"
  db.execute(selectquery, ("http://" + full + "%", "https://" + full + "%"))
  
  if (db.rowcount > 0):
    firstvisit = False
  
  s = requests.Session()
  # On the first visit we need to get the robots.txt file so we know what we can and cannot crawl.
  if firstvisit:
    try:
      robots = s.get(protocol + full + "/robots.txt", headers=headers)
      robot = robots.text
    except:
      # Allow by default.
      robot = "User-agent: *\nAllow: /"
      if (cfg["debug"] == "yes"):
        print("Failed to get robots.txt for site " + full + ".")
  
    if (len(robot) > 0):
      insertquery = "REPLACE INTO robots (url, content) VALUES (?, ?)"
      try:
        db.execute(insertquery, (full[:2048], robot[:16777215]))
        conn.commit()
      except mariadb.Error as e:
        if (cfg["debug"] == "yes"):
          print("Database Error: " + str(e))
        conn.rollback()
  # Otherwise, check the existing robots.txt file and see if we're allowed to crawl this URL.
  else:
    selectquery = "SELECT content FROM robots WHERE url=?"
    db.execute(selectquery, (full,))
    if (db.rowcount > 0):
      robot = db.fetchone()[0]
  
  # If we are allowed to crawl, do it.
  parsed = parseRobotsFile(robot, url)
  if parsed:
    page = s.get(url, headers=headers)
    stop = False
    
    # Stop if the content we received isn't UTF-8 (not a text file).
    try:
      page.content.decode("utf-8")
    except:
      stop = True
      if (cfg["debug"] == "yes"):
        print("Found non utf-8 content on page: " + url)
    
    soup = BeautifulSoup(page.text, "lxml")
    content = soup
    
    # Get the first title we find.
    title = content.find("title").text
    
    if ((title == "") or (title == None)):
      title = "Untitled Webpage"
    
    for bad in content(["script", "style"]):
      content.extract()
      
    content = content.get_text()
    
    if ((not stop) and (len(content) > 0)):
      # Add the page to the database.
      insertquery = "REPLACE INTO pages (url, content, title) VALUES (?, ?, ?)"
      try:
        db.execute(insertquery, (url[:2048], content[:16777215], title[:60]))
        conn.commit()
      except mariadb.Error as e:
        if (cfg["debug"] == "yes"):
          print("Database Error: " + str(e))
        conn.rollback()
      # Get all of the links from the page.
      links = soup.find_all("a")
      for link in links:
        if not link.get("href"):
          continue
        blacklisted = False
        for scheme in SCHEMEBLACKLIST:
          if link["href"].startswith(scheme):
            blacklisted = True
        # Absolute links.
        if (link["href"].startswith("http://") or link["href"].startswith("https://")):
          if link["href"] not in tocrawl:
            # Enforce URL length limit.
            if (len(link["href"]) <= 2048):
              tocrawl.append(link["href"])
              insertquery = "INSERT INTO tocrawl (url) VALUES (?)"
              linky = link["href"]
              try:
                db.execute(insertquery, (linky,))
                conn.commit()
              # Just skip what are probably duplicate URLs.
              except mariadb.IntegrityError:
                conn.rollback()
            else:
              print("Discarding URL for being too long.")
        elif (blacklisted) and (cfg["debug"] == "yes"):
          print("Skipping blacklisted link type. Link: " + link["href"])
        # Relative links.
        else:
          if (link["href"].startswith("/")):
            linky = protocol + full + link["href"]
          else:
            linky = protocol + full + "/" + link["href"]
          if linky not in tocrawl:
            # Enforce URL length limit.
            if (len(linky) <= 2048):
              tocrawl.append(linky)
              insertquery = "INSERT INTO tocrawl (url) VALUES (?)"
              try:
                db.execute(insertquery, (linky,))
                conn.commit()
              # Just skip what are probably duplicate URLs.
              except mariadb.IntegrityError:
                conn.rollback()
            else:
              print("Discarding URL for being too long.")
  
  customContinue()

print("Ending crawl...")
cleanup()
print("Done!")
