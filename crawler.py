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
VERSION = "1.2"
TOCRAWLCOMMENT = "# URLs of websites to be visited by the spider."
# Blacklisted file types that will never be saved to the database.
# jpg, png, gif x2, riff, pdf, elf, rar x2, zip (and similar) x3, jpeg 2000 x2, class, ogg, flac, microsoft office, tar x2, 7z, gz, xz, matroska, mpeg4 x2
# Thanks to https://en.wikipedia.org/wiki/List_of_file_signatures
MAGICNUMS = [b"\xff\xd8\xff", b"\x89\x50\x4e\x47\x0d\x0a\x1a\x0a", b"\x47\x49\x46\x38\x37\x61", b"\x47\x49\x46\x38\x39\x61", b"\x52\x49\x46\x46", b"\x25\x50\x44\x46\x2d", b"\x7F\x45\x4C\x46", b"\x52\x61\x72\x21\x1a\x07\x00", b"\x52\x61\x72\x21\x1a\x07\x01\x00", b"\x50\x4b\x03\x04", b"\x50\x4b\x05\x06", b"\x50\x4b\x07\x08", b"\x00\x00\x00\x0c\x6a\x50\x20\x20\x0d\x0a\x87\x0a", b"\xff\x4f\xff\x51", b"\xca\xfe\xba\xbe", b"\x4f\x67\x67\x53", b"\x66\x4c\x61\x43", b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1", b"\x75\x73\x74\x61\x72\x00\x30\x30", b"\x75\x73\x74\x61\x72\x20\x20\x00", b"\x37\x7a\xbc\xaf\x27\x1c", b"\x1f\x8b", b"\xfd\x37\x7a\x58\x5a\x00", b"\x1a\x45\xdf\xa3", b"\x66\x74\x79\x70\x69\x73\x6f\x6d", b"\x66\x74\x79\x70\x4d\x53\x4e\x56"]

def regenUserAgent():
  cfg["useragent"] = cfg["botname"] + "/" + cfg["botversion"] + " (+" + cfg["boturl"] + ")"

def flush():
  # Flush the tocrawl file.
  with open("tocrawl.txt", "wt") as f:
    f.write(TOCRAWLCOMMENT + "\n")
    for t in tocrawl:
      f.write(t + "\n")
      
def cleanup():
  flush()
  # Close the database connection.
  db.close()
  conn.close()

def parseRobotsFile(r, u):
  tempurl = u.replace("https://", "").replace("http://", "")
  tempdirs = None
  if "/" in tempurl:
    tempdirs = tempurl[tempurl.find("/")+1:].strip("/").split("/")

  robo = r.split("\n")

  us = False
  for line in robo:
    line = line.strip()
    if line.startswith("user-agent:"):
      # This is case-insensitive.
      if cfg["botname"].lower() in line.lower():
        us = True
      elif (line == "user-agent: *"):
        us = True
      else:
        us = False
    elif us:
      if ((line == "disallow: /") or (line == "disallow: /*")):
        return False
      # Look for explicit allows.
      elif ((line == "allow: /" + "/".join(tempdirs)) or (line == "allow: /" + "/".join(tempdirs) + "/")):
        return True
      elif ((tempdirs != None) and (line.startswith("disallow: /" + tempdirs[0]))):
        # File level restrictions.
        if (line == "disallow: /" + tempdirs[0]):
          return False
        # Restricted dir.
        elif (line == "disallow: /" + tempdirs[0] + "/"):
          return False
        else:
          # Otherwise a subdir/file may be restricted.
          temprule = "disallow: /" + tempdirs[0] + "/"
          for i in range(1, len(tempdirs)):
            # File level restrictions.
            if (line == (temprule + tempdirs[i])):
              return False
            # Restricted dir.
            elif (line == (temprule + tempdirs[i] + "/")):
              return False
            else:
              temprule += tempdirs[i] + "/"
  
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
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `url` varchar(2048) NOT NULL,
  `timescraped` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `content` mediumtext NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `url` (`url`)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;""")
  db.execute("""CREATE TABLE IF NOT EXISTS `robots` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `url` varchar(2048) NOT NULL,
  `timescraped` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `content` mediumtext NOT NULL,
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
  print("Error: TLD list file not found.")
  exit()
with open("tlds-alpha-by-domain.txt", "rt") as f:
  for line in f:
    # Ignoring comments.
    if not line.startswith("#"):
      tlds.append(line.strip().lower())

tocrawl = []

# TODO: remove this as it will be done in the database instead.
# (will be replaced with a "seed.txt" file)
# Get the list of URLs we need to crawl.
with open("tocrawl.txt", "rt") as f:
  for line in f:
    # Ignoring comments.
    if not line.startswith("#"):
      tocrawl.append(line.strip())

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

# Main loop.
print("Starting crawl...")
while (len(tocrawl) > 0):
  url = tocrawl[0]
  
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
    if (cfg["debug"] == "yes"):
      print("Error: invalid TLD. (" + url + ")")
    tocrawl.pop(0)
    continue
  # The longest matching TLD will be the correct one.
  tld = max(matches, key=len)
  temp = temp[:temp.rfind(tld)-1]
  domain = temp[temp.rfind(".")+1:]
  subdomain = temp.replace(domain, "").strip(".")
  full = temp + "." + tld
  
  # Don't crawl blacklisted pages.
  if full in blacklist:
    if (cfg["debug"] == "yes"):
      print("Skipping blacklisted page.")
    tocrawl.pop(0)
    continue
  
  # Don't crawl pages we've already crawled.
  selectquery = "SELECT url FROM pages WHERE url=?"
  db.execute(selectquery, (url,))
  if (db.rowcount > 0):
    tocrawl.pop(0)
    if (cfg["debug"] == "yes"):
      print("Skipping duplicate page.")
    continue
  
  firstvisit = True
  
  selectquery = "SELECT url FROM pages WHERE (url LIKE ? OR url LIKE ?)"
  db.execute(selectquery, ("http://" + full + "%", "https://" + full + "%"))
  
  if (db.rowcount > 0):
    firstvisit = False
  
  s = requests.Session()
  # On the first visit we need to get the robots.txt file so we know what we can and cannot crawl.
  if firstvisit:
    robots = s.get(protocol + full + "/robots.txt", headers=headers)
    
    robot = robots.text
  
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
    
    # If it's a known bad filetype, don't waste space in the database.
    for mn in MAGICNUMS:
      if page.content.startswith(mn):
        if (cfg["debug"] == "yes"):
          print("Skipping known bad filetype.")
        stop = True
    
    if ((not stop) and (len(page.text) > 0)):
      # Add the page to the database.
      insertquery = "REPLACE INTO pages (url, content) VALUES (?, ?)"
      try:
        db.execute(insertquery, (url[:2048], page.text[:16777215]))
        conn.commit()
      except mariadb.Error as e:
        if (cfg["debug"] == "yes"):
          print("Database Error: " + str(e))
        conn.rollback()
      # Get all of the links from the page.
      soup = BeautifulSoup(page.text, "lxml")
      links = soup.find_all("a")
      for link in links:
        if not link.get("href"):
          continue
        # Absolute links.
        if (link["href"].startswith("http://") or link["href"].startswith("https://")):
          if link["href"] not in tocrawl:
            tocrawl.append(link["href"])
        # Relative links.
        elif (link["href"].startswith("/")):
          linky = protocol + full + link["href"]
          if linky not in tocrawl:
            tocrawl.append(linky)
  
  # Remove this URL from the list.
  tocrawl.pop(0)
  counter += 1
  
  # Periodically flush the crawl list.
  if (counter >= int(cfg["flushinterval"])):
    flush()
    counter = 0

print("Ending crawl...")
cleanup()
print("Done!")
