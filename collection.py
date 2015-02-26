# collection.py
# usage:
#   python collection.py "Tag name" outputfile
# creates an output list of all startups that match specified tag

import requests
import json
import sys
from time import sleep
from concurrent.futures import ThreadPoolExecutor
import itertools
import pickle

requests.packages.urllib3.disable_warnings()

# make request
def doGet (url):
  r = requests.get(url,verify=False)
  return r.json()

# attempt to make a request to angellist
# print an error and retry on failure
def getAngel (param):
  try:
    return doGet("https://api.angel.co/1/" + param)
  except ValueError:
    print "Do Get Angel Failure", param, "retrying"
    return getAngel(param)

keyI = 0
keys = ["c89b9cb0fe0338fe1e396ad2859b1b09", "32b35c21b861f3ff608cfb3a9009c1d5", 
        "6aea78709597798d788f90c190c08964", "a6642e33a7bf7352bc34b616b4cf29a7", 
        "e31154ea007e2a72a8ed195cf5c35640", "89b8be277631ed864afe92509aadcccc"
        "e7c78586a60ade0506b5d6c4b984763c", "c21c8dec57e2c8506d2ba80f721f9162",
        "96b13cedce6c1ea2f7f3bb54c14753b8", "cc0354e7d57816ef359329e2ab6ec9ea",
        "065809b7e96ffe44b4fdc282ae9b0c70", "516d0f3e2154554eaf944771d2d2afe4",
        "c2ef881b0170b9f1b46bd1df96b11b66", "4f4d52493644c8c67ab38fa9510fa7ed",
        "5f7ec1e68c4899be6b3e8767ee8ff0a1", "14fe80a9334ba1cb292218b01ce4c3ee",
        "4f9588ddf0b3d74c1422d67afe18dd05"]

# attempt to make a request to crunchbase
# use a rotating set of keys; print an error and retry on failure
def getCrunchbase (param, take=0, index=-1):
  if index == -1:
    global keyI
    keyI = (keyI + 1) % len(keys)
    index = keyI
  else:
    index = index % len(keys)

  #print keys[index]
  try:
    return doGet("http://api.crunchbase.com/v/2/" + param + "?user_key=" + keys[index])["data"]
  except ValueError, ConnectionError:
    print "Get Error", "Retry in 30 seconds!", take, index
    sleep(30)
    return getCrunchbase(param, take+1, index+1)
  except:
    return None

# parallelize a task
def runThreads (func, argsl, workers=10):
  tpe = ThreadPoolExecutor(max_workers=workers)
  results = [tpe.submit(func, *args) for args in argsl]
  tpe.shutdown()
  return [result.result() for result in results]

# get startups corresponding to a particular tag
def getStartups (tag_id):
  tag_id = str(tag_id)
  def processFund (fund):
    return getCrunchbase (fund["path"])

  def isStartup (startup):
    if "company_type" not in startup:
      return False
    for t in startup["company_type"]:
      if "name" in t and t["name"] == "startup":
        return True
    return False

  def processStartup (startup):
    if not (isStartup(startup)):
      return None
    if "crunchbase_url" in startup and startup["crunchbase_url"] != None:
      cbname = startup["crunchbase_url"].rsplit('/',1)[1]
      funds = getCrunchbase("organization/" + cbname + "/funding_rounds")
      if funds and "items" in funds:
        funds = funds["items"]
        args = [[fund] for fund in funds]
        funds = runThreads(processFund, args)
        startup["funding"] = funds

        totalFunds = sum([info["properties"]["money_raised"] for info in startup["funding"]
                        if "properties" in info and "money_raised" in info["properties"]
                            and info["properties"]["money_raised"] is not None])

        if totalFunds > 500000:
          return None
        #print startup
    if "funding" not in startup:
      startup["funding"] = []
    return startup

  def processPage(page):
    startups = getAngel("/tags/" + tag_id + "/startups?page=" + str(page))["startups"]
    # process startups in parallel 
    args = [[startup] for startup in startups]
    startups = runThreads(processStartup, args)
    startups = [x for x in startups if x != None]
    print "finished page",page, "and got", len(startups), "startups"
    return startups

  # first, get num pages (serial)
  pages = getAngel("tags/" + tag_id + "/startups")["last_page"]
  #print pages

  # get all startups in parallel
  args = [[page+1] for page in xrange(int(pages))]
  pages = runThreads(processPage, args, workers=20)

  return list(itertools.chain(*pages))

def output (startups, outputFile):
  maxFunds = 0
  for startup in startups:
    maxFunds = max(maxFunds,len(startup["funding"]))

  print "max number of funds is",maxFunds

  cols = "name,description,url,location,market"
  for i in xrange(maxFunds):
    cols+=",FundRound"+str(i)+"Date"+",FundRound"+str(i)+"Stage"+",FundRound"+str(i)+"Amount"
  cols += "\n"

  def cleanString (prop):
    return ''.join (x for x in prop if x not in ',\r\n\t')
  def fixParam (startup, param):
    if param in startup and startup[param]:
      return cleanString(startup[param])
    else:
      return ""

  def convert (startup):
      if startup == None or startup['hidden']:
        return ""
      out = []

      out.append (fixParam (startup, "name") )
      out.append (fixParam (startup, "product_desc") )
      out.append (fixParam (startup, "company_url") )


      if len(startup['locations']):
        out.append ( cleanString(startup['locations'][0]['name']) )
      else:
        out.append ( "" )
      if len(startup['markets']):
        out.append ( cleanString(startup['markets'][0]['name']) )
      else:
        out.append ( "" )

      for i in xrange(maxFunds):
        if i < len(startup["funding"]) and "properties" in startup["funding"][i]:
          out.append ( startup["funding"][i]["properties"]["announced_on"] )
          out.append ( startup["funding"][i]["properties"]["funding_type"] )
          if "money_raised" in startup["funding"][i]["properties"]:
            out.append ( str(startup["funding"][i]["properties"]["money_raised"]) )
          else:
            out.append ( "0" )

      return ','.join(out) + "\n"

  # write to file'''
  f = open(outputFile, 'w')
  f.write(cols)
  for startup in startups:
    f.write(convert(startup).encode("utf8","ignore"))

def searchTag (tagName):
  res = getAngel("search?query=" + tagName + "&type=MarketTag")
  for i in res:
    if (i['name'].lower() == tagName.lower()):
      return i['id']
  return None

def loadStartups (tag, fromPickle=False):
  if (fromPickle):
    with open('data.pickle', 'r') as handle:
      return pickle.load(handle)
  else:
    startups = getStartups(tag)
    print "Done,", len(startups), "records processed\n"
    with open('data.pickle', 'wb') as handle:
      pickle.dump(startups, handle)
    print "Pickled!"
    return startups

tag = searchTag(sys.argv[1])
if not tag: 
  print "Tag not found"
  sys.exit()

startups = loadStartups (tag, fromPickle = sys.argv[3]=='pickle')
output(startups, sys.argv[2])