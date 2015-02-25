import requests
import json
import sys
from time import sleep
from concurrent.futures import ThreadPoolExecutor
import itertools
import pickle
from pyquery import PyQuery as pq
from lxml import etree
import urllib
import traceback

fields = [["offeringSalesAmounts totalOfferingAmount", "total offering amount"], 
          ["offeringSalesAmounts totalAmountSold", "total amount sold"],
          ["offeringSalesAmounts totalRemaining", "total remaining"],
          ["offeringData issuerSize revenuerange", "issuer size"],
          ["typeOfFiling dateOfFirstSale value", "date of first sale"],
          ["typesOfSecuritiesOffered isEquityType", "is equity"],
          ["typesOfSecuritiesOffered isDebtType", "is debt"]]

# make request
def doGet (url):
  #print "GETTING", url
  r = requests.get(url)
  return r.text

def getLink (doc, search):
  res = doc("a:contains('" + search + "')")
  if len(res) == 0:
    return None
  return res[0].attrib['href']

working = 0

def getEdgar (param):
  try:
    param = param.encode("ASCII", 'ignore')
  except Exception:
    print param
    return
  global working
  working += 1
  if working % 100 == 0:
    print working
  try:
    sec = "http://www.sec.gov"
    d = pq(url= sec + "/cgi-bin/browse-edgar?action=getcompany&company=" + param)
    link = getLink(d, "[html]") or getLink(d, "Documents")
    if not link:
      return None

    d = pq(url = sec + link)
    link = getLink(d, "primary_doc.xml")

    if link is None:
      return

    d = pq(url = sec + link)

    output = {}
    for i in fields:
      output[i[1]] = d(i[0]).html() or ""
    output['name'] = param
    return output
  except Exception:
    print traceback.format_exc()
    return None

def runThreads (func, argsl, workers=100):
  tpe = ThreadPoolExecutor(max_workers=workers)
  results = [tpe.submit(func, *args) for args in argsl]
  tpe.shutdown()
  return [result.result() for result in results]

def output (startups, outputFile):
  cols = ",".join(startups[0].keys()) + "\n"

  def convert (startup):
      if startup == None:
        return ""

      out = ""
      for i in startup:
        out += startup[i].replace(",","") + ","
      return out[:-1] + "\n"

  # write to file
  f = open(outputFile, 'w')
  f.write(cols)
  for startup in startups:
    f.write(convert(startup).encode("utf8","ignore"))

#print getEdgar("welltrackone")

with open('startups.csv') as f:
    content = f.readlines()
startups = [[line[:line.index(",")]] for line in content[1:]]
print len(startups)
results = runThreads(getEdgar, startups)
results = [x for x in results if x != None]

output(results, "edgarOut.csv")