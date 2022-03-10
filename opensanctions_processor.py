#! env python3

import argparse
import orjson
import csv
import sys

def isOrg(schema):
   return recType(schema) == 'ORGANIZATION'

def recType(schema):
   if schema in ('Organization','Company'):
     return 'ORGANIZATION'
   elif schema in ('Person'):
     return 'PERSON'
   elif schema in ('LegalEntity'):
     return None # this is an unknown entity
   else:
     print(f'Unknown schema [{schema}]', file=sys.stderr)
     return schema
   

def processNames(names, is_org):
   arr = names.split(';')
   if not arr:
     return None

   ret = []
   for val in arr:
     tmpDict = {}
     tmpDict['NAME_TYPE'] = 'PRIMARY'
     if is_org:
       tmpDict['NAME_ORG'] = val
     else:
       tmpDict['NAME_FULL'] = val
     ret.append(tmpDict)

   return ret

def processAliases(aliases, is_org):
   arr = aliases.split(';')
   if not arr:
     return None

   ret = []
   for val in arr:
     tmpDict = {}
     tmpDict['NAME_TYPE'] = 'ALIAS'
     if is_org:
       tmpDict['NAME_ORG'] = val
     else:
       tmpDict['NAME_FULL'] = val
     ret.append(tmpDict)

   return ret

def processCOA(countries,is_org):
   arr = countries.split(';')
   if not arr:
     return None

   ret = []
   for val in arr:
     tmpDict = {}
     tmpDict['COUNTRY_OF_ASSOCIATION'] = val
     ret.append(tmpDict)

   return ret

def processID(ids,is_org):
   arr = ids.split(';')
   if not arr:
     return None

   ret = []
   for val in arr:
     tmpDict = {}
     tmpDict['OTHER_ID_NUMBER'] = val
     ret.append(tmpDict)

   return ret

def processDOB(dobs,is_org):
   arr = dobs.split(';')
   if not arr:
     return None

   ret = []
   for val in arr:
     tmpDict = {}
     tmpDict['DATE_OF_BIRTH'] = val
     ret.append(tmpDict)

   return ret

def processPhones(phones,is_org):
   arr = phones.split(';')
   if not arr:
     return None

   ret = []
   for val in arr:
     tmpDict = {}
     tmpDict['PHONE_NUMBER'] = val
     ret.append(tmpDict)

   return ret

def processEmails(emails,is_org):
   arr = emails.split(';')
   if not arr:
     return None

   ret = []
   for val in arr:
     tmpDict = {}
     tmpDict['EMAIL_ADDRESS'] = val
     ret.append(tmpDict)

   return ret

def processAddresses(addresses,is_org):
   arr = addresses.split(';')
   if not arr:
     return None

   ret = []
   for val in arr:
     tmpDict = {}
     tmpDict['ADDR_FULL'] = val
     ret.append(tmpDict)

   return ret

   
split_map = {
  'aliases' : processAliases,
  'identifiers' : processID,
  'countries' : processCOA,
  'birth_date' : processDOB,
  'name' : processNames,
  'phones' : processPhones,
  'emails' : processEmails,
  'addresses' : processAddresses,
}

direct_map = {
  'id' : 'RECORD_ID',
}


def processRecord(jdict):
   out_dict = { "DATA_SOURCE" : "OPENSANCTIONS" }

   is_org = isOrg(jdict['schema'])
   rec_type = recType(jdict['schema'])
   if rec_type:
      jdict['REC_TYPE'] = rec_type

   for key,value in jdict.items():
      if not value:
        continue

      if key in direct_map:
        out_dict[direct_map[key]] = value
      elif key in split_map:
        result = split_map[key](value,is_org)
        if result:
          out_dict[key] = result
      elif value:
        out_dict[key] = value

   out_json = orjson.dumps(out_dict).decode()
   print(out_json)

parser = argparse.ArgumentParser(description='Process opensanctions data')
parser.add_argument('sanctions', help='opensanctions CSV file')
args = parser.parse_args()

with open(args.sanctions, 'r') as fp:
   numLines = 0
   dict_reader = csv.DictReader(fp)

   for record in dict_reader:
      numLines += 1
      processRecord(record)
      if numLines%100 == 0:
         print(f'Processed {numLines} records', file=sys.stderr)
 
