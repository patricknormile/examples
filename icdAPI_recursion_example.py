import requests, json, time, pandas as pd
# Initial setup
client_id = ''
client_secret = ''
token_endpoint = 'https://icdaccessmanagement.who.int/connect/token'
scope = 'icdapi_access'
grant_type = 'client_credentials'
payload = {'client_id' : client_id,
           'client_secret' : client_secret, 
           'scope' : scope,
           'grant_type' : grant_type}
r = requests.post(token_endpoint, data=payload, verify=False).json()
token = r['access_token']

headers = {'Authorization': 'Bearer '+token,
           'Accept' : 'application/json',
           'Accept-Language' : 'en',
           'API-Version' : 'v2',
           'releaseId' : '2021'}
# allow to update token and headers
# API requires a new token after an hour, so these functions allow you to update global variables
def update_token() :
  global token
  r = requests.post(token_endpoint, data=payload, verify=False).json()
  token = r['access_token']
def update_headers() : 
  #update with new token
  global headers
  update_token()
  headers = {'Authorization': 'Bearer '+token,
             'Accept' : 'application/json',
             'Accept-Language' : 'en',
             'API-Version' : 'v2',
             'releaseId' : '2021'}

dc = json.JSONDecoder()
def get(uri) :
  r = requests.get(uri, headers=headers, verify=False) #could you just use .json() here?
  js = dc.decoder(r.text)
  return js


#### Set up base case
uri_base = 'https://id.who.int/icd/release/10'
uri_latest = get(uri_base)['latestRelease']

# Get contents 
# function to return name, code, and any children links that also need to be investigated

def get_contents(uri) :
  g = get(uri)
  try :
    chs = g['child']
  except KeyError :
    chs = None
  try :
    p = get(g['parent'][0])
    if 'code' not in p :
      p['code'] = ''
  except KeyError :
    p = {'code':'', 'title':{'@value':''}}
  try :
    g['code'] #check if code in g might be faster?
  except KeyError :
    g['code'] = 'base'
  
  out_dict = {
    'parent_code' : [p['code']],
    'parent_title' : [p['title']['@value']],
    'code' : [g['code']],
    'title' : [g['title']['@value']]
  }
  df = pd.DataFrame(out_dict) # could just return dict, might be faster?
  return df, chs

# Create recursive function 

dfs = []
t0 = time.time()
def recurs(uri) : 
  global dfs
  global t0
  if time.time() - t0 > 1800 :#half hour
    update_headers()
    t0 = time.time()
  df, ch = get_contents(uri)
  dfs.append(df) 
  if ch is not None :
    for c in ch :
      recurs(c)
      
# run and get your tree structure
#took about 4 hours
recurs(uri_latest)

df_all = pd.concat(dfs, axis=1)
df_all.head()
