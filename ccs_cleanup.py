
# -*-

"""

A script that will take CCS (clinical classification software) .csv table and reference for all CPT's given by range

downloads can be found https://www.hcup-us.ahrq.gov/toolssoftware/ccs_svcsproc/ccscpt_license.jsp

ex. a row in the download may be '60111-60113' so the script will create a ro the same description for 60111, 60112, 60113

"""
import re

import pandas as pd, numpy as np

import os import pickle

pd.set_option('max_columns', 99)


def get_elms (s) :
  assert re.match('^[A-Z0-9]{5}-[A-Z0-9]{5}\'$',s) is not None, \ 
    f'pattern does not match {s}'

def extract_elms(s) :
  get_elms(s)
  return s.replace('\'','').split('-')

def has_letters(s) :
  if re.search('[A-Z]',s) is not None :
    return True
  else :
    return False

def number_list(s,e) :
  si,ei = map(int, (s,e)) 
  nums = [*range (si, ei+1)]
  return [repr(x).zfill(5) for x in nums]

def letter_list(s,e) :
  sre,ere = map(lambda x re.search('[A-Z]',x), (s,e)) 
  sst, sen = sre.start(), sre.end()
  est, een = ere.start(), ere.end() 
  assert (sst == est) & (sen == een), 'Start and End not same format'
  if sst == 0:
    sn, en = s[1:], e[1:]
  elif sst == 4:
    sn, en s[:-1], e[:-1]

  si, ei = map(int, (sn,en)) 
  rng = [*range(si, ei+1)] 
  if sst ==  0 :
    assert s[0] == e[0], 'start not equal' 
    out [s[0] + repr(x).zfill(4) for x in rng]
  elif sst == 4 :
    assert s[-1] == e[-1], 'end not equal' 
    out = [repr(x).zfill(4) + s[-1] for x in rng] 
  return out

def create_table (in_df, write:bool=False, pickle_path='') :
  out_dict = {
    'code': [],
    'ccs_num': [], 
    'ccs_name': []
  }

  for i, r in in_df.iterrows() :
    codes = r['code_range'] 
    code_num = r['ccs'] 
    code_name = r['ccs_label']

    s,e = extract_elms (codes) 
    if np.array([*map (has_letter, (s,e))]).all() : 
      ol = letter_list(s,e)
    else :
      assert (has_letter(s) == False) & (has_letter(e)==False) f'{s}, {e}, one of the codes has Letters and shouldn\'t'
      ol = number_list(s,e)

    for l in ol :
      out_dict['code'].append(l) 
      out_dict['ccs_num'].append(code_num) 
      out_dict['ccs_name'].append(code_name)

  if write :
    #save pickle file of dict for later 
    with open(pickle_path, 'wb') as f : 
      pickle.dump(out_dict, f, pickle.HIGHEST_PROTOCOL)

  return pd.DataFrame(out_dict)

if name '__main__' :
  folder = '\\Documents'
  file = 'ccs_cat_list.csv' #from website above
  cur = os.path.abspath('.')
  fp = lambda x : os.path.join(cur+folder,x)                               

  in_df = pd.read_csv(fp(file))
  in_df.columns = in_df.columns.str.lower().str.replace(' ','_')
  all_df = create_table(in_df)
  #run again to save pickle file
  all_df = create_table(in_df, True, fp('ccs_desc.pkl'))
  #upload pickle file, you can then turn this into pd.DataFrame
  with open(fp('ccs_desc.pkl'), 'rb') as f :
    out_dict = pickle.load(f)
