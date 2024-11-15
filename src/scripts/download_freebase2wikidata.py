# install wikidata before with the following command:
# pip install Wikidata

import argparse
import json
import os
import re

import numpy as np
import pandas as pd
from tqdm import tqdm
from wikidata.client import Client

parser = argparse.ArgumentParser()
parser.add_argument("--data_dir", type=str, default="../data/MovieSummaries")
parser.add_argument("--output_dir", type=str, default="../data/")
args = parser.parse_args()

os.system("wget https://storage.googleapis.com/freebase-public/fb2w.nt.gz")
os.system("gzip -dv fb2w.nt.gz")
out_path = os.path.join(args.output_dir, "fb2w.nt")
os.system(f"mv fb2w.nt {out_path}")

with open(out_path) as f:
    lines = f.read().splitlines()

pattern_subj = re.compile("rdf.freebase.com/ns/.*>")
pattern_obj = re.compile("www.wikidata.org/entity/.*>")

fb2w_raw = {}
for line in tqdm(lines):
    if not line or line[0] == "#":  # skip lines
        continue
    subject_raw, predicate_raw, obj_raw = line.split("\t")
    subj = re.findall(pattern_subj, subject_raw)[0][19:-1].replace(".", "/", 1)
    obj = re.findall(pattern_obj, obj_raw)[0][24:-1]
    fb2w_raw[subj] = obj

# prepare usefull data
result_json = {}
character = pd.read_csv(os.path.join(args.data_dir, "character_processed.csv"))
all_etnicities = set(character["Actor ethnicity (Freebase ID)"].values)
lost_char_names = set(
    character[
        pd.isna(character["Character name"])
        & ~pd.isna(character["Freebase character ID"])
    ]
)

# no actors such as "Freebase actor ID" is not nan but "Actor date of birth" is nan
# A lot of actors with known freebase actor id but unknown height... Do we want to parse it?
all_required_fbid = all_etnicities | lost_char_names

result_json = {fbid: fb2w_raw[fbid] for fbid in all_required_fbid if fbid in fb2w_raw}
with open(os.path.join(args.output_dir, "useful_fb2w.json"), "w") as f:
    json.dump(result_json, f)

# fill etnicities
client = Client()
etnid2name = {}
for etn_id in tqdm(all_etnicities):
    if etn_id in result_json:
        entity = client.get(result_json[etn_id], load=True)
        etnid2name[etn_id] = str(entity.label)

with open(os.path.join(args.output_dir, "etnid2name.json"), "w") as f:
    json.dump(etnid2name, f)
