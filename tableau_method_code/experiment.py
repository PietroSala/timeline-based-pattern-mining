import sys
import subprocess

# EXPOECTS TO HAVE THE TRANSACTIONS GENERATED FOR "calories","steps", "distance", "heart_rate" in the fitbit/p{P}/fitbit/transaction/{n}.csv
# AND ALSO exercise.csv and sleep.csv copied in the same directory


import lexapriori_mem as mem
import json
import lexapriori_mem.tools.preprocess as preprocess
from lexapriori_mem.lexical_apriori.lexApriori import apriori 
from lexapriori_mem.lex.lex_mem import memLexRepr
import pandas as pd

P = str(sys.argv[1])
print(subprocess.run(["pip","install", "-e", "."], capture_output=True))
print(subprocess.run(["rm","-R", f"experiment_{P}_files"], capture_output=True))
print(subprocess.run(["mkdir", f"experiment_{P}_files"], capture_output=True))


for n in ["calories","steps", "distance", "heart_rate"]: 
    df = pd.read_csv(f"fitbit/p{P}/fitbit/transaction/{n}.csv")
    df["cluster"] = df.cluster.apply(lambda x: "".join(x.split('_')[0:2]) if n != "heart_rate" else "".join(x.split('_')[0:3] )  )
    for name, g in df.groupby(["cluster"]):
        g.to_csv(f"experiment_{P}_files/{name}.csv", index=False)

print(subprocess.run(["cp",f"fitbit/p{P}/fitbit/transaction/sleep.csv", 
	f"experiment_{P}_files/sleep.csv"], capture_output=True))

print(subprocess.run(["cp",f"fitbit/p{P}/fitbit/transaction/exercise.csv", 
	f"experiment_{P}_files/exercise.csv"], capture_output=True))

PATH = f"experiment_{P}_files/"

feature_files = subprocess.check_output(["ls", f"{PATH}"]).decode("utf-8")
feature_files = [PATH + f for f in  feature_files.split("\n")[0:-1]]
feature_files.sort()

print(feature_files)

digit_to_letter = {
    '0': 'A',
    '1': 'B',
    '2': 'C',
    '3': 'D',
    '4': 'E',
    '5': 'F',
    '6': 'G',
    '7': 'H',
    '8': 'I',
    '9': 'J'
}

state_variables = {}
i = 0

def tokenize_dict(d):
    return tuple([sanitize_label(d[key]) if key == 'cluster' else sanitize_timestamp(d[key])  for key in ['cluster', 'begin', 'end']])

def sanitize_label(l):
    r  = l.replace('_', '-')
    for d in [str(i) for i in range(10)]:
        r = r.replace(d, digit_to_letter[d])
    return r    

from datetime import datetime

def sanitize_timestamp(f):
    return pd.to_datetime(f, format = "%Y-%m-%d %H:%M:%S").timestamp()
    #return datetime.strptime(f, "%Y-%m-%d %H:%M:%S").timestamp()

transactions = {}
for i in range(len(feature_files)):    
    current_feature = pd.read_csv(feature_files[i])
    current_feature.drop_duplicates(inplace=True)
    grouped = current_feature.groupby('transaction')
    state_variables[i] = {name: [tokenize_dict(d) for d in list(group.to_dict(orient='records'))] for name, group in grouped}
    for j in state_variables[i]:
        if j not in transactions:
            transactions[j] = { t:[] for t in range(len(feature_files)) }    
        transactions[j][i] = state_variables[i][j] 
        
data = list(transactions.values())
dataset = [memLexRepr(preprocess.intervals_to_words(preprocess.dict_to_list(i))) for i in data]
        
now = datetime.now()        

apriori_obj = apriori(dataset, 0.05, database=f"p{P}_full_{str(now)}.sqlite", save_all=True)
frequent_itemsets = apriori_obj.apriori()
print([f'size: {i}: {len(frequent_itemsets[i])}' for i in frequent_itemsets])
print(apriori_obj.print_statistics())