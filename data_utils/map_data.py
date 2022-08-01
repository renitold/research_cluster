#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jul 29 18:28:32 2022

@author: danieldomek
"""
import numpy as np
import pandas as pd
import os
import requests as r
from tqdm import tqdm
import datetime
import time
import json

if __name__ == "__main__":
    
    
    pathh = r'/Users/research_cluster/stocks_all_history/'
    dirs = os.listdir(pathh)
    
    for y in tqdm(range(0,len(dirs))):
        
        if(dirs[y][-4:] == '.txt'):
            begin = time.time()
            f = open(pathh+'/'+dirs[y])
            stuff = f.read()
            f.close()
            
            stuff = stuff.split("\n")
            stuff = [x.split(',') for x in stuff]
            stuff = np.array(stuff)
            inds = np.unique(stuff[:,0],return_index=True)
            #960 elems in length
            mapp = {}
            for x in range(0,len(inds[0])):
                mapp[str(inds[0][x])] = int(inds[1][x])
            mapp = json.dumps(mapp)
            f = open(r'/Users/research_cluster/stocks_all_history_maps/'+'/'+dirs[y], 'w')
            f.write(str(mapp))
            f.close()
            
            
            end = time.time()
            # print(end-begin)
    
    
