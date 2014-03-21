#!/usr/bin/env python
import numpy as np
import matplotlib.pyplot as plt

# 1 G x 20

def avg(vec):
    numeric_vec = [int(s.replace(",", "")) for s in vec]
    return np.average(numeric_vec)

def std(vec):
    numeric_vec = [int(s.replace(",", "")) for s in vec]
    return np.std(numeric_vec)

tcp = ("56,941,111,714", "57,425,424,643", "58,552,010,333")
tcpNc = ("50,950,205,711", "51,988,494,233", "52,604,911,053")
scrReads = ("26,925,647,069", "27,417,391,153", "27,086,041,030")
scrReadsNc = ("23,574,844,081", "23,546,252,040", "23,112,340,964")
zcrReads = ("12,602,014,986", "12,717,778,761", "12,808,630,928")

allVec = [ tcp, tcpNc, scrReads, scrReadsNc, zcrReads ]

N = 5
ind = np.arange(N)
barwidth = 0.35

fig, ax = plt.subplots()
rects1 = ax.bar(ind, [avg(v) for v in allVec], barwidth, color='r', yerr=[std(v) for v in allVec])

#twentyGbMeans = (tcpReadsAvg20G, tcpReadsNcAvg20G, scrReadsAvg20G, scrReadsNcAvg20G, zcrReadsAvg20G)
#twentyGbStd =   (tcpReadsStd20G, tcpReadsNcStd20G, scrReadsStd20G, scrReadsNcStd20G, zcrReadsStd20G)
#rects2 = ax.bar(ind+barwidth, twentyGbMeans, barwidth, color='y', yerr=twentyGbStd)

# add some
ax.set_ylabel('total cycles')
ax.set_title('vecsum2 total cycle count')
ax.set_xticks(ind+barwidth)
ax.set_xticklabels( ('TCP', 'TCP-no-csum', 'SCR', 'SCR-no-csum', 'zero-copy') )

#ax.legend( (rects1[0]), ('1 GB', '20 GB'), loc='left')

#ax.legend( (rects1[0], rects2[0]), ('1 GB', '20 GB'), loc='left')

plt.savefig("micro2.png")
