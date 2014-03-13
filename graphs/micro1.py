#!/usr/bin/env python
import numpy as np
import matplotlib.pyplot as plt

tcpReadsRaw1G = (0.808, 0.7939, 0.8139)
tcpReadsAvg1G = np.average(tcpReadsRaw1G)
tcpReadsStd1G = np.std(tcpReadsRaw1G)

tcpReadsNcRaw1G = (0.88606, 0.83474, 0.88464)
tcpReadsNcAvg1G = np.average(tcpReadsNcRaw1G)
tcpReadsNcStd1G = np.std(tcpReadsNcRaw1G)

scrReadsRaw1G = (1.9305, 1.9526, 1.9529)
scrReadsAvg1G = np.average(scrReadsRaw1G)
scrReadsStd1G = np.std(scrReadsRaw1G)

scrReadsNcRaw1G = (2.3661, 2.3495, 2.3445)
scrReadsNcAvg1G = np.average(scrReadsNcRaw1G)
scrReadsNcStd1G = np.std(scrReadsNcRaw1G)

zcrReadsNcRaw1G = (5.8865, 5.8788, 5.8646)
zcrReadsNcAvg1G = np.average(zcrReadsNcRaw1G)
zcrReadsNcStd1G = np.std(zcrReadsNcRaw1G)

N = 5
oneGbMeans = (tcpReadsAvg1G, tcpReadsNcAvg1G, scrReadsAvg1G, scrReadsNcAvg1G, zcrReadsNcAvg1G)
oneGbStd =   (tcpReadsStd1G, tcpReadsNcStd1G, scrReadsStd1G, scrReadsNcStd1G, zcrReadsNcStd1G)

ind = np.arange(N)
barwidth = 0.35

fig, ax = plt.subplots()
rects1 = ax.bar(ind, oneGbMeans, barwidth, color='r', yerr=oneGbStd)

tcpReadsRaw20G = ( 0.78764, 0.75296, 0.78238 )
tcpReadsAvg20G = np.average(tcpReadsRaw20G)
tcpReadsStd20G = np.std(tcpReadsRaw20G)

tcpReadsNcRaw20G = ( 0.9147, 0.9244, 0.888 )
tcpReadsNcAvg20G = np.average(tcpReadsNcRaw20G)
tcpReadsNcStd20G = np.std(tcpReadsNcRaw20G)

scrReadsRaw20G = (1.9387, 1.9295, 1.9342)
scrReadsAvg20G = np.average(scrReadsRaw20G)
scrReadsStd20G = np.std(scrReadsRaw20G)

scrReadsNcRaw20G = (2.2875, 2.3336, 2.3552)
scrReadsNcAvg20G = np.average(scrReadsNcRaw20G)
scrReadsNcStd20G = np.std(scrReadsNcRaw20G)

zcrReadsRaw20G = (2.675, 2.654, 2.633)
zcrReadsAvg20G = np.average(zcrReadsRaw20G)
zcrReadsStd20G = np.std(zcrReadsRaw20G)

twentyGbMeans = (tcpReadsAvg20G, tcpReadsNcAvg20G, scrReadsAvg20G, scrReadsNcAvg20G, zcrReadsAvg20G)
twentyGbStd =   (tcpReadsStd20G, tcpReadsNcStd20G, scrReadsStd20G, scrReadsNcStd20G, zcrReadsStd20G)
rects2 = ax.bar(ind+barwidth, twentyGbMeans, barwidth, color='y', yerr=twentyGbStd)

# add some
ax.set_ylabel('GB/s')
ax.set_title('vecsum2 throughput')
ax.set_xticks(ind+barwidth)
ax.set_xticklabels( ('TCP', 'TCP-no-csum', 'SCR', 'SCR-no-csum', 'zero-copy') )

ax.legend( (rects1[0], rects2[0]), ('1 GB', '20 GB'), loc='left')

plt.savefig("out.png")

