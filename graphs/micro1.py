#!/usr/bin/env python
import numpy as np
import matplotlib.pyplot as plt

N = 5
oneGbMeans = (20.314314, 35.314314, 30.314314, 35.314314, 27.314314)
oneGbStd =   (2, 3, 4, 1, 2)

ind = np.arange(N)
barwidth = 0.35

fig, ax = plt.subplots()
rects1 = ax.bar(ind, oneGbMeans, barwidth, color='r', yerr=oneGbStd)

twentyGbMeans = (25.314314, 32.314314, 34.314314, 20.314314, 25.314314)
twentyGbStd =   (3, 5, 2, 3, 3)
rects2 = ax.bar(ind+barwidth, twentyGbMeans, barwidth, color='y', yerr=twentyGbStd)

# add some
ax.set_ylabel('GB/s')
ax.set_title('vecsum2 throughput')
ax.set_xticks(ind+barwidth)
ax.set_xticklabels( ('TCP', 'TCP-no-csum', 'SCR', 'SCR-no-csum', 'zero-copy') )

ax.legend( (rects1[0], rects2[0]), ('1 GB', '20 GB') )

plt.savefig("out.png")

