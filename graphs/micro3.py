#!/usr/bin/env python
import numpy as np
import matplotlib.pyplot as plt

def avg(vec):
    return np.average(vec)

def std(vec):
    return np.std(vec)

# remote cached TCP reads
tcp = (0.82963, 0.8238, 0.81175)

# iperf numbers
# gathered with iperf -c a2418.halxg.cloudera.com -f MBytes
# note 10 gig-E network (10 gigabits)
iperf = (1.0518, 1.0762, 1.07129)

# local uncached short-circuit local read numbers
scrNc = (0.12222, 0.12205, 0.12218)

# local dd rate to copy 20GB to a single disk
# perf stat dd if=~/tdata/20gig of=/dev/null bs=1048576
dd = (0.13490361136967638, 0.13494912418018407, 0.13048868010700071)

allVec = [tcp, iperf, scrNc, dd]

N = 4
ind = np.arange(N)
barwidth = 0.35

fig, ax = plt.subplots()
rects1 = ax.bar(ind, [avg(v) for v in allVec], barwidth, color='r', yerr=[std(v) for v in allVec])

ax.set_ylabel('GB/s')
ax.set_title('Comparison of different access methods')
ax.set_xticks(ind+barwidth)
ax.set_xticklabels( ('remote_cached', 'iperf', 'local_uncached', 'dd' ) )

#ax.legend( (rects1[0]), ('1 GB', '20 GB'), loc='left')

#ax.legend( (rects1[0], rects2[0]), ('1 GB', '20 GB'), loc='left')

plt.savefig("micro3.png")
