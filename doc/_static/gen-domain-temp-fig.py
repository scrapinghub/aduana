#!/usr/bin/env python

import matplotlib.pyplot as plt
import numpy as np

steps_time = [1.0, 2.5, 2.6, 2.7, 4.0, 5.0]

t0 = 0.0
t1 = steps_time[-1] + 1.0
N = 1000 # number of time steps
dt = (t1 - t0)/N
tt = np.linspace(t0, t1, N)
nn = np.zeros(N)

n = 0.0  # number of crawls in time window
T = 2.0 # time window
for i, t in enumerate(tt):
    n += -n*dt/T
    if n<0:
        n = 0.0
    if len(steps_time)>0 and t>=steps_time[0]:
        n += 1.0
        steps_time.pop(0)    
    nn[i] = n

plt.figure()
plt.plot(tt, nn)
plt.xlabel('t')
plt.ylabel('n')
plt.savefig('domain-temp-evolve.svg')

