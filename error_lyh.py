#!/usr/bin/env python3

import csv
import matplotlib.pyplot as plt
from collections import defaultdict
import numpy as np

plt.style.use('classic')

fig, (ax1, ax2) = plt.subplots(2,1,figsize = (8,8))
solvers = set()
def plot(input):
    filename = input[8:-4]
    print(filename)


    #budget -> querysize -> dimlevel -> runs
    errorXtime = defaultdict(lambda: {})
    #timeXfraction = defaultdict(lambda: {})
    errorXfraction = defaultdict(lambda: {})

    with open(input) as fh:
        header = [h.strip() for h in fh.readline().split(',')]
        reader = csv.DictReader(fh, fieldnames=header)
        data = list(reader)
        timescale=1000.0 * 1000.0
        errorFactor = 0.5
        prevError = 0
        prevSolver = ""
        epsilon = 0.00001
        for row in data:
            solver = row['Solver']
            solvers.add(solver)
            fraction = float(row['FractionOfSamples'])
            time = float(row['ElapsedTotalTime'])/timescale
            error = float(row['Error']) * errorFactor
            if(solver != prevSolver):
                prevError = errorFactor
                errorXtime[solver][-epsilon] = prevError

            errorXtime[solver][time-epsilon] = prevError
            errorXtime[solver][time + epsilon] = error
            errorXfraction[solver][fraction - epsilon] = prevError
            errorXfraction[solver][fraction + epsilon] = error
            prevError = error
            prevSolver = solver

        def myplot(data, l, ax):
            (X,Y) = zip(*data.items())
            ax.plot(X, Y, label=l)
        for s in sorted(filter(lambda x: ("nline" in x) or ("aive" in x), solvers)):
            myplot(errorXtime[s], s, ax1)
            myplot(errorXfraction[s], s, ax2)
        ax1.legend()
        ax2.legend()
        ax1.set_ylabel('Error')
        ax2.set_ylabel('Error')
        ax1.set_xlabel('Time(s)')
        ax2.set_xlabel('SampleFraction')
cubeGenerator = 'SSB'
matalgo = 'prefix'
title = f"{cubeGenerator} {matalgo}"
minD = 9
plot(f'expdata/online-sampling/202310/20/211842/SSB-sf1-true-qsize_interleaving.csv')

fig.suptitle(title,fontsize='xx-large', fontweight='heavy')
plt.subplots_adjust(hspace=0.4, wspace=0.5)
plt.savefig(f'figs/online-sampling/{cubeGenerator}_{matalgo}_{minD}_20231020.pdf', bbox_inches = 'tight', pad_inches=0.01)



