import sys
import matplotlib.pyplot as plt
import numpy
from scipy.misc import logsumexp

def read_file(filename):
    objective = {}
    with open(filename, "r") as fp:
        for lines in fp:
            (rule,atom,sample,value) = lines.split(";")
            if not rule in objective:
                objective[rule] = {}
            if atom == "all":
                objective[rule][sample] = float(value)
            else:
                if not atom in objective[rule]:
                    objective[rule][atom] = []
                objective[rule][atom].append(float(value))

    for rule in objective:
        for atom in objective[rule]:
            objective[rule][atom] = numpy.array(objective[rule][atom])
    return objective

def obj_value(rule, w):

    atom_arr = []
    for atom in rule:
        if not atom == "obj":
            atom_arr.append(logsumexp(rule[atom]*-w) - numpy.log(len(rule[atom])))
    return -1 * (w*rule["obj"] + sum(atom_arr))

def plot_function(rule, label, ax):
    y = []
    for i in x:
        y.append(obj_value(rule, i))
    ax.plot(x,y,label=label)

filename = sys.argv[1]
rule = sys.argv[2]
obj = read_file(filename)

x = numpy.linspace(0,20,100)
fig=plt.figure()
fig.show()
ax=fig.add_subplot(111)

plot_function(obj[rule], rule, ax)
plt.show()
