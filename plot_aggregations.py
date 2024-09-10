import numpy as np
import matplotlib.pyplot as plt


def plot_aggregations(path):
    fich_datos=path
    datos=np.loadtxt(fich_datos,comments="#")      
    F=datos
    f=np.diff(F)
    g=3251-f
    x=np.hstack(F)
    y=np.hstack(g)

    #plotting
    fig=plt.figure()
    ax1=fig.add_subplot(111)
    ax1twin=ax1.twinx()
    ax1.plot(np.arange(1,len(F)+1,1),F,"-o",label="Documents in the collection",color="black")
    ax1.set_xlabel("Calls")
    ax1.set_ylabel("Number of documents")
    ax1twin.plot(np.arange(1,len(F),1),f,"-o",label="Aggregations",color="red")
    fig.legend()
    plt.show()
    return


if __name__== "__main__" :
    plot_aggregations("./españa_sin_galicia.txt")