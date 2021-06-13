import datetime
import multiprocessing
import pandas as pd
import time

start_time = datetime.datetime.now()

# contantes
HOW_MANY_SITES = 10
HOW_MANY_PROCESSORS = (multiprocessing.cpu_count() - 1)
METHOD = 0  # Se recomienda un valor de cero


def worker_task(num):
    df = pd.DataFrame({'a': range(3), 'b': range(3)})
    print('Worker:', num)
    time.sleep(20)  # Espere 20 segundos
    return df


if __name__ == '__main__':
    fullDataFrame = pd.DataFrame()  # Definir el marco de datos de Pandas vacío
    print('Creating a pool of %i processes.' % HOW_MANY_PROCESSORS)
    pool = multiprocessing.Pool(processes=HOW_MANY_PROCESSORS)  # Iniciar todos los procesos de trabajo
    if METHOD == 0:  # Multiple/parallel processing
        resultsList = []  # Inicializando como una lista vacía
        print('Running %i tasks in groups of %i parallel processors.' % (HOW_MANY_SITES, HOW_MANY_PROCESSORS))
        for i in range(HOW_MANY_SITES):
            poolResults = pool.apply_async(worker_task, (i,))
            resultsList.append(poolResults)
        pool.close()   # Close first before joining!
        pool.join()
        for result in resultsList:
            # for result in poolResults.get():
            # indxDataFrame = result.get()
            fullDataFrame = fullDataFrame.append(result.get())
    elif METHOD == 1:  # ¡Realmente no utiliza procesamiento múltiple/paralelo!
        for i in range(HOW_MANY_SITES):
            poolResults = pool.apply_async(worker_task, (i,))
            fullDataFrame = fullDataFrame.append(poolResults.get())
    else:  # Single processing, in series
        for i in range(HOW_MANY_SITES):
            indxDataFrame = worker_task(i)
            fullDataFrame = fullDataFrame.append(indxDataFrame)

    print('len(fullDataFrame) = %s' % len(fullDataFrame))
    print(fullDataFrame)
    endTime = datetime.datetime.now()
    print('With METHOD = %s, este código se ejecutó en %s segundos.' % (METHOD, str((endTime - start_time).seconds)))
