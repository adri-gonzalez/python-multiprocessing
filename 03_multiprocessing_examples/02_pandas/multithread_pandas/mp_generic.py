"""
Soporte de multiprocesamiento genérico para llamadas Pandas de la forma:
            df_out = df_in.groupby(gb_cols).apply(foo_func, args)

Esto es útil cuando el trabajo realizado por foo_func consume mucha CPU para cada grupo.
La llamada de multiprocesamiento equivalente se convierte en,
            df_out = mp_groupby(df_in, gb_cols, gb_func, *args, **mp_args)

dónde:
- df_in es la entrada df
- df_out es la salida df
- gb_cols es la lista de columnas en la llamada group-by. Debe ser una lista. Si gb_cols == [],
realiza una aplicación de fila.
- args es la lista de argumentos de gb_func
- mp_args: argumentos de multiprocesamiento (# CPUs, # queues, ...)

"""

import multiprocessing as mp
import numpy as np
import pandas as pd
from operator import itemgetter
import sys


def mp_groupby(df_in, gb_cols, gb_func, *gb_func_args, **mp_args):
    """
    Implementación MP de df_out = df_in.groupby (gb_cols) .apply (gb_func, gb_func_args)
    : param gb_cols: lista de group_by cols
    : param gb_func: función para aplicar en el group_by
    : param gb_func_args: gb_func args
    mp_args
    : param n_cpus: número de CPU a utilizar. Si es 0, no utilice multiprocesamiento.
    : param n_queues: Número de colas con tareas. Si n_queues! = 1, se establece en n_cpus. El valor predeterminado es 1.
    : return: Igual que group_by sin multiprocesamiento.

    Opciones de procesamiento:
    - n_queues! = 1: una cola de tareas separada por CPU. La cola de tareas contiene varios DF, un DF por combinación de GB.
    - n_queues = 1: Una sola lista de tareas compartida. La lista de tareas contiene varios DF, un DF por grupo_por combinación.
    """

    # MP parameters
    # numero de CPUs
    n_cpus = n_cpus_input(**mp_args)
    if n_cpus == 0:  # ejecutar sin multiprocesamiento
        return df_in.groupby(gb_cols).apply(gb_func, *gb_func_args)
    else:  # mantenga el valor propuesto pero asegúrese de que no esté por encima de las CPU disponibles
        n_cpus = min(n_cpus, mp.cpu_count())
    qs = mp_args['n_queues'] if 'n_queues' in mp_args else 1
    n_queues = n_cpus if qs != 1 else 1

    # construir los grupos de procesamiento
    if n_queues == n_cpus:  # crear tantas colas de tareas como CPU
        df_groups = df_grouper(df_in, gb_cols, n_cpus)  # grupos de procesamiento para dfs
    else:  # crear una sola cola de tareas
        df_groups = df_grouper(df_in, gb_cols, 1)

    gb_func_args += (df_in,)  # añadir df_in a los argumentos de función
    mp_func_by_groups = simple_parallel(func_by_groups, n_cpus)
    # una plantilla general que agrupa una función utilizada en un grupo pd por
    result = mp_func_by_groups(df_groups.keys(), df_groups, gb_func, *gb_func_args)
    if len(result) > 0:
        return pd.concat(result)
    else:
        return pd.DataFrame()


def n_cpus_input(**mp_args):
    if 'n_cpus' not in mp_args:
        'Falta el número de CPU'
        sys.exit(0)
    else:
        n_cpus = mp_args['n_cpus']

    if isinstance(n_cpus, int) is False:
        print('número inválido de CPUs: ' + str(n_cpus))
        sys.exit(0)
    return n_cpus


def df_grouper(df, gb_cols, n_groups):
    """
    df: df para procesar.
    gb_cols: cols para realizar el group_by. Si gb_cols = [], no hay group_by, es decir, realiza directamente df.apply (..., axis = 1)
    n_groups: número de grupos a utilizar.
    Devuelve: un dict con n_groups claves 0, .., n_groups - 1 con:
             - dict [group_nbr] = [..., (start_df, len_df), ...]
             - start_df: primer índice en uno de los DF del grupo y len_df: LENGTH del df.
    """
    # preparar el dictado de índice para iterar
    idx_dict = dict()
    if len(gb_cols) > 0:  # tradicional group_by + apply
        srt_df = df.sort_values(by=gb_cols)
        g = srt_df.groupby(gb_cols)
        idx_dict = {np.min(v): len(v) for v in g.indices.values()}
    else:  # plain apply
        df.reset_index(inplace=True, drop=True)
        sz, r = divmod(len(df), n_groups)
        start = sz + r
        idx_dict[0] = start
        for _ in range(1, n_groups):
            idx_dict[start] = sz
            start += sz

    groups = mp_balancer(idx_dict.keys(), n_groups, idx_dict.values())  # groups[k] = [tid1, tid2, ...]
    df_grp = {g: [(s, idx_dict[s]) for s in groups[g]] for g in groups}  # df_grp[k] = [..., (idx_start, len), ...]
    return df_grp


def func_by_groups(key, group_dict, func, *args):
    """
    Implementa la versión group_by de la función func
    : param key: clave de grupo
    : param group_dict: group_dict [key] = [..., (df_start, df_len), ...]
    : param func: función a ejecutar por grupo
    : param df: DF arg para func
    : param args: args para func, excluyendo df
    : return: DF concat de la función aplicada a cada groupby en group_dict [key]
    """
    df = args[-1]
    args = args[:-1]  # crea un nuevo argumento dejando caer el df
    d_list = []
    for df_start, df_len in group_dict[key]:
        d = func(df.iloc[df_start:(df_start + df_len)], *args)
        if len(d) > 0:
            d_list.append(d)
    if len(d_list) > 0:
        return pd.concat(d_list)
    else:
        return pd.DataFrame()


def mp_balancer(task_list, n_groups, weights=None):
    """
    Equilibre la lista de tareas (lista de claves de tareas o ID de tareas o índices de tareas) en n_grupos para que la cantidad de trabajo sea aproximadamente la misma en cada lista.
    lista de tareas:
    weights: es una lista de pesos de tareas para guiar el equilibrio. Cuanto mayor sea el peso, más procesamiento necesitará.
    n_groups: cuántos grupos de procesamiento devolver
    return: diccionario con claves n_groups. grupos [k] = lista de tareas para el grupo k

    Balance the task_list (list of task keys or task ids or task indices) into n_groups so that the amount of work is about the same in each list.
    task_list:
    weights: is a list of task weights to guide the balancing. The larger the weight the more processing needed.
    n_groups: how many processing groups to return

    return: dictionary with n_groups keys. groups[k] = task list for group k
    """
    if weights is None:
        weights = [1] * len(task_list)

    t_list = zip(task_list, weights)  # (tid, w_tid) list
    s_list = sorted(t_list, key=itemgetter(1), reverse=True)  # ordenar en peso descendente  [(tid, weight), ...]
    groups = {k: [] for k in range(n_groups)}  # groups[k] = [(cid1, w1), (cid2, w2), ...]  list of tasks in group k
    w_dict = {k: 0 for k in range(n_groups)}  # peso acumulado de cada grupo

    next_g = 0
    for s in s_list:
        groups[next_g].append(s[0])  # s = (tid, weight)
        w_dict[next_g] += s[1]
        # asignar el tid más alto disponible al grupo con el peso acumulativo más bajo
        next_g = min(w_dict.items(), key=itemgetter(1))[0]
    return groups  # groups[k] = [tid1, tid2, ...]


def simple_parallel(function, n_procs=None):
    """
    Inputs
    ======
    function: la función que se paralelizará. El primero
        El argumento debe ser el que se va a iterar (en paralelo). El otro
        los argumentos son los mismos en todas las ejecuciones paralelas de la función.
    n_procs: int, el número de procesos a ejecutar. El valor predeterminado es Ninguno.
        Se pasa a multiprocessing.Pool (ver eso para más detalles).

    Output
    ======
    Una función paralelizada. NO LO NOMBRE IGUAL QUE LA FUNCIÓN DE ENTRADA.

    Ejemplo
    =======
     def _square_and_offset(value, offset=0):
        return value**2 + offset

    pparallel_square_and_offset = simple_parallel(_square_and_offset,n_procs=5)
    print parallel_square_and_offset(range(10), offset=3)
    > [3, 4, 7, 12, 19, 28, 39, 52, 67, 84]
    """

    def apply(iterable_values, *args, **kwargs):
        args = list(args)
        p = mp.Pool(n_procs)
        result = [p.apply_async(function, args=[value] + args, kwds=kwargs) for value in iterable_values]
        p.close()  # no use p.join ya que serializa los procesos!
        try:
            return [r.get() for r in result]
        except KeyError:
            return []

    return apply
