import numpy as np
import itertools as itt

# Источник для CPCV - de Prado Advances in Financial Machine Learning

def cpcv_generator(t_span, n, k):
    # 1. разбиваем моменты времени на n групп
    group_num = np.arange(t_span) // (t_span // n)
    group_num[group_num == n] = n-1

    # 2. считаем количество всевозможных симуляций и путей
    test_groups = np.array(list(itt.combinations(np.arange(n), k))).reshape(-1, k)
    C_nk = len(test_groups)
    n_paths = C_nk * k // n

    # 3. отмечаем моменты времени и группы как тестовые
    is_test_group = np.full((n, C_nk), fill_value=False)
    is_test = np.full((t_span, C_nk), fill_value=False)

    for sim_i, comb in enumerate(test_groups):
        is_test_group[comb, sim_i] = True # группа тестовая в симуляции sim_i

        mask = np.isin(group_num, comb)
        is_test[mask, sim_i] = True

    # 4. для каждого пути отмечаем из каких групп он состоит
    path_folds = np.full((n, n_paths), fill_value=np.nan) # всемозможные пути тестирования

    # для каждой группы находим симуляцию с минимальным номером,
    # в которой она тестовая
    for i in range(n_paths):
        for j in range(n):
            sim_i = is_test_group[j, :].argmax().astype(int)
            path_folds[j, i] = sim_i
            is_test_group[j, sim_i] = False

    # 5. для каждого пути отмечаем из каких моментов времени он состоит
    paths = np.full((t_span, n_paths), fill_value=np.nan)

    for p in range(n_paths):
        for i in range(n):
            mask = (group_num == i)
            paths[mask, p] = int(path_folds[i, p])

    return (is_test, paths, path_folds)


