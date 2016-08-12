from collections import Counter

def sequence_distribution(sequence):
    '''
    '''
    lenseq = len(sequence)
    return [[v,c,round(float(c)/len(sequence),5)] for v,c in Counter(sequence).most_common()]

def sequence_distribution_next(sequence):
    '''
    '''
    ana = {}
    for each in set(sequence):
        ana[each] = []

    for i,v in enumerate(sequence):
        if i:
            ana[sequence[i-1]].append(v)

    for k,v in ana.items():
        ana[k] = [[vv,c,p] for vv,c,p in sequence_distribution(v)]

    return ana

def pair_distribution_next(pairs):
    '''
    *pairs* - [['a',1], ['b',3], ['a',2], ['c',4], ['b',1]]

    Return: {'a': [[3, 1, 0.5], [4, 1, 0.5]], 'c': [[1, 1, 1.0]], 'b': [[2, 1, 1.0]]}
    '''
    ana = {}
    for each in set([m for m, n in pairs]):
        ana[each] = []

    for i, pair in enumerate(pairs):
        if i:
            ana[pairs[i-1][0]].append(pair[1])

    for k,v in ana.items():
        ana[k] = [[vv,c,p] for vv,c,p in sequence_distribution(v)]

    return ana

def ana_results(results):
    '''
    *results*   - a list of results, e.g.: ['^_^', 'T_T', '^_^', ...]
    Return all_count, succeed_count, failed_count, percentage(float).
    '''
    all_count = len(results)
    succeed_count = results.count('^_^')
    failed_count = results.count('T_T')
    percentage = float(succeed_count)/all_count

    return all_count, succeed_count, failed_count, percentage


if __name__ == '__main__':
    sample = ['a','b','a','b','c','b','e']
    print sequence_distribution(sample)

    ana = sequence_distribution_next(sample)
    for k,v in ana.items():
        print 'Key: {0}'.format(k)
        for vv,c,p in v:
            print '\t', vv,c,p


    print ana_results(['^_^', 'T_T', '^_^', '^_^'])

    print pair_distribution_next([['a',1], ['b',3], ['a',2], ['c',4], ['b',1]])