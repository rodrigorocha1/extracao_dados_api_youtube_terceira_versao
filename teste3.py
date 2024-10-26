def somar(**kwargs):
    a = kwargs['a']
    b = kwargs['b']
    print(a, b)


d = {
    'a': 1,
    'b': 2
}
somar(**d)
