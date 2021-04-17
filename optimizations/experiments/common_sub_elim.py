import arkouda as ak


def complex_one(a):
    """
    Problem: Does not remember a * a which can be a reusable temporary
    """
    # Creates 3 total temporaries
    # Calculates a * a twice
    (a * a) + (a * a)


def complex_two(a, b):
    """
    Problem: Does not remember a * a which can be a reusable temporary
    """
    # Creates two temporaries and destroys the a * a temporary
    c = a * a + b
    # Creates three temporaries, computes a * a again
    d = (a * a) + (b * b)


def sort_one(a):
    """
    Problem: Does not remember permutation of a, which can be stored
    as a temporary.
    """
    # Creates temporary and deletes it immediately
    ak.argsort(a)
    # Creates temporary again and deletes it immediately
    ak.argsort(a)


def sort_two(a):
    """
    Problem: Does not realize that adding a to itself
    does not influence relative ordering of a.
    """
    # Creates a temporary for a + a
    # Creates a temporary for sorted perm of a + a
    # Deletes both
    ak.argsort(a + a)


def sort_three(a, b):
    """
    Problem: Does not use the fact that a and b have
    previously been sorted.
    """
    # Creates and deletes temporary
    ak.argsort(a)
    # Creates and deletes temporary
    ak.argsort(b)
    # Creates and deletes two temporaries
    ak.argsort(ak.concatenate([a, b]))


ak.connect(connect_url='tcp://MacBook-Pro.local:5555')
A = ak.randint(0, 10000, 10000)
B = ak.randint(0, 10000, 10000)
A + B
sort_three(A, B)
