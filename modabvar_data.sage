import sqlite3

@parallel
def compute_data_at_level(N):
    print(N)
    for i, f in enumerate(Newforms(N, names='a')):
        A = AbelianVariety(f)

        # compute the label
        label = str(N) + chr(ord('a')+i)

        # compute the dimension
        dim = f.base_ring().degree()

        # compute the q-exp
        q_exp = f.q_expansion()

        # compute the order of rational torsion subgroup
        T = A.rational_torsion_subgroup()
        lower_bound = T.divisor_of_order()
        upper_bound = T.multiple_of_order()
        if lower_bound == upper_bound:
            order_of_torsion = lower_bound
        else:
            order_of_torsion = '?'

        # compute the analytic rank
        rank = 0
        for j in range(dim):
            L = f.lseries(embedding = j)
            T = L.taylor_series(a = 1, k = 14)
            C = T.padded_list()
            rank_f = next(n for n, x in enumerate(C) if abs(x) > 10e-10)
            rank += rank_f

        conn = sqlite3.connect('modabvar_data.db')
        c = conn.cursor()
        c.execute("INSERT INTO abvar VALUES ({}, {}, {}, {}, {}, {}, {}" \
                .format(label,
                        dim,
                        q_exp,
                        lower_bound,
                        upper_bound,
                        order_of_torsion,
                        analytic_rank))

        conn.commit()
        conn.close()

conn = sqlite3.connect('modabvar_data.db')

c = conn.cursor()
c.execute('DROP TABLE if exists abvar')
c.execute('''CREATE TABLE abvar (
    label TEXT,
    dimension TEXT,
    q_expansion TEXT,
    lower_bound TEXT,
    upper_bound TEXT,
    order_of_torsion TEXT,
    analytic_rank TEXT
)''')

conn.commit()
conn.close()

Ns = [1..100]
list(compute_data_at_level(Ns))
