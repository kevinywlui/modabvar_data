import sqlite3
import sys

@parallel
def compute_data_at_level(N):
    print(N)
    for i, f in enumerate(Newforms(N, names='a')):
        A = AbelianVariety(f)
        L = A.lseries() 

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

        # compute L(A_f,1)
        l1 = L(1)

        # compute the analytic rank
        T = f.lseries().taylor_series(a = 1, k = 14)
        C = T.padded_list()
        rank_f = next(n for n, x in enumerate(C) if abs(x) > 0)
        analytic_rank = dim * rank_f
        # analytic_rank = 0
        # for j in range(dim):
        #     L = f.lseries(embedding = j)
        #     T = L.taylor_series(a = 1, k = 14)
        #     C = T.padded_list()
        #     rank_f = next(n for n, x in enumerate(C) if abs(x) > 10e-10)
        #     analytic_rank += rank_f

        conn = sqlite3.connect('modabvar_data.db')
        c = conn.cursor()
        c.execute("INSERT INTO abvar VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')" \
                .format(label,
                    dim,
                    q_exp,
                    lower_bound,
                    upper_bound,
                    order_of_torsion,
                    l1,
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
    l1 TEXT,
    analytic_rank TEXT
)''')

conn.commit()
conn.close()

try:
    M = int(sys.argv[1])
except:
    M = 100

Ns = [1..M]
list(compute_data_at_level(Ns))
