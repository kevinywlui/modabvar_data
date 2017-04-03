import psycopg2
import sys

@parallel
def compute_data_at_level(N):
    print(N)
    for i, f in enumerate(Newforms(N, names='a')):
        A = AbelianVariety(f)
        L = A.lseries()

        # compute the label
        label = str(N) + chr(ord('a')+i)

        # compute the level
        level = N

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
        T = f.lseries().taylor_series(a=1, k=14)
        C = T.padded_list()
        rank_f, leading_coeff = next((n, x) for n, x in enumerate(C)
                if abs(x) > 10e-10)

        analytic_rank = dim * rank_f

        conn = psycopg2.connect("dbname='modabvar' host='localhost'")
        c = conn.cursor()
        c.execute("INSERT INTO Af VALUES ('{}', {}, {}, '{}', {}, {}, '{}', {}, {}, {})" \
                .format(label,
                    level,
                    dim,
                    q_exp,
                    lower_bound,
                    upper_bound,
                    order_of_torsion,
                    l1,
                    analytic_rank,
                    leading_coeff
                    ))

        conn.commit()
        conn.close()

conn = psycopg2.connect("dbname='modabvar' host='localhost'")

c = conn.cursor()
c.execute("DROP TABLE if exists Af")
c.execute('''CREATE TABLE Af (
    label TEXT,
    level INT,
    dimension INT,
    q_expansion TEXT,
    lower_bound INT,
    upper_bound INT,
    order_of_torsion TEXT,
    l1 REAL,
    analytic_rank INT,
    leading_coeff DOUBLE PRECISION
)''')

conn.commit()
conn.close()

try:
    M = int(sys.argv[1])
except:
    M = 100

Ns = [1..M]
list(compute_data_at_level(Ns))
