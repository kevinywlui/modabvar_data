import sqlite3

# @parallel
def compute_data_at_level(N):
    print(N)
    for i, f in enumerate(Newforms(N, names='a')):
        for j in range(f.base_ring().degree()):
            label = str(N) + chr(ord('a')+i)
            embedding = j
            L = f.lseries(embedding = j)
            T = L.taylor_series(a = 1, k = 14)
            C = T.padded_list()
            taylor_coefficients = C
            rank = next(n for n, x in enumerate(C) if abs(x) > 10e-10)

            # c.execute("INSERT INTO mf VALUES ('{}', {}, '{}', {})" \
            #         .format(label,
            #             embedding,
            #             taylor_coefficients,
            #             rank))

# conn = sqlite3.connect('modabvar_data.db')

# c = conn.cursor()
# c.execute('DROP TABLE if exists mf')
# c.execute('''CREATE TABLE mf
#         (label TEXT,
#         embedding INT,
#         taylor_coefficients TEXT,
#         rank INT);''')

Ns = (N for N in [1..500] if Newforms(N, names='a'))
# compute_data_at_level(Ns)
for N in Ns:
    compute_data_at_level(N)
        
# conn.commit()
# conn.close()
