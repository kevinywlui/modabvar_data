import sqlite3

@parallel
def compute_data_at_level(N):
    print(N)
    for i, f in enumerate(Newforms(N, names='a')):
        for j in range(f.base_ring().degree()):
            label = str(N) + chr(ord('a')+i) + str(j)
            L = f.lseries(embedding = j)
            T = L.taylor_series(a = 1, k = 14)
            C = T.padded_list()
            taylor_coefficients = C
            rank = next(n for n, x in enumerate(C) if abs(x) > 10e-10)

            conn = sqlite3.connect('modabvar_data.db', timeout=600000)
            conn.execute('PRAGMA journal_mode=wal')
            c = conn.cursor()
            c.execute("INSERT INTO mf VALUES ('{}', '{}', {})" \
                    .format(label,
                        taylor_coefficients,
                        rank))
            conn.commit()
            conn.close()

conn = sqlite3.connect('modabvar_data.db', timeout=600000)
conn.execute('PRAGMA journal_mode=wal')

c = conn.cursor()
c.execute('DROP TABLE if exists mf')
c.execute('''CREATE TABLE mf
        (label TEXT,
        taylor_coefficients TEXT,
        rank INT)''')
conn.commit()
conn.close()

Ns = [1..100]
list(compute_data_at_level(Ns))
# for N in Ns:
#     compute_data_at_level(N)
        
