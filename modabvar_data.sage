import sqlite3
conn = sqlite3.connect('modabvar_data.db')

c = conn.cursor()
c.execute('DROP TABLE if exists modabvar')
c.execute('''CREATE TABLE modabvar
        (label  TEXT,
        d   INT)''')

# very inefficient for now
Ns = [N for N in [1..30] if J0(N).dimension() > 0]
for N in Ns:
    print(N)
    for A in J0(N):
        label = A.newform_label()
        d = A.dimension()
        
        c.execute("INSERT INTO modabvar VALUES ('{}', {})".format(label, d))

conn.commit()
conn.close()
