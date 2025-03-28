import pandas as pd
import psycopg2 as pg

CSV_NAME = 'grupo_economico.csv'
TABLE_NAME = 'cadastro.grupo_economico' #'cadastro.empresa'

class Csv_to_postgres:

    def __init__(self):
        query = self.make_query()
        self.save_data(query)

    def save_data(self, query):
        conn = pg.connect(**{
        'dbname': 'operations_db',
        'user': 'postgres',
        'password': 'postgres',
        'port': 5432,
        'host': 'localhost'
        })
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
        cur.execute("SELECT * FROM cadastro.empresa;")



    def make_query(self):
        df = pd.read_csv(CSV_NAME)
        columns = tuple(df.columns[:-1])
        
        itens = [] 
        
        for i in df.itertuples(index=False):
            row = i[:-1]
            item = []
            for r in row:
            
                if type(r) == str:
                    a = r.replace("'", '').replace('"', '')
                else:
                    a = r
                item.append(a)
            itens.append(tuple(item))


        return f"""
            INSERT INTO {TABLE_NAME} {str(columns).replace("'", '"')} \n VALUES 
            \n {
                str(itens).replace('[', '')
                .replace(']', '')
                .replace('None', 'null')
                .replace('nan', 'null')
                .replace('"', "'")}

            ON CONFLICT ("id") DO NOTHING
        """

Csv_to_postgres()