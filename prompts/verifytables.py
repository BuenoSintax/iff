import duckdb

def list_tables():
    # Conecta ao banco DuckDB; ajuste 'mydatabase.duckdb' conforme necessário ou use ':memory:'
    con = duckdb.connect('mydatabase.duckdb')
    
    # Executa a query para obter os nomes das tabelas
    tables = con.execute("SELECT table_name FROM information_schema.tables").fetchall()

    con.close()
    
    # Extrai os nomes (cada linha é uma tupla com um elemento)
    return [t[0] for t in tables]
    
if __name__ == "__main__":
    tables = list_tables()
    print("Tabelas existentes no DuckDB:")
    for table in tables:
        print(table)