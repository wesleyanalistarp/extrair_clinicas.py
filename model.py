import sqlite3

DB = "usuarios.db"

def criar_tabela():
    conn = sqlite3.connect(DB)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT UNIQUE,
        password TEXT
    )
    """)

    conn.commit()
    conn.close()

if __name__ == "__main__":
    criar_tabela()
    print("✅ Banco de usuários criado!")