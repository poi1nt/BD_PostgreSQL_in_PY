import psycopg2
from psycopg2.sql import SQL, Identifier

def create_db(conn):
    cur.execute('''
    CREATE TABLE IF NOT EXISTS client (
        client_id SERIAL PRIMARY KEY,
        first_name VARCHAR (60) NOT NULL,
        last_name VARCHAR (60) NOT NULL,
        email VARCHAR (100));
        ''')

    cur.execute('''
    CREATE TABLE IF NOT EXISTS phone (
        phone_id SERIAL PRIMARY KEY,
        phone BIGINT,
        client_id integer not null references client(client_id));
        ''')
         
    conn.commit()

def add_new_client(conn, first_name, last_name, email, phone=None): 
    cur.execute('''
        INSERT INTO client (first_name, last_name, email)
        VALUES(%s, %s, %s)
        RETURNING client_id;  
        ''', (first_name, last_name, email))
    client_id = cur.fetchone()

    if phone != None:
        cur.execute('''
            INSERT INTO phone(phone, client_id)
            VALUES(%s, %s);
            ''', (phone, client_id))
    else:
        cur.execute('''
            INSERT INTO phone(phone, client_id)
            VALUES(%s, %s);
            ''', (phone, client_id))

    conn.commit()

def add_number_phone(conn, client_id, phone):
    cur.execute('''
        SELECT * FROM phone
        WHERE client_id = %s;
    ''',(client_id))
    if cur.fetchone() is None:
        print('Клиента с таким id нет.')
    else:
        cur.execute('''
        SELECT * FROM phone
        WHERE phone = %s;
    ''',(phone))
        if cur.fetchone() is None:
            cur.execute('''
                INSERT INTO phone(phone, client_id)
                VALUES(%s, %s);
                ''', (phone, client_id))
        else:
            print(f'Номер уже существует')
    
    conn.commit()

def change_client_data(conn, client_id, first_name=None, last_name=None, email=None, phone=None):
    if first_name is not None:
        cur.execute('''
        UPDATE client SET first_name=%s WHERE client_id=%s;
        ''', (first_name, client_id)
        )
    
    if last_name is not None:
        cur.execute('''
        UPDATE client SET last_name=%s WHERE client_id=%s;
        ''', (last_name, client_id)
        )

    if email is not None:
        cur.execute('''
        UPDATE client SET email=%s WHERE client_id=%s;
        ''', (email, client_id)
        )
    
    if phone is not None:
        cur.execute('''
        UPDATE phone SET phone=%s WHERE client_id=%s;
        ''', (phone, client_id)
        )
    
    conn.commit()

def delete_number_phone(conn, client_id, phone):
    cur.execute('''
        SELECT * FROM phone
        WHERE client_id = %s;
    ''',(client_id))
    if cur.fetchone() is None:
        print('Клиента с таким id нет.')
    else:
        cur.execute('''
        SELECT * FROM phone
        WHERE phone = %s;
    ''',(phone))
        if cur.fetchone() is not None:
            cur.execute('''
                DELETE FROM phone
                WHERE phone = %s AND client_id = %s;
                ''', (phone, client_id))
        else:
            print(f'Такого номера нет в базе данных существует')
    
    conn.commit()

def delete_client(conn, client_id):
    cur.execute('''
        SELECT * FROM client
        WHERE client_id = %s;
    ''',(client_id))
    if cur.fetchone() is None:
        print('Клиента с таким id нет.')
    else:
        cur.execute('''
            DELETE FROM phone
            WHERE client_id = %s;
            ''', (client_id))

        cur.execute('''
            DELETE FROM client
            WHERE client_id = %s;
            ''', (client_id))
        
    conn.commit()

def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    arg_list ={'first_name':first_name, 'last_name':last_name, 'email':email, 'phone':phone}
    for key, arg in arg_list.items():
        if arg:
            cur.execute(SQL("""
            SELECT * FROM client
            JOIN phone ON client.Client_id=phone.Client_id
            WHERE {} = %s;
            """).format(Identifier(key)), (arg,))

            print(cur.fetchall())

            conn.commit()

with psycopg2.connect(database="client_management_test", user="postgres", password="135797531") as conn:
    with conn.cursor() as cur:
        cur.execute("""
        DROP TABLE phone;
        DROP TABLE client;
        """)
        conn.commit() 
        
        create_db(conn)
        
        first_name = 'Алексей'
        last_name = 'Захаров'
        email = 'Amail@mail.ru'
        phones = [+79895004326, +79897778326]

        add_new_client(conn, first_name, last_name, email)

        first_name = 'Виктор'
        last_name = 'Федоров'
        email = 'VF@mail.ru'
        phones = [+79998583344]

        add_new_client(conn, first_name, last_name, email)

        add_number_phone(conn, '1', (+79990555500,))

        change_client_data(conn, '2', 'Иван', 'Литвин', 'LIT@mail.ru', (+79583546446,))

        delete_number_phone(conn, '1', (+79990555500,))

        delete_client(conn, '2')
        
        find_client(conn, 'Алексей', 'Amail@mail.ru')

conn.close()