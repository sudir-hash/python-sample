
'''
Created on 
@author: rajacsp.raman
source:
    https://www.psycopg.org/docs/extras.html#psycopg2.extras.Json

    https://stackoverflow.com/questions/10252247/how-do-i-get-a-list-of-column-names-from-a-psycopg2-cursor

    https://github.com/rajasgs/python-postgres-vanilla-crud/blob/main/crud.py
'''

import psycopg2
import json
# from psycopg2.extras import Json

def sayHi():

    return "Whats up"


def add(conn, name, state):

    '''
        CNAME, STATE
    '''

    cur = conn.cursor()
    
    sc_id = -1
    try:
        cur.execute("INSERT INTO CITY (CNAME, STATE) VALUES (%s, %s) RETURNING CITYID", (name, state))        
        
        sc_id = cur.fetchone()[0]
    except psycopg2.IntegrityError as sqle:
        print("SQLite error : {0}".format(sqle))
    finally:
        if cur is not None:
            cur.close()
    
    return sc_id



def update(conn, id, state):

    cur = conn.cursor()

    c_sql = f'''
    WITH rows AS (
        UPDATE CITY
        SET STATE = '{state}'
        WHERE CITYID = '{id}'
        RETURNING 1
    )
    SELECT count(*) FROM rows;
    '''
    
    sc_id = -1
    try:
        cur.execute(c_sql)        
        
        sc_id = cur.fetchone()[0]
    except psycopg2.IntegrityError as sqle:
        print("SQLite error : {0}".format(sqle))
    finally:
        if cur is not None:
            cur.close()
    
    return sc_id

def delete(conn, id):

    cur = conn.cursor()

    c_sql = f'''
    WITH rows AS (
        DELETE FROM CITY
        WHERE CITYID = '{id}'
        RETURNING 1
    )
    SELECT count(*) FROM rows;
    '''
    
    sc_id = -1
    try:
        cur.execute(c_sql)        
        
        sc_id = cur.fetchone()[0]
    except psycopg2.IntegrityError as sqle:
        print("SQLite error : {0}".format(sqle))
    finally:
        if cur is not None:
            cur.close()
    
    return sc_id



def create_connection():
    
    hostname = '127.0.0.1'
    username = 'postgres'
    password = 'kaipulla'

    database = 'postgres'
    
    try:
        conn = psycopg2.connect( host=hostname, user=username, password=password, dbname=database )
        conn.autocommit = True
        return conn
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
 
    return None


def select_all(conn):
    
    cur = conn.cursor()
    cur.execute("SELECT CITYID, CNAME, STATE FROM CITY")
 
    rows = cur.fetchall()
    
    print('rows count : '+str(len(rows)))
    
    if(len(rows) <= 0):
        print('No Data available')
        return
 
    for row in rows:
        print(row)  


def startpy():

    # create a database connection
    conn = create_connection()

    # CREATE
    # print('Create City')
    # id = add(conn, "Unjapalayam", "Nammakal")
    # print(f"id : {id}")
    
    # up = update(conn,8,"Namakkal")

    remove  = delete(conn,5)
    
    select_all(conn)
    

if __name__ == '__main__':
    startpy()

    # print("Sudhir ")