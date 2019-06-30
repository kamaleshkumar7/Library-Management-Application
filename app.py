import pandas as pd
import time
import multiprocessing as mp 
import threading 
import mysql.connector
from flask import Flask, render_template, request,Response,jsonify

app = Flask(__name__)

global q,s 
s = time.time()
q = mp.JoinableQueue() 

def writetosql():
    while not q.empty():
        try:
            #print(threading.enumerate())
	    print("write starts")
            t = q.get()
	    mydb = mysql.connector.connect(host='localhost',user='root',passwd='',db='dbname')
	    query_string = "INSERT INTO books (name,author,count) VALUES (%s, %s,%s)ON DUPLICATE KEY UPDATE count = count +VALUES(count);"
	    cursor = mydb.cursor()
	    cursor.executemany(query_string,t.values.tolist())
            mydb.commit()
            cursor.close()
            print("write ends")
            print(time.time()-s)
        except mysql.connector.Error as error :
            print(error)
            print("deadlock")
            q.put(t)
            mydb.rollback()

def readfun(filepath):
	for chunk_data in pd.read_csv(filepath,chunksize=10000,header=None):
		print("read")
		q.put(chunk_data)


@app.route('/', methods=['GET', 'POST'])
def index0():
    return render_template('index.html')

@app.route('/insert', methods=['GET', 'POST'])
def index():
    if request.method == "POST":
        details = request.form
        filepath = details['addbooks']
    	t1 = threading.Thread(target = readfun,args=(filepath,))
	t1.start()
	while q.empty():
            pass
        for i in range(10): 
           t2 = threading.Thread(target = writetosql)
	   t2.start()
        
    return render_template('insert.html')
    


@app.route('/select', methods=['GET', 'POST'])
def index1():
    if request.method == "POST":
        details = request.form
        bookname = details['bname']
        qval = details['books']

        if qval == "Book Name":
            s = time.time()
            mydb = mysql.connector.connect(host='localhost',user='root',passwd='',db='dbname')
            cur = mydb.cursor()
            cur.execute("SELECT * FROM books where name = %s;",[bookname])
            data = cur.fetchall()
            mydb.commit()
            cur.close()
            e = time.time()
            return render_template('select.html',result=data,qval =qval)

        elif qval == "Author Name":
            s = time.time()
            mydb = mysql.connector.connect(host='localhost',user='root',passwd='',db='dbname')
            cur = mydb.cursor()
            cur.execute("SELECT * FROM books where author = %s;",[bookname])
            data = cur.fetchall()
            mydb.commit()
            cur.close()
            e = time.time()
            return render_template('select.html',result=data,qval =qval)
      
        
    return render_template('select.html')

if __name__ == '__main__':
    app.run(debug=True ,threaded=True)

