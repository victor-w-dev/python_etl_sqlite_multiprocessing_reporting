from tkinter import Tk, Button,Label,Scrollbar,Listbox,StringVar,Entry,W,E,N,S,END
from tkinter import ttk
from tkinter import messagebox
from BSO import rawdata_newread as readfile
from BSO.time_analysis import time_decorator
import sqlite3 as pyo
import os
import datetime
import time

rawdata_folder="C&SD_raw_data"
db_path = "merchandise_trades_DB"
'''
con = pyo.connect(db_path)
print(con)

cursor = con.cursor()
'''
def read_file(file_handler, table):
    for line in file_handler:
        row = line.strip()
        if table=='hsccit':
            yield [(row[0]),
                   row[1:9],
                   row[9:12],
                   row[12:30],
                   row[30:48],
                   row[48:66],
                   row[66:84],
                   row[84:102],
                   row[102:120],
                   row[120:138],
                   row[138:156],
                   row[156:174],
                   row[174:192],
                   row[192:210],
                   row[210:228]]

        if table=='hscoit':
            yield [(row[0]),
                   row[1:9],
                   row[9:12],
                   row[12:30],
                   row[30:48],
                   row[48:66],
                   row[66:84]]

        if table=='hscoccit':
            yield [(row[0]),
                   row[1:9],
                   row[9:12],
                   row[12:15],
                   row[15:33],
                   row[33:51],
                   row[51:69],
                   row[69:87]]


#@time_decorator
class TradeDB:
    def __init__(self):
        if not os.path.exists(db_path):
            os.makedirs(db_path)
        self.con = pyo.connect(db_path+"/"+"trades.db")
        self.cursor = self.con.cursor()

        create_HSCCIT_query = ("CREATE TABLE IF NOT EXISTS hsccit(ID INTEGER PRIMARY KEY,"
        "TransactionType INTEGER," #1
        "HScode TEXT," #2
        "CountryConsignmentCode INTEGER," #3
        "ImportValueMonthly INTEGER," #4
        "ImportQuantityMonthly INTEGER," #5
        "ImportValueYTD INTEGER," #6
        "ImportQuantityYTD INTEGER," #7
        "DomesticExportValueMonthly INTEGER," #8
        "DomesticExportQuantityMonthly INTEGER," #9
        "DomesticExportValueYTD INTEGER," #10
        "DomesticExportQuantityYTD INTEGER," #11
        "ReExportValueMonthly INTEGER," #12
        "ReExportQuantityMonthly INTEGER," #13
        "ReExportValueYTD INTEGER," #14
        "ReExportQuantityYTD INTEGER," #15
        "ReportPeriod TEXT," #16
        "UpdatedDate TIMESTAMP)" #17
        )

        create_HSCOIT_query = ("CREATE TABLE IF NOT EXISTS hscoit(ID INTEGER PRIMARY KEY,"
        "TransactionType INTEGER," #1
        "HScode TEXT," #2
        "CountryOriginCode INTEGER," #3
        "ImportByOriginValueMonthly INTEGER," #4
        "ImportByOriginQuantityMonthly INTEGER," #5
        "ImportByOriginValueYTD INTEGER," #6
        "ImportByOriginQuantityYTD INTEGER," #7
        "ReportPeriod TEXT," #8
        "UpdatedDate TIMESTAMP)" #9
        )

        create_HSCOCCIT_query = ("CREATE TABLE IF NOT EXISTS hscoccit(ID INTEGER PRIMARY KEY,"
        "TransactionType INTEGER," #1
        "HScode TEXT," #2
        "CountryOriginCode INTEGER," #3
        "CountryDestinationCode INTEGER," #4
        "ReExportValueMonthly INTEGER," #5
        "ReExportQuantityMonthly INTEGER," #6
        "ReExportValueYTD INTEGER," #7
        "ReExportQuantityYTD INTEGER," #8
        "ReportPeriod TEXT," #9
        "UpdatedDate TIMESTAMP)" #10
        )

        self.cursor.execute(create_HSCCIT_query)
        self.cursor.execute(create_HSCOIT_query)
        self.cursor.execute(create_HSCOCCIT_query)

        self.con.commit()
        print("You have connected to the database")
        print(self.con)

    def __del__(self):
        self.con.close()

    def view(self):
        self.cursor.execute("SELECT * FROM books")
        rows = self.cursor.fetchall()
        return rows

    def check_report_period(self,table):
        self.cursor.execute(f"SELECT DISTINCT ReportPeriod FROM {table}")
        rows = self.cursor.fetchall()
        return [i[0] for i in rows]
    #@time_decorator
    def insert_DB(self, table, year, month=12, path=rawdata_folder):

        period = f'{year}{month}'
        try:
            file_path = f'{path}/{period}/{table}.dat'
            print(path)
            open(file_path)
        except:
            file_path = f'{path}/{period}/{table}.txt'
            open(file_path)
            print(f"Import from txt file: {file_path}")
        else:
            print(f"Import from dat file: {file_path}")

        # using readlines methods may be slow, but actually the program need
        #not to use all the column after spliting
        #it will be faster than using pd.read_fwf as this pd.read_fwf function
        #seem need to
        #import all columns
        #print("db instance start")
        with open(file_path, 'r', encoding='utf-8') as file_object:
            for line in read_file(file_object,table):
                #print(type(i))
                line = line + [f'{year}{month}'] + [datetime.datetime.now()]
                #print(line)
                #print(len(line))
                self.insert_line(table,*line)
            self.con.commit()



    def insert_line(self, table, *values):
        if table == "hsccit":
            column_str = """TransactionType, HScode, CountryConsignmentCode,
                            ImportValueMonthly,
                            ImportQuantityMonthly,
                            ImportValueYTD,
                            ImportQuantityYTD,
                            DomesticExportValueMonthly,
                            DomesticExportQuantityMonthly,
                            DomesticExportValueYTD,
                            DomesticExportQuantityYTD,
                            ReExportValueMonthly,
                            ReExportQuantityMonthly,
                            ReExportValueYTD,
                            ReExportQuantityYTD,
                            ReportPeriod,
                            UpdatedDate
                            """
            insert_str = ("?, " * 17)[:-2]

        if table == "hscoit":
            column_str = """TransactionType, HScode,
                            CountryOriginCode,
                            ImportByOriginValueMonthly,
                            ImportByOriginQuantityMonthly,
                            ImportByOriginValueYTD,
                            ImportByOriginQuantityYTD,
                            ReportPeriod,
                            UpdatedDate
                            """
            insert_str = ("?, " * 9)[:-2]

        if table == "hscoccit":
            column_str = """TransactionType, HScode,
                            CountryOriginCode,
                            CountryDestinationCode,
                            ReExportValueMonthly,
                            ReExportQuantityMonthly,
                            ReExportValueYTD,
                            ReExportQuantityYTD,
                            ReportPeriod,
                            UpdatedDate
                            """
            insert_str = ("?, " * 10)[:-2]

        sql=(f"INSERT INTO {table} ({column_str}) VALUES ({insert_str})")
        #print(f"sql query: {sql}")
        #print(f"hhi {len(values)}")
        #print(f"hhi2 {values}")
        #print(f"hhhhh{type(values)}")
        #print(f"hhhhh{values}")
        #values =[title,author,isbn]
        self.cursor.executemany(sql,[values])
        #self.con.commit()
        #print(self.con)
        #con.commit()
        #messagebox.showinfo(title="Book Database",message="New book added to database")

    def update(self, id, title, author, isbn):
        tsql = 'UPDATE books SET  title = ?, author = ?, isbn = ? WHERE id=?'
        self.cursor.execute(tsql, [title,author,isbn,id])
        self.con.commit()
        messagebox.showinfo(title="Book Database",message="Book Updated")

    def delete(self, id):
        delquery ='DELETE FROM books WHERE id = ?'
        self.cursor.execute(delquery, [id])
        self.con.commit()
        messagebox.showinfo(title="Book Database",message="Book Deleted")

#db = TradeDB()
#a = list(readfile.get_hsccit(2018, month='12'))
#print(a)
#print(f"final {a}")



"""
def get_selected_row(event):
    global selected_tuple
    index = list_bx.curselection()[0]
    selected_tuple = list_bx.get(index)
    title_entry.delete(0, 'end')
    title_entry.insert('end', selected_tuple[1])
    author_entry.delete(0, 'end')
    author_entry.insert('end', selected_tuple[2])
    isbn_entry.delete(0, 'end')
    isbn_entry.insert('end', selected_tuple[3])

def view_records():
    list_bx.delete(0, 'end')
    for row in db.view():
        #print(row)
        list_bx.insert('end', row)

def add_book():
    db.insert(title_text.get(),author_text.get(),isbn_text.get())
    list_bx.delete(0, 'end')
    list_bx.insert('end', (title_text.get(), author_text.get(), isbn_text.get()))
    title_entry.delete(0, "end") # Clears input after inserting
    author_entry.delete(0, "end")
    isbn_entry.delete(0, "end")
    con.commit()

def delete_records():
    db.delete(selected_tuple[0])
    con.commit()

def clear_screen():
    list_bx.delete(0,'end')
    title_entry.delete(0,'end')
    author_entry.delete(0,'end')
    isbn_entry.delete(0,'end')

def update_records():
    db.update(selected_tuple[0], title_text.get(), author_text.get(), isbn_text.get())
    title_entry.delete(0, "end") # Clears input after inserting
    author_entry.delete(0, "end")
    isbn_entry.delete(0, "end")
    con.commit()

def on_closing():
    dd = db
    if messagebox.askokcancel("Quit", "Do you want to quit?"):
        root.destroy()
        del dd


root = Tk()  # Creates application window

root.title("Business Statistics Offline Enhanced V1.0") # Adds a title to application window
root.configure(background="gray25")  # Add background color to application window
root.geometry("850x500")  # Sets a size for application window
root.resizable(width=False,height=False) # Prevents the application window from resizing

# Create Labels and entry widgets

title_label = ttk.Label(root,text="Title",background="gray25",foreground="white",font=("Calibri", 16, "bold"))
title_label.grid(row=0, column=0, sticky=W)
title_text = StringVar()
title_entry = ttk.Entry(root,width=24,textvariable=title_text)
title_entry.grid(row=0, column=1, sticky=W)

author_label = ttk.Label(root,text="Author",background="gray25",foreground="white",font=("Calibri", 16, "bold"))
author_label.grid(row=0, column=2, sticky=W)
author_text = StringVar()
author_entry = ttk.Entry(root,width=24,textvariable=author_text)
author_entry.grid(row=0, column=3, sticky=W)

isbn_label = ttk.Label(root,text="ISBN",background="gray25",foreground="white",font=("Calibri", 16, "bold"))
isbn_label.grid(row=0, column=4, sticky=W)
isbn_text = StringVar()
isbn_entry = ttk.Entry(root,width=24,textvariable=isbn_text)
isbn_entry.grid(row=0, column=5, sticky=W)

# Add a button to insert inputs into database

add_btn = Button(root, text="Add Book",bg="dim gray",fg="white",font="Calibri 10 bold", command=add_book)
add_btn.grid(row=0, column=6, sticky=W)

# Add  a listbox  to display data from database
list_bx = Listbox(root,height=16,width=40,font="Calibri  13",bg='light gray')
list_bx.grid(row=3,column=1, columnspan=14,sticky=W + E,pady=40,padx=15)
list_bx.bind('<<ListboxSelect>>',get_selected_row)

# Add scrollbar to enable scrolling
scroll_bar = Scrollbar(root)
scroll_bar.grid(row=1,column=8, rowspan=14,sticky=W )

list_bx.configure(yscrollcommand=scroll_bar.set) # Enables vetical scrolling
scroll_bar.configure(command=list_bx.yview)

# Add more Button Widgets

modify_btn=Button(root,text="Modify Record",bg="dim gray",fg="white",font="Calibri 10 bold",command=update_records)
modify_btn.grid(row=15, column=4)

delete_btn=Button(root,text="Delete Record",bg="dim gray",fg="white",font="Calibri 10 bold",command=delete_records)
delete_btn.grid(row=15, column=5)

view_btn=Button(root,text="View Records",bg="dim gray",fg="white",font="Calibri 10 bold",command=view_records)
view_btn.grid(row=15, column=1)#, sticky=tk.N)

clear_btn=Button(root,text="Clear Screen",bg="dim gray",fg="white",font="Calibri 10 bold",command=clear_screen)
clear_btn.grid(row=15, column=2)#, sticky=tk.W)

exit_btn=Button(root,text="Exit Application",bg="dim gray",fg="white",font="Calibri 10 bold",command=root.destroy)
exit_btn.grid(row=15, column=3)

root.mainloop()  # Runs the application until exit
"""

@time_decorator
def importdataDB(dbinstance, exist_periods_dict, startyear=2006, endyear=2020):
    for yr in range(startyear,endyear+1):
        for m in range(1,13):
            for table, existing_periods in exist_periods_dict.items():
                p = f'{yr}{m:02}'
                if p not in existing_periods:
                    try:
                        dbinstance.insert_DB(table,yr,month=f"{m:02}")

                    except FileNotFoundError:
                        print(f"{yr}{m:02} {table} does not exist\n")
                    else:
                        print(f"Successfully imported {yr}{m:02} {table} into DB\n")
                else:
                    print(f"Already had {yr}{m:02} {table} in DB\n")

if __name__ == '__main__':
    start = time.time()
    db = TradeDB()
    #importdataDB()#411s from 2006
    db_exist_periods={'hsccit': db.check_report_period('hsccit'),
                      'hscoit': db.check_report_period('hscoit'),
                      'hscoccit': db.check_report_period('hscoccit')}
    print(db_exist_periods)

    importdataDB(db, db_exist_periods, startyear=2006, endyear=datetime.datetime.today().year)

    end = time.time()
    print(f'used time {end-start:.3f}s')
