# -*- coding: utf-8 -*-

import tkinter as tk
from tkinter import ttk
import tkinter.messagebox
import pyodbc
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from tkinter.filedialog import asksaveasfilename


def disable_entry():
    if var.get() ==1:
        log_entry.config(state= "normal")
        log_entry.delete(0, tk.END)
        log_entry.insert(0,'Login')
        log_entry2.config(state= "normal")
        log_entry2.delete(0, tk.END)
        log_entry2.insert(0,'Password')
    elif var.get() ==2:
        log_entry.config(state= "disable")
        log_entry2.config(state= "disable")
   
def message_server():
    global cursor
    global conn
    global engine
    server = Combo.get()
    database =  my_entry2.get()
    
    if var.get()==1:

        login=log_entry.get()
        password=log_entry2.get()
        conn_str = ("Driver={SQL Server};"
                    f"Server={server};"
                    f"Database={database};"
                    f"uid={login};"
                    f"pwd={password}")
        
    else:
        conn_str = ("Driver={SQL Server};"
                    f"Server={server};"
                    f"Database={database};"
                    "Trusted_Connection=yes;")
    try:

        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        connection_url = URL.create(
            "mssql+pyodbc", 
            query={"odbc_connect": conn_str}
        )
        engine = create_engine(connection_url)
        
        tkinter.messagebox.showinfo(message=f"Connect with Database { Combo.get() } {my_entry2.get()}")
    except:
        tkinter.messagebox.showerror(message=f"Disconnect with Database { Combo.get() } {my_entry2.get()}")
def del_name():
    cursor.execute("""update ObjTbl set ObjTbl.number = NULL from ObjTbl join TAB_IN on ObjTbl.ID = TAB_IN.obj_id where status=1 and number='temp' """)
    conn.commit()
    tkinter.messagebox.showinfo(message=f"Delate name of Mark")

def obj_string(tabela,df):
    
    open_tab = pd.read_sql(f"SELECT * FROM {tabela} ", conn)
    ObjTbl =  pd.read_sql("SELECT * FROM [ObjTbl]  ", conn)
    rowVTbl = pd.read_sql("SELECT * FROM [rowVTbl]  ", conn)
    MarkVTbl = pd.read_sql("SELECT * FROM [MarkVTbl]  ", conn)
    TAB_IN = pd.read_sql("SELECT * FROM [TAB_IN]  ", conn)
    TAB_IN = TAB_IN [TAB_IN[['ATTRIB1','ATTRIB2','ATTRIB3']].isnull().any(1)]
    
    type_obj = pd.merge(open_tab.add_suffix('_p'),ObjTbl.add_suffix('_o'), left_on='obj_id_p', right_on='ID_o' , suffixes=("_p","_o"))
    type_obj_row= pd.merge(type_obj,rowVTbl.add_suffix('_l'),left_on='obj_id_p', right_on='obj_id_l' )
    type_obj_li_v= pd.merge(type_obj_row,MarkVTbl.add_suffix('_Mark'),left_on='v_ID_l', right_on='v_ID_Mark' )
    type_obj_li_v_in= pd.merge(type_obj_li_v, TAB_IN.add_suffix('_in'),left_on='obj_id_Mark', right_on='obj_id_in')
    type_obj_li_v_in_obj= pd.merge(type_obj_li_v_in, ObjTbl.add_suffix('_o2'),left_on='obj_id_in', right_on='ID_o2')
    
    
    df=df.append(type_obj_li_v_in_obj)
    return(df)

def ob_space(tabela,df):
    
    open_tab = pd.read_sql(f"SELECT * FROM {tabela} ", conn)
    ObjTbl =  pd.read_sql("SELECT * FROM [ObjTbl]  ", conn)
    SpaceVTbl = pd.read_sql("SELECT * FROM [SpaceVTbl]  ", conn)
    MarkVTbl = pd.read_sql("SELECT * FROM [MarkVTbl]  ", conn)
    TAB_IN = pd.read_sql("SELECT * FROM [TAB_IN]  ", conn)
    TAB_IN = TAB_IN [TAB_IN[['ATTRIB1','ATTRIB2','ATTRIB3']].isnull().any(1)]
    
    
    type_obj = pd.merge(open_tab.add_suffix('_p'),ObjTbl.add_suffix('_o'), left_on='obj_id_p', right_on='ID_o' , suffixes=("_p","_o"))
    type_obj_row= pd.merge(type_obj,SpaceVTbl.add_suffix('_l'),left_on='obj_id_p', right_on='obj_id_l' )
    type_obj_li_v= pd.merge(type_obj_row,MarkVTbl.add_suffix('_Mark'),left_on='v_ID_l', right_on='v_ID_Mark' )
    type_obj_li_v_in= pd.merge(type_obj_li_v, TAB_IN.add_suffix('_in'),left_on='obj_id_Mark', right_on='obj_id_in')
    type_obj_li_v_in_obj= pd.merge(type_obj_li_v_in, ObjTbl.add_suffix('_o2'),left_on='obj_id_in', right_on='ID_o2')
    
    df=df.append(type_obj_li_v_in_obj)
    return(df)

def the_newest_data (group_data, pkt_id, nr, ATTRIB1, ATTRIB2, ATTRIB3,z):
  a=0
  
  for i, j in group_data:
      pkt_id.append(i)
      nr.append(j.obj_id_Mark.iloc[0])
      if len(set(list(j.ATTRIB1_p)))>1:
              if ((46 in list(j.ATTRIB1_p)) or (1159 in list(j.ATTRIB1_p))):
                  j=j[j.ATTRIB1_p != 46]
                  if len(set(list(j.ATTRIB1_p)))>1:
                      z.append(i)
                      if (1159 in list(j.ATTRIB1_p)):
                          j=j[j.ATTRIB1_p != 1159]
                  j=j.sort_values(by=['DPZ_p'], ascending=False)
                  ATTRIB1.append(j.ATTRIB1_p.iloc[0])
                  ATTRIB2.append(j.ATTRIB2_p.iloc[0])
                  ATTRIB3.append(j.ATTRIB3_p.iloc[0])
              else:
                  j=j.sort_values(by=['DPZ_p'], ascending=False)
                  ATTRIB1.append(j.ATTRIB1_p.iloc[0])
                  ATTRIB2.append(j.ATTRIB2_p.iloc[0])
                  ATTRIB3.append(j.ATTRIB3_p.iloc[0])
      else:
          j=j.sort_values(by=['DPZ_p'], ascending=False)
          ATTRIB1.append(j.ATTRIB1_p.iloc[0])
          ATTRIB2.append(j.ATTRIB2_p.iloc[0])
          ATTRIB3.append(j.ATTRIB3_p.iloc[0])
      a=a+1 
      pb['value'] = a/len(group_data.size())*100
      root.update() 
   
             
  return (pkt_id,nr, ATTRIB1, ATTRIB2, ATTRIB3,z)
def evopora():
    tab_cord=['UWIR_UI','UANOTHER_OB_UI','UUTIL_LIN_UI','G_ANUT_UI' ]
    tab_Space = ['UPKT_CEL','UPKT_TEAH','UPKT_ZAG','UPKT_KAN','UPKT_DIFF','UPKT_IND','UPKT_PHO','UPKT_WAT','UANOTHER_OB_UI','G_ANUT_UI']
    #basic list
    pkt_id,nr, ATTRIB1, ATTRIB2, ATTRIB3=[],[],[],[],[]
    z=[] 
    df=pd.DataFrame()
    pb2['value'] = 10
    a=0
    for tab in tab_cord:
        df = obj_string(tab,df)
        a=a+1 
        pb['value'] = a/len(tab_cord)*100
        root.update() 
    pb2['value'] = 30
    pb['value']=0 
    a=0
    for tab in tab_Space:
        df = ob_space(tab, df)
        a=a+1 
        pb['value'] = a/len(tab_Space)*100
        root.update() 
    pb2['value'] = 50
    grouped= df.groupby('IDentifier_o2')
    pkt_id,nr, ATTRIB1, ATTRIB2, ATTRIB3, z = the_newest_data(grouped, pkt_id, nr, ATTRIB1, ATTRIB2, ATTRIB3,z)            
    pb2['value'] = 80
    root.update()       
    #Attribute list
    global pkt_list
    pkt_list =pd.DataFrame({'pkt': pkt_id,
                            'nr':nr,
         'ATTRIB1': ATTRIB1,
         'ATTRIB2': ATTRIB2,
         'ATTRIB3' : ATTRIB3})

    pkt_list.to_sql('_temp', engine)
    pb2['value'] = 100
    root.update()   
    tkinter.messagebox.showinfo(message=f"Table pairing completed")
    
def ATTRIB2_fun():
    cursor.execute("UPDATE [TAB_IN] SET [TAB_IN].ATTRIB2 = _temp.ATTRIB2 	FROM [TAB_IN]  join _temp on _temp.nr = TAB_IN.obj_id where [TAB_IN].ATTRIB2 is null")
    conn.commit()
    tkinter.messagebox.showinfo(message=f"Upadate attribute number 2")
    
def ATTRIB1_fun():
    cursor.execute("UPDATE [TAB_IN] SET [TAB_IN].ATTRIB1 = _temp.ATTRIB1 	FROM [TAB_IN]  join _temp on _temp.nr = TAB_IN.obj_id where [TAB_IN].ATTRIB1 is null")
    conn.commit()
    tkinter.messagebox.showinfo(message=f"Upadate attribute number 1")
    
def ATTRIB3_fun():
    cursor.execute("UPDATE [TAB_IN] SET [TAB_IN].ATTRIB3 = _temp.ATTRIB3 	FROM [TAB_IN]  join _temp on _temp.nr = TAB_IN.obj_id where [TAB_IN].ATTRIB3 is null")
    conn.commit()
    tkinter.messagebox.showinfo(message=f"Upadate attribute number 3")
    
def del_tabele():
    # usun tabele
    cursor.execute("DROP TABLE dbo._temp")
    conn.commit()
    tkinter.messagebox.showinfo(message=f"Usunieto tabele TEMP")
    
def close_base():
    #zamknij połączenie 
    conn.close()
       
    engine.dispose()
    
    tkinter.messagebox.showinfo(message=f"Zamknięto bazę")
def save_file():

    file_name = asksaveasfilename()

    if file_name:
        f = open(file_name, 'a')
        contents = tk.tab_chrono.text_Space.get(1.0, 'end')
        f.write(contents)
        f.close()

def opperative_p():
    vertex_sql="""select DISTINCT v_ID, c_x, c_y from rowVTbl
    join VTbl on rowVTbl.v_ID= VTbl.ID
    join ObjTbl on ObjTbl.ID=rowVTbl.obj_id
    join LevelTab on LevelTab.ID = ObjTbl.c_code_ID
    where ObjTbl.status=1 and (rowVTbl.v_ID not in (select v_ID from MarkVTbl))
    and (LevelTab.c_name in ('GROUP1','GROUP2','GROUP3')
	or (LevelTab.c_name like 'G[RU][^O]%'))
    union
    select DISTINCT  v_ID, c_x, c_y from SpaceVTbl
    join VTbl on SpaceVTbl.v_ID= VTbl.ID
    join ObjTbl on ObjTbl.ID=SpaceVTbl.obj_id
    join LevelTab on LevelTab.ID = ObjTbl.c_code_ID
    where ObjTbl.status=1 and (SpaceVTbl.v_ID not in (select v_ID from MarkVTbl))
    and (LevelTab.c_name in ('GROUP1','GROUP2','GROUP3')
	or (LevelTab.c_name like  'G[RU][^O]%'))
    """
    file_name = asksaveasfilename()
    tab_v = pd.read_sql(v_sql, conn)
    tab_v['v_ID']='temp' 
    tab_v.to_csv(file_name, header=False, index=False)
    tkinter.messagebox.showinfo(message=f"Saved the table to a *.csv file")

def BIG_tabele():
    tab_Space_BIG=['U_BUILDINGS1']
    
    for tabela in tab_Space_BIG:
        open_tab = pd.read_sql(f"SELECT * FROM {tabela} ", conn)
        ObjTbl =  pd.read_sql("SELECT * FROM [ObjTbl]  ", conn)
        SpaceVTbl = pd.read_sql("SELECT * FROM [SpaceVTbl]  ", conn)
        MarkVTbl = pd.read_sql("SELECT * FROM [MarkVTbl]  ", conn)
        TAB_IN = pd.read_sql("SELECT * FROM [TAB_IN]  ", conn)
        attributeTbl= pd.read_sql("SELECT * FROM [G_ATTRIBUTE_ATTRIB2] ", conn)
        
        type_obj = pd.merge(open_tab.add_suffix('_p'),attributeTbl.add_suffix('_a'),how='left', left_on='obj_id_p', right_on='obj_id_a' )
        type_obj = pd.merge(type_obj,ObjTbl.add_suffix('_o'), left_on='obj_id_p', right_on='ID_o')
        type_obj_row= pd.merge(type_obj,SpaceVTbl.add_suffix('_l'),left_on='obj_id_p', right_on='obj_id_l' )
        type_obj_li_v= pd.merge(type_obj_row,MarkVTbl.add_suffix('_Mark'),left_on='v_ID_l', right_on='v_ID_Mark' )
        type_obj_li_v_in= pd.merge(type_obj_li_v, TAB_IN.add_suffix('_in'),left_on='obj_id_Mark', right_on='obj_id_in')
        type_obj_li_v_in_obj= pd.merge(type_obj_li_v_in, ObjTbl.add_suffix('_o2'),left_on='obj_id_in', right_on='ID_o2')
        
        wynik_grupy= type_obj_li_v_in_obj.groupby('IDentifier_o2')
    group_data=wynik_grupy
    
    pkt_id,nr, ATTRIB1, ATTRIB2, ATTRIB3,z=[],[],[],[],[],[]
    for i,j in group_data:
        pkt_id.append(i)
        nr.append(j.obj_id_Mark.iloc[0])
        if len(set(list(j.ATTRIB1_p)))>1:
                if 46 in list(j.ATTRIB1_p):
                    z.append(i)
                    j=j[j.ATTRIB1_p != 46]
                   #j=j.sort_values(by=['DPZ_p'], ascending=False)
                    ATTRIB1.append(j.ATTRIB1_p.iloc[0])
                    ATTRIB2.append(j.ATTRIB2_a.iloc[0])
                    ATTRIB3.append(j.ATTRIB3_p.iloc[0])
                else:
                   # j=j.sort_values(by=['DPZ_p'], ascending=False)
                    ATTRIB1.append(j.ATTRIB1_p.iloc[0])
                    ATTRIB2.append(j.ATTRIB2_a.iloc[0])
                    ATTRIB3.append(j.ATTRIB3_p.iloc[0])
        else:
            ATTRIB1.append(j.ATTRIB1_p.iloc[0])
            ATTRIB2.append(j.ATTRIB2_a.iloc[0])
            ATTRIB3.append(j.ATTRIB3_p.iloc[0])
            
            
    pkt_list =pd.DataFrame({'pkt': pkt_id,
                            'nr':nr,
         'ATTRIB1': ATTRIB1,
         'ATTRIB2': ATTRIB2,
         'ATTRIB3' : ATTRIB3})
    
    
    pkt_list.to_sql('_temp_BIG', engine)
    
    cursor.execute("UPDATE [TAB_IN] SET [TAB_IN].ATTRIB1 = _temp_BIG.ATTRIB1 	FROM [TAB_IN]  join _temp_BIG on _temp_BIG.nr = TAB_IN.obj_id")
    cursor.execute("UPDATE [TAB_IN] SET [TAB_IN].ATTRIB2 = _temp_BIG.ATTRIB2 	FROM [TAB_IN]  join _temp_BIG on _temp_BIG.nr = TAB_IN.obj_id")
    cursor.execute("UPDATE [TAB_IN] SET [TAB_IN].ATTRIB3 = _temp_BIG.ATTRIB3 	FROM [TAB_IN]  join _temp_BIG on _temp_BIG.nr = TAB_IN.obj_id")
    
    # usun tabele
    cursor.execute("DROP TABLE dbo._temp_BIG")
    #zapisz
    conn.commit()
    tkinter.messagebox.showinfo(message=f"ATTRIB3, ATTRIB1, ATTRIB2 filled.")
    
def BIG_usppre():
    query = f'''select ID from LevelTab
                where c_name='Band1' '''
    df = pd.read_sql(query, conn)
    conn.commit()
    if len(df)>0:
        nr=df.ID[0]
        cursor.execute(f'''update ObjTbl
                            set ObjTbl.c_code_ID = {nr}
                            from ObjTbl
                            join LevelTab c on c.ID = ObjTbl.c_code_ID
                            join MarkVTbl on MarkVTbl.obj_id=ObjTbl.ID
                            join rowVTbl on MarkVTbl.v_ID=rowVTbl.v_ID
                            join ObjTbl ob2 on ob2.ID=rowVTbl.obj_id
                            join LevelTab on LevelTab.ID = ob2.c_code_ID
                            where c.c_name ='WORK' and ObjTbl.status=1 and
                            LevelTab.c_name in ('GROUP1','GROUP2','GROUP3') ''')
        conn.commit()
        cursor.execute(f'''update ObjTbl
                            set ObjTbl.c_code_ID ={nr}
                            from ObjTbl
                            join LevelTab c on c.ID = ObjTbl.c_code_ID
                            join MarkVTbl on MarkVTbl.obj_id=ObjTbl.ID
                            join SpaceVTbl on MarkVTbl.v_ID=SpaceVTbl.v_ID
                            join ObjTbl ob2 on ob2.ID=SpaceVTbl.obj_id
                            join LevelTab on LevelTab.ID = ob2.c_code_ID
                            where c.c_name ='WORK' and  ObjTbl.status=1 and
                            LevelTab.c_name in ('GROUP1','GROUP2','GROUP3')
                            ''')
        conn.commit()
    
    
        tkinter.messagebox.showinfo(message=f"Changed the group type")
    else:
        tkinter.messagebox.showinfo(message=f"NO WORK class")
    
# root window
root = tk.Tk()
root.geometry("500x330")


# create a notebook
notebook = ttk.Notebook(root)
notebook.pack(pady=10, expand=True)

# create frames
frame = ttk.Frame(notebook, width=400, height=280)
frame2 = ttk.Frame(notebook, width=400, height=280)
frame0 = ttk.Frame(notebook, width=400, height=280)
frame3 = ttk.Frame(notebook, width=400, height=280)

frame0.pack(fill='both', expand=True)
frame.pack(fill='both', expand=True)
frame2.pack(fill='both', expand=True)
frame3.pack(fill='both', expand=True)
# add frames to notebook
notebook.add(frame0, text='DataBase')
notebook.add(frame2, text='WORKING_GROUP')
notebook.add(frame, text='ATTRIBUTE1')
notebook.add(frame3, text='ATTRIBUTE BIG')

leftframe = ttk.Frame(frame0)
leftframe.pack(side=tk.TOP)
 
rightframe = ttk.Frame(frame)
rightframe.pack(side=tk.TOP)

frameBIG = ttk.Frame(frame3)
frameBIG.pack(side=tk.TOP)
 
bottomframe = ttk.Frame(frame)
bottomframe.pack(side=tk.BOTTOM)
 
label = tk.Label(leftframe , text = "Connection to Database")
label.pack()

vlist = ["DATABASE_1","DATABASE"]

Combo = ttk.Combobox(leftframe, values = vlist)
Combo.set("Select the Server")
Combo.pack(padx = 8, pady = 8)

my_entry2 = tk.Entry(leftframe, width = 25)
my_entry2.insert(0,'Nazwa bazy')
my_entry2.pack(padx = 5, pady = 5)

var = tk.IntVar()
R2 = tk.Radiobutton(leftframe , text='Windows Authentication', variable=var, value=2, command=disable_entry)
R2.pack( anchor = tk.W )
R1 = tk.Radiobutton(leftframe , text='SQL Sever Authentication', variable=var, value=1, command=disable_entry)
R1.pack( anchor = tk.W )


log_entry = tk.Entry(leftframe, width = 25,state= "disabled")
log_entry.insert(0,'Login')
log_entry.pack(padx = 5, pady = 5)

log_entry2 = tk.Entry(leftframe, width = 25,state= "disabled")
log_entry2.insert(0,'Password')
log_entry2.pack(padx = 5, pady = 5)



button1 = tk.Button(leftframe, text = "Connect", command=message_server)
button1.pack(padx = 3, pady = 3)


########################
button3 = tk.Button(rightframe, text = "Parse the table", command=evopora)
button3.pack(padx = 3, pady = 3)

button4 = tk.Button(rightframe, text = "Fill in the attribute 1", command=ATTRIB2_fun)
button4.pack(padx = 3, pady = 3)

button5 = tk.Button(rightframe, text = "Fill in the attribute 2", command=ATTRIB1_fun)
button5.pack(padx = 3, pady = 3)

button6 = tk.Button(rightframe, text = "Fill in the attribute 3", command=ATTRIB3_fun)
button6.pack(padx = 3, pady = 3)

button6a = tk.Button(rightframe, text = "Delate temp table", command=del_tabele)
button6a.pack(padx = 3, pady = 3)

button7 = tk.Button(rightframe, text = "Close the base", command=close_base)
button7.pack(padx = 3, pady = 3)
##########################

button8 = tk.Button(frameBIG, text = "Parse the tables BIG", command=BIG_tabele)
button8.pack(padx = 3, pady = 3)

button9 = tk.Button(frameBIG, text = "Change GROUP", command=BIG_usppre)
button9.pack(padx = 3, pady = 3)



# progressbar
pb = ttk.Progressbar(
    bottomframe,
    orient='horizontal',
    mode='determinate',
    length=3000
)
# place the progressbar
pb.pack(padx = 5, pady =10)

pb2 = ttk.Progressbar(
    bottomframe,
    orient='horizontal',
    mode='determinate',
    length=3000
)
# place the progressbar
pb2.pack(padx = 5, pady =10)



### save file name 

label = tk.Label(frame2 , text = "Save the table ")
label.pack(padx = 15, pady = 15)

button11 = tk.Button(frame2, text = "OPPERATIVE GROUP", command=opperative_p)
button11.pack(padx = 3, pady = 3)

label = tk.Label(frame2 , text = "Before you start")
label.pack(padx = 10, pady = 10)

button2 = tk.Button(frame2, text = "Check Group tables", command=del_name)
button2.pack(padx = 3, pady = 3)

 

root.title("Modify the attribute in the database")

root.mainloop()