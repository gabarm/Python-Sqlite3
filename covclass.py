import pandas as pd
import sqlite3

class CovClass():

    def __init__(self,path:str,url_db:str,name:str):
        self.df = pd.read_csv(path)
        self.conn = sqlite3.connect(url_db)
        self.name = name
    
    def get_dummies(self,colname,delimiter:str):
        self.df_dummies = self.df[colname].str.get_dummies(sep=delimiter)
     
    def join_dataframes(self,new_dataframe):
        self.df = self.df.join(new_dataframe)
        
    def create_database(self):
        self.db = self.df.to_sql(self.name,self.conn)

    def seed_database(self):
        self.db = self.conn.cursor()
        
    def exec_query(self,query:str):
        self.db.execute(query)
    
    def drop_na_values(self,axs,thres):
        self.df_droped_na = self.df.dropna(axis=axs, thresh=thres)
        
    def rename_columns(self,old_col_name, new_col_name):
        self.df = self.df.rename(columns=dict(zip(old_col_name, new_col_name)))
                  

