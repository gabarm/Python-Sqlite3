import pandas as pd
import matplotlib.pyplot as plt
import sqlite3

class ClassVisuel():
    
    def __init__(self,db_path):
        self.db = sqlite3.connect(db_path)
    
    def __extract_data(self):
        self.vaccin = pd.read_sql("""SELECT iso_code,date, total_vaccinations 
                                  FROM vaccinations 
                                  WHERE iso_code IN ('SWE') AND total_vaccinations IS NOT NULL""",self.db)
        return self.vaccin
    
    def plot_daily_vaccinations(self):
        self.dateframe = self.__extract_data()
        plt.plot(self.dateframe["total_vaccinations"], self.dateframe["date"])
        plt.xlabel("tot_vac")
        plt.ylabel("date")
        plt.show()
    