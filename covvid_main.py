import covclass as cov
import classvisuel as clv

def main():
    ## create an object of the class
    con = cov.CovClass("vaccin_covid.csv","vaccin_covid.db","cov_vacc")

    ## split column vaccines, values 0 and 1 and save it to a new dataframe
    
    con.get_dummies("vaccines", ", ")
    
    ## join dataframes
    con.join_dataframes(con.df_dummies)
   
    ##create list of old column names and new column names 
    old_colnames = "Johnson&Johnson","Oxford/AstraZeneca","Pfizer/BioNTech","RBD-Dimer","Sinopharm/Beijing","Sinopharm/HayatVax","Sinopharm/Wuhan","Sputnik V","source_name","source_website"
    new_colnames = "JohnsonJohnson","OxfordAstraZeneca","PfizerBioNTech","RBDDimer","SinopharmBeijing","SinopharmHayatVax","SinopharmWuhan","SputnikV","sourceName","sourceWebsite"
    
    ##rename columnnames
    con.rename_columns(old_colnames,new_colnames)
    ##create database and cursor connection
    con.create_database()
    con.seed_database()
    
   
    ##Drop columns that has less than 20000 values
    con.drop_na_values(1,20000)

    ##create new table
      
    con.exec_query("""CREATE TABLE country_vaccines(country_ID INTEGER PRIMARY KEY AUTOINCREMENT, country TEXT UNIQUE  ON CONFLICT REPLACE,       iso_code TEXT UNIQUE  ON CONFLICT REPLACE, Abdala INT,	CanSino	INT, Covaxin INT, EpiVacCorona INT, JohnsonJohnson INT, Moderna INT, OxfordAstraZeneca INT, PfizerBioNTech INT, QazVac INT,	RBDDimer INT, SinopharmBeijing INT, SinopharmHayatVax INT, SinopharmWuhan INT, Sinovac INT, Soberana02 INT,SputnikV INT, sourceName TEXT,sourceWebsite TEXT)""")
    con.conn.commit()
    
    ##Insert value from old table to new table
    
    con.exec_query("""INSERT INTO country_vaccines(country, iso_code, Abdala, CanSino, Covaxin,EpiVacCorona, JohnsonJohnson, Moderna, OxfordAstraZeneca, PfizerBioNTech, QazVac, RBDDimer, SinopharmBeijing,SinopharmHayatVax, SinopharmWuhan,Sinovac, Soberana02 ,SputnikV,sourceName,sourceWebsite)SELECT country, iso_code, Abdala, CanSino, Covaxin,EpiVacCorona, JohnsonJohnson, Moderna, OxfordAstraZeneca, PfizerBioNTech,QazVac, RBDDimer, SinopharmBeijing, SinopharmHayatVax, SinopharmWuhan, Sinovac, Soberana02 ,SputnikV,sourceName,sourceWebsite FROM cov_vacc""")
    con.conn.commit()
    
    
    ##create new table 
    con.exec_query("""CREATE TABLE vaccinations(iso_code TEXT,date DATE, total_vaccinations FLOAT, people_vaccinated FLOAT, people_fully_vaccinated FLOAT, daily_vaccinations FLOAT, total_vaccinations_per_hundred FLOAT,people_vaccinated_per_hundred FLOAT, people_fully_vaccinated_per_hundred FLOAT,daily_vaccinations_per_million FLOAT,FOREIGN KEY (iso_code) REFERENCES cov_vacc(iso_code) PRIMARY KEY (iso_code,date))""")
    con.conn.commit()
    
    ## Insert values from old table
    con.exec_query("""INSERT INTO vaccinations(iso_code,date,total_vaccinations,people_vaccinated,people_fully_vaccinated,daily_vaccinations,total_vaccinations_per_hundred, people_vaccinated_per_hundred,people_fully_vaccinated_per_hundred,daily_vaccinations_per_million)SELECT iso_code, date,total_vaccinations,people_vaccinated,people_fully_vaccinated,daily_vaccinations, total_vaccinations_per_hundred,people_vaccinated_per_hundred,  people_fully_vaccinated_per_hundred, daily_vaccinations_per_million FROM cov_vacc""")
    con.conn.commit()
    
    cv = clv.ClassVisuel("vaccin_covid.db")
    
    cv.plot_daily_vaccinations()
    
if __name__ == "__main__":
    main()