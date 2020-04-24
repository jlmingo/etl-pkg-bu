from variables import *
from functions import *

def main():
    
    program_mode = int(input("Please introduce program mode: "))
    
    #Calculating PCK budget
    
    if program_mode in [0,1]:
        print("Processing packages files")
        
        df_pck = pd.read_csv(path_pck_bu, delimiter=";", dtype={"D_RU": "str", "D_ORU": "str", "D_PE": "str"})
        df_pck = transform_df_bu20(df_pck, path_scopes, scope_equivalences)
        
        df_list_months = []
        
        for month in range(1,13):
            date = "2020-"+str(month).zfill(2)+"-01"
            print(f"filtering: {date}")
            df_month_ytd = df_pck[df_pck["PE"] == date].copy()
            if month == 1:
                df_append = df_month_ytd.copy()
                df_list_months.append(df_append)
                df_month_ytd.drop("PE", axis=1, inplace=True)
                df_previous_month_ytd = df_month_ytd.copy()
                print(f"Month {month} appended")
            else:
                df_month_ytd.drop("PE", axis=1, inplace=True)
                df_month = ytd_to_month(df_month_ytd, df_previous_month_ytd)
                df_month["PE"] = date
                a = df_month[
                        (df_month["AC"] == "R7152000") &
                        (df_month["RU"] == "3000") &
                        (df_month["PE"] == date)
                    ].P_AMOUNT.sum()
                print(a)
                df_list_months.append(df_month)
                df_previous_month_ytd = df_month_ytd.copy()
                print(f"Month {month} appended")
        df_pck = pd.concat(df_list_months)
        print("Generating packages csv")
        df_pck.to_csv("../output/bu_20_pck.csv", index=False)
        print("Packages CSV generated")

    #Calculating SAP budget
    if program_mode in [0,2]:
        df_sap = pd.read_csv(path_sap, dtype={"G/L Account": "str", "Trading partner": "str", "Company Code": "str"})
        df_join = df_query_gen(path_join)
        df_sap = transform_sap_bu20(df_sap, df_join, scope_equivalences, path_scopes)
        df_sap.to_csv("../output/bu_20_sap.csv")
        print("SAP file processed")
    
    if program_mode == 0:
        print(df_pck.columns)
        print(df_sap.columns)
        df_final = sap_dif_mag(df_pck, df_sap)
        print("Processing final csv...")
        df_final.to_csv("../output/monthly_pl&bs_pk_2020B.csv")
        print("Final CSV generated")

if __name__ == "__main__":
    main()