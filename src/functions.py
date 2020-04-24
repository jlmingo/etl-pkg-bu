import pandas as pd
import os
import re

def transform_df_bu20(df, path_scopes, scope_equivalences):
    #Filter initial dataframe
    
    #Clients and products are null
    filter_clients_products = (df["D_CLIENTES"].isnull()) & (df["D_PRODUTOS"].isnull())
    
    #Active, passive and results accounts
    filter_2_1 = df["D_AC"].str.startswith("A")
    filter_2_2 = df["D_AC"].str.startswith("P")
    filter_2_3 = df["D_AC"].str.startswith("R")
    filter_accounts = (filter_2_1 | filter_2_2 | filter_2_3)
    
    #FL values start wuth F, except FA and FB which are dropped
    filter_3 = df["D_FL"].str.startswith("F")
    filter_4 = ~df["D_FL"].str.startswith("FA")
    filter_5 = ~df["D_FL"].str.startswith("FB")
    filter_flow = filter_3 & filter_4 & filter_5
    
    #Other nulls
    filter_6 = df["D_T1"] != "S9999"
    filter_7 = df["D_T2"].isnull()
    filter_8 = df["D_LE"].isnull()
    filter_9 = df["D_NU"].isnull()
    filter_10 = df["D_DEST"].isnull()
    filter_11 = df["D_AREA"].isnull()
    filter_12 = df["D_MU"].isnull()
    filter_13 = df["D_PMU"].isnull()
    filter_other_nulls = filter_6 & filter_7 & filter_8 & filter_9 & filter_10 & filter_11 & filter_12 & filter_13

    #Total filter
    filter_total = filter_clients_products & filter_accounts & filter_flow & filter_other_nulls
    df = df[filter_total]

    ##Drop rows
    #Drop results with F distinct from F99
    filter_1 = (df["D_AC"] == "P8800000") & (df["D_FL"] == "F10")
    filter_2 = ~df["D_AC"].str.startswith('R') & (df["D_FL"] == "F99")
    filter_3 = df["D_AC"].str.startswith('R') & (df["D_FL"] != "F99")
    index_drop = df[filter_1 | filter_2 | filter_3].index
    df = df.drop(index_drop)

    #Drop RU not beggining with "3"
    filter_ru = ~df["D_RU"].str.startswith("3")
    index_drop = df[filter_ru].index
    df = df.drop(index_drop)

    #columns selection
    selected_cols = ["D_CA", "D_DP", "D_PE", "D_RU", "D_AC", "D_FL", "D_AU", "D_T1", "P_AMOUNT"]
    df = df[selected_cols]

    #date fix
    print(df.D_PE.unique())
    df.loc[df["D_PE"] == "2020.1","D_PE"] = "2020.10"

    #Passive and result multiplied by -1
    df.loc[filter_2_2, "P_AMOUNT"] = df["P_AMOUNT"].multiply(-1)
    df.loc[filter_2_3, "P_AMOUNT"] = df["P_AMOUNT"].multiply(-1)

    #Convert R, F99 to F10
    df.loc[df["D_AC"].str.startswith("R"), 'D_FL'] = "F10"

    #look for scope
    # df_codes = pd.read_excel(path_scopes, dtype={"Reporting unit (code)": "str"})
    # df = scope_adding(df, df_codes)
    # df.loc[:,"Scope"] = df["Scope"].map(lambda x: scope_equivalences[x] if x in list(scope_equivalences.keys()) else "OTHER")

    ##fix transactions with third parties
    #separated dataframe with blank T1
    index_drop = df[~df["D_T1"].isnull()].index
    df_F_blanks = df.drop(index_drop)
    df_F_blanks.drop(["D_T1"], axis=1, inplace=True)

    #separated dataframe with "group companies" T1
    index_drop = df[df["D_T1"].isnull()].index
    df_F_ggcc = df.drop(index_drop)
    df_F_ggcc.drop(["D_T1"], axis=1, inplace=True)
    df_F_ggcc.loc[:,"P_AMOUNT"] = df_F_ggcc["P_AMOUNT"].multiply(-1)

    #additional dataframe with new calculated "S9999"
    df_third_parties = pd.concat([df_F_blanks, df_F_ggcc])
    df_third_parties = df_third_parties.groupby(["D_CA", "D_DP", "D_PE", "D_RU", "D_AC", "D_FL", "D_AU"], as_index=False).sum()
    df_third_parties.loc[:,"D_T1"] = "S9999"
    
    #output dataframe
    index_drop = df[df["D_T1"].isnull()].index
    df = df.drop(index_drop)
    df = pd.concat([df, df_third_parties])

    #type corrections
    df.loc[:,"D_RU"] = df["D_RU"].astype("str")
    print(df.D_PE.unique())
    df.loc[:, 'D_PE'] =  pd.to_datetime(df['D_PE'], format='%Y.%m')

    df.loc[:,"D_PE"] = df["D_PE"].astype("str")

    #clear rows with value 0
    df = df[df.P_AMOUNT != 0]
    
    #drop columns
    index_drop = ["D_CA", "D_DP"]
    df.drop(index_drop, axis=1, inplace=True)
    
    #rename columns
    df = df.rename(columns={"D_RU": "RU", "D_AC": "AC", "D_FL": "FL", "D_AU": "AU", "D_T1": "T1", "D_PE": "PE"})
    print(df.PE.unique())
    return df

def ytd_to_month(df_YTD_current_month, df_YTD_previous_month):
    df_YTD_previous_month.loc[:,"P_AMOUNT"] = df_YTD_previous_month["P_AMOUNT"].multiply(-1)
    df_final_current_month = pd.concat([df_YTD_current_month, df_YTD_previous_month])
    df_final_current_month = df_final_current_month.groupby(['RU', 'AC', 'FL', 'AU', 'T1'], as_index=False).sum()
    return df_final_current_month

def transform_sap_bu20(df, df_join, scope_equivalences, path_scopes):
    
    #columns selection

    selection = ['Amount in local currency', 'Text', 'Trading partner', 'G/L Account',
    'Unnamed: 12', 'Amount in doc. curr.', 'Order',
    'Year/month', 'Company Code', 'WBS element', 'Purchasing Document', 'Material',
    'General ledger amount']
    df = df[selection]

    #drop rows where date contains month 13
    index_drop = df[df["Year/month"].str.contains("/13")].index
    df = df.drop(index_drop)

    #Correct dates
    df.loc[:,"Year/month"] = df["Year/month"].apply(lambda x: reg_date(x))

    #rename unnamed
    df = df.rename(columns={"Unnamed: 12": "CoCe"})
    
    #add AU column
    df["AU"] = "0LIA01"

    #Drop 2019 values
    index_drop = df[df["Year/month"].str.contains("2019")].index
    df = df.drop(index_drop)

    #format date column
    df.loc[:, 'Year/month'] =  pd.to_datetime(df['Year/month'], format='%Y/%m')

    #correct numbers
    numeric_fields = ['Amount in local currency', 'Amount in doc. curr.', 'General ledger amount']
    df.loc[:, numeric_fields] = df[numeric_fields].replace(",", "", regex=True)
    df.loc[:,numeric_fields] = df[numeric_fields].astype(float)
    
    #join_df to lookup AC
    df = df.merge(df_join, on="G/L Account", how="left")
    print(f"shape after merge: {df.shape}")
    
    # Aadd new columns
    df_codes = pd.read_excel(path_scopes, dtype={"Reporting unit (code)": "str"})
    df = codes_columns_adding(df, df_codes)

    #find new society code for Trading Partner
    # df_trading_partner = pd.read_excel(path_trading_partner, sheet_name="ZPMIG_ZCVBUND", dtype={"OLD CODE": str, "SIM R CODE": str})
    # df_trading_partner = df_trading_partner.rename(columns={"OLD CODE": "Trading partner", "SIM R CODE": "Reporting unit (code)"})
    # df_trading_partner = df_trading_partner.drop_duplicates(subset="Trading partner", keep="last")
    # df = df.merge(df_trading_partner[["Reporting unit (code)", "Trading partner"]], on="Trading partner", how="left")
    
    # df.loc[:,"Trading partner"] = df["Trading partner"].astype("str")
    print(df["Trading partner"].unique())
    df["Trading partner"].fillna("S9999", inplace = True) 
    print(df["Trading partner"].unique())
    # df.loc[:, "Reporting unit (code)_y"] = df["Reporting unit (code)_y"].replace("-", "S9999", regex=True)
    # df.drop("Trading partner", axis=1, inplace=True)
    df["FL"] = "F10"
    df = df.rename(columns={"Trading partner": "T1", 
                            "Company Code": "RU", 
                            "Year/month": "PE", 
                            "FS Item": "AC",
                            "Amount in local currency": "P_AMOUNT"})
    df = df.astype({'RU': 'str'})
    print(f"current shape: {df.shape}")
    
    # correct scopes
    print(df.Scope.unique())
    df.loc[:,"Scope"] = df["Scope"].map(lambda x: scope_equivalences[x] if x in list(scope_equivalences.keys()) else "OTHER")
    print(df.Scope.unique())

    return df

def df_query_gen(path_query):
    df_join = pd.read_csv(path_query, dtype={"Account Number": "str", "FS Item": "str"})
    df_join = df_join.rename(columns={"Account Number": "G/L Account"})
    df_join = df_join[["G/L Account", "FS Item"]].copy()
    return df_join

def codes_columns_adding(df, df_codes):
    df_codes = df_codes.rename(columns={"Reporting unit (code)": "Company Code"})
    # df_codes = df_codes.drop_duplicates(subset ="Company Code", keep = "first")
    merging_columns = ["Company Code", "Reporting unit (description)", 'Revised method (Closing)', 'Revised Conso. (Closing)',
    'Revised Own. Int. (Closing)', 'Revised Fin. Int. (Closing)', "Scope", "D_CU"]
    df = df.merge(df_codes[merging_columns], on="Company Code", how="left")
    print(f"shape after merge: {df.shape}")
    return df

def scope_adding(df, df_codes):
    df_codes = df_codes.rename(columns={"Reporting unit (code)": "D_RU"})
    # df_codes = df_codes.drop_duplicates(subset ="Company Code", keep = "first")
    merging_columns = ["D_RU", "Scope"]
    df = df.merge(df_codes[merging_columns], on="D_RU", how="left")
    return df

'''function to correct dates, for instance converts 2020/010 to 2020/10'''
def reg_date(date):
    matching = re.findall(r"/0\d{2}", date)
    if (matching):
        matching = re.findall(r"/0\d{2}", date)[0]
        date = re.sub(matching, matching[0]+matching[2]+matching[3], date)
        return date
    else:
        return date

def sap_dif_mag(df_pck, df_sap):
    #take only certain columns of df_sap and multiply by -1
    df_pck.loc[:,"RU"] = df_pck["RU"].astype("str")
    df_sap.loc[:,"RU"] = df_sap["RU"].astype("str")
    df_sap_2 = df_sap[['RU', 'AC', 'FL', 'AU', 'T1', 'P_AMOUNT', 'PE']].copy()
    df_sap_2.loc[:,"P_AMOUNT"] = df_sap_2['P_AMOUNT'].multiply(-1)

    #concat and groupby
    df_dif = pd.concat([df_pck, df_sap_2])
    df_dif = df_dif.groupby(['RU', 'AC', 'FL', 'AU', 'T1', 'PE'], as_index=False).sum()
    
    df_sap["Source"] = "SAP"
    df_dif["Source"] = "Differences"

    df_final = pd.concat([df_sap, df_dif])

    return df_final