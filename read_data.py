import datetime
import pandas as pd
import datetime as dt
from datetime import datetime,timedelta
import numpy as np
import csv
import warnings
warnings.filterwarnings('ignore')
import bill_data_process as bdp

today= datetime.now()


class GatherData():

    def execute_data(self,inppath,tax_data,wrong_pid):

            # Read property data
            property_file = "Demand Excluding Illegal 2023-24 27072023.csv"
            property_demand = pd.read_csv(inppath + property_file, engine='pyarrow')
            property_demand.dropna(subset=['propertycode', 'propertykey'], how='all', inplace=True)
            property_demand['propertycode'] = property_demand['propertycode'].astype(float)
            #-------------------------------------------------------------------------------------------------

            # Read property receipt details
            property_receipt_file = "Property_Receipt_26102023.csv"
            property_receipt_df = pd.read_csv(tax_data + property_receipt_file, low_memory=False, encoding='utf-8-sig')

            # Read the property details data which is property parameters & details.
            property_list_df = pd.read_csv(tax_data + "Property_List_26102023.csv",low_memory=False)
            property_list_df = bdp.cleaned_propertylist(property_list_df,wrong_pid)

            # Read property bill details & bill data which is included bill amount & balanceamount
            property_bill_df = pd.read_csv(tax_data + "Property_Bill_26102023.csv",
                                           low_memory=False,
                                           encoding='utf-8-sig')
            #------------------------------------------------------------------------------------------------
            arrears_bill,ty_bill =  bdp.identify_billdetails(property_bill_df,property_list_df)
            property_data = bdp.property_bill_demand(property_demand,property_list_df,arrears_bill,ty_bill)
            property_data['propertycode'] = property_data['propertycode'].astype(float)

            # Read Shasti propertykey data
            shasti_file = "Property_Shasti_list.csv"
            shasti_data = pd.read_csv(tax_data + shasti_file)
            shasti_data['shasti_Flag'] = 1

            # Read bill distributed details
            bill_distributed_file = "Master_Bill_Distributed_Payments.csv"
            bill_distributed_details = pd.read_csv(inppath + bill_distributed_file, encoding='utf-8')
            bill_distributed_details['propertycode'] = bill_distributed_details['propertycode'].astype(float)

            # Read japtinotice data
            japtinotice_file = "Japti_data23112023.csv"
            japtinotice_data = pd.read_csv(tax_data + japtinotice_file, encoding='utf-8')

            return  property_list_df, property_data, property_receipt_df, shasti_data, bill_distributed_details, japtinotice_data

    def property_list(self,tax_data):
        # Read the property details data which is property parameters & details.
        property_list_df = pd.read_csv(tax_data + "Property_List_25062023.csv",low_memory=False)
        property_list_df = property_list_df[property_list_df['verified'] != 'N']
        property_list_df.dropna(subset=['propertycode','propertykey'], how='all', inplace=True)
        property_list_df['propertykey'] = property_list_df['propertykey'].drop_duplicates()
        property_list_df['propertycode'] = property_list_df['propertycode'].drop_duplicates()
        property_list_df['propertycode'] = property_list_df['propertycode'].astype(float)

        return property_list_df

    def mapping_data(self,mappath):
        # Read Use type master data Residential, Non-residential & Industrial etc...

        usetype = pd.read_csv(mappath + "usetype.csv")
        usemap = dict(zip(usetype['usetypekey'],usetype['eng_usename']))

        # Read the Construction type of data
        consttype = pd.read_csv(mappath + "constructiontype.csv")
        construcmap = dict(zip(consttype['constructiontypekey'],consttype['eng_constructiontypename']))

        # Read Ocuupancy type of data
        occptype=  pd.read_csv(mappath + "occupancy.csv")
        occpmap = dict(zip(occptype['occupancykey'],occptype['occupancyname']))

        subusetype= pd.read_csv(mappath + "subusetype.csv")
        subusemap = dict(zip(subusetype['subusetypekey'],subusetype['eng_subusename']))

        zonetype =pd.read_csv(mappath + "zone.csv")
        zonemap = dict(zip(zonetype['zonekey'],zonetype['eng_zonename']))

        gattype = pd.read_csv(mappath + "gat.csv")
        gattype['gatname_z'] = gattype['gatname'].astype(str)
        gatnamemap = dict(zip(gattype['gat'], gattype['gatname_z']))

        specialowner = pd.read_csv(mappath + "specialownership.csv")
        splownmap = dict(zip(specialowner['specialownership'], specialowner['eng_specialownershipname']))

        splacctype = pd.read_csv(mappath + "specialoccupant.csv")
        splaccmap = dict(zip(splacctype['specialoccupantkey'], splacctype['eng_specialoccupantname']))

        wrong_pid = pd.read_excel(mappath + "japtiwrong_pid.xlsx")

        return zonemap,usemap,construcmap,occpmap,subusemap,gatnamemap,splownmap,splaccmap,wrong_pid


    def identify_unique_key_property(self,property_list_df):
        ### find the Unique property code (pid) from the property bill data
        unique_values = property_list_df.propertykey.unique()
        unique_pkey_df = pd.DataFrame({'propertykey': unique_values})

        return unique_pkey_df

    def read_paidamount_data(self,paidamount_file,day):
        ## Read YTD data
        ytddata = pd.read_excel(paidamount_file + f"Paidamount_list_{day}.xlsx")

        ## Replace the property code values in ytd data
        try:
            ytddata["propertycode"] = ytddata["propertycode"].replace("1100900002.10.10", "1100900002.20").replace("1100900002.10.20", "1100900002.30")
        except:
            pass
        ytddata['propertycode'] = ytddata['propertycode'].astype(float)
        ytddata.dropna(subset=['propertycode'], how='all', inplace=True)
        ytddata1 = ytddata.sort_values('receiptdate')
        # ytddata1 = ytddata1.groupby(['propertycode']).agg({'receiptdate': 'last', 'paidamount': 'sum'}).reset_index()
        ytddata1 = ytddata1.groupby(['propertycode']).agg({'modeofpayment':'last',
                                                           'receiptdate': 'last',
                                                           'paidamount': 'sum'}).reset_index()

        ytddata1 = ytddata1.rename(
            columns={'receiptdate': 'This Year Paiddate', 'paidamount': 'This Year Paidamount'})
        ytddata1 = ytddata1[['propertycode', 'This Year Paiddate', 'This Year Paidamount','modeofpayment']]
        ytddata1['paidTY_Flag'] = 1
        return ytddata1

    def read_paidamount_LY(self,tax_data):
        lypaid = pd.read_csv(tax_data + "Paid_amount 2022-04-01 To 2023-03-31.csv")
        lypaid["propertycode"] = lypaid["propertycode"].replace("1100900002.10.10", "1100900002.20")
        lypaid['propertycode'] = lypaid['propertycode'].astype(float)
        lypaid = lypaid.rename(columns={'receiptdate': 'last payment date','paidamount':'last payment amount'})
        lypaid.dropna(subset=['propertycode'], how='all', inplace=True)
        lypaid = lypaid[['propertycode', 'last payment date', 'last payment amount']]
        lypaid1 = lypaid.sort_values('last payment date')
        lypaid1 = lypaid1.groupby(['propertycode']).agg({'last payment date': 'last', 'last payment amount': 'sum'}).reset_index()

        lypaid1['paidLY_Flag'] = 1
        return lypaid1

    def filter_data(self,property_bill_df):
        property_df = property_bill_df[
            ['propertybillkey', 'propertykey', 'financialyearkey', 'fromdate', 'balanceamount', 'billamount']]

        property_df = property_df.rename(columns={'fromdate':'billdate','balanceamount':'totalbillamount'})

        property_fin_yrmth_df = property_df.copy()
        ## find the (receiptdate) column year or month
        property_fin_yrmth_df['receiptdate'] = pd.to_datetime(property_df['receiptdate'],errors = 'coerce',format='%Y-%m-%d')
        property_fin_yrmth_df['fin_year_r'] = property_fin_yrmth_df['receiptdate'].dt.year
        property_fin_yrmth_df['fin_month_r'] = property_fin_yrmth_df['receiptdate'].dt.month
        print("Identified the financial year & month\n---------------------------------------------------------------"
                  "----------------------------------------------------------------------")

        return property_fin_yrmth_df,property_df