import pandas as pd
import datetime as dt
import numpy as np
import warnings
warnings.filterwarnings('ignore')
import read_data as rd
import flager
import re

read_data = rd.GatherData()


class ptax_activity():

    def ptax_procedure(self,filter_propertybill_df,unique_pid_df):

        property_financial_yrmth_df = self.fin_yearmonth(filter_propertybill_df)

        ## find the GroupBy MAX PID, financial year or month using receipt Date
        property_fyrmth_r_max =\
                    property_financial_yrmth_df.groupby(['propertykey'])['fin_year_r','fin_month_r'].max().reset_index()

        # Apply merge using unique property id
        merge_maxfyrmth_uniqpid_df_Year_r = \
                                            property_fyrmth_r_max.merge(unique_pid_df, on='propertykey',how='inner')

        ## drop data if financial year is less than 2022
        filterdata_lessthan_2022 = merge_maxfyrmth_uniqpid_df_Year_r[merge_maxfyrmth_uniqpid_df_Year_r['fin_year_r'] != 2022]

        print("Executed the ptax procedure\n---------------------------------------------------------------"
              "----------------------------------------------------------------------")

        return merge_maxfyrmth_uniqpid_df_Year_r,filterdata_lessthan_2022,property_financial_yrmth_df

    def find_finyearmonth(self,property_data):
        #### find the financial year & Month
        property_data['fin_year_r'] = np.where(property_data['fin_month_r'] <4,property_data['fin_year_r']-1,property_data['fin_year_r'])
        property_data['fin_month_r'] = np.where(property_data['fin_month_r']<4,property_data['fin_month_r']+9,property_data['fin_month_r']-3)
        print("Identied the financial year & month using receipt date column\n---------------------------------------------------------------"
              "----------------------------------------------------------------------")

        fin_yearmonth_propertydf = property_data.copy()
        return fin_yearmonth_propertydf


class tax_process():

    def identify_billdetails(self, property_bill_df, property_list_df):
        ###-----------------------------------------------------------------------------------------------------------------
        ## Starting the Property bill data
        propertybill_df = property_bill_df[['propertykey', 'propertybillkey', 'financialyearkey', 'fromdate',
                                                     'billamount', 'balanceamount']]

        # propertybill_non0 = propertybill_df[propertybill_df['balanceamount'] > 0]
        propertybill_df['balanceamount'] = propertybill_df['balanceamount'].fillna(0)
        propertybill_non0pkey = propertybill_df[propertybill_df['propertykey'] > 0]
        ###-----------------------------------------------------------------------------------------------------------------
        # 152- 2022-2023
        # 153- 2023-2024

        ## Current Arrears till last years
        arrears_bill_details = propertybill_non0pkey[propertybill_non0pkey['financialyearkey'] != 153]
        arrears_bill = arrears_bill_details.groupby(['propertykey'])['balanceamount'].sum().reset_index()
        arrears_bill = arrears_bill.rename(columns={'balanceamount': 'Arrears'})

        # Only Current Year bills
        current_yr_bill = propertybill_non0pkey[propertybill_non0pkey['financialyearkey'] == 153]
        ty_bill = current_yr_bill.groupby(['propertykey'])['balanceamount'].sum().reset_index()
        ty_bill = ty_bill.rename(columns={'balanceamount': 'Current Bill'})

        ## Read Property list
        plist_df = property_list_df[['propertykey', 'propertycode', 'assessmentdate']]
        plist_df['propertykey'] = plist_df['propertykey'].drop_duplicates()
        plist_df['propertycode'] = plist_df['propertycode'].drop_duplicates()

        ## Merge Property list with cuurent arrears & arrears
        merge_plist_wtharrears = plist_df.merge(arrears_bill, on='propertykey', how='left')
        propertybill_detail = merge_plist_wtharrears.merge(ty_bill, on='propertykey', how='left')
        propertybill_detail['Current Bill'] = propertybill_detail['Current Bill'].fillna(0)

        return arrears_bill,ty_bill,propertybill_detail

    def identify_property_receipt(self, property_receipt_df, unique_pkey_df):

        ## Find the last receipt date of each property code
        last_receiptdate_pkey = property_receipt_df.sort_values(['propertykey', 'receiptdate']).drop_duplicates('propertykey', keep='last')
        last_receiptdate_pkey = last_receiptdate_pkey.drop(columns=['propertybillkey','modeofpayment','honoureddate'])
        last_receiptdate_pkey = last_receiptdate_pkey.rename(columns={'receiptdate':'last receipts date','paidamount':'last receipts amount'})

        return last_receiptdate_pkey

    def identify_no_of_receiptgenrated(self,property_receipt_df):
        no_of_receipt_generated = property_receipt_df.groupby(['propertykey'])['receiptdate'].count().reset_index()
        no_of_receipt_generated = no_of_receipt_generated.rename(columns={'receiptdate': 'number_of_receipt_generated'})

        return no_of_receipt_generated

    def ptax_process(self,maxfyr_uniqpkey_df,plist_selected_data):
        ###==================================================================================================================================
        maxfyr_uniqpkey_df.dropna(subset=['propertykey'], how='all', inplace=True)

        ## merge property list with max finacial yr & month
        merge_plist_maxuniqpid = plist_selected_data.merge(maxfyr_uniqpkey_df,
                                                                            on='propertykey', how='left')

        return merge_plist_maxuniqpid


    def data_process(self,mappath, outpth, property_data, property_list_df, last_receiptdate_pkey,
                     japti_flager, shasti_flags,bill_distributed_details,paidamount_ly,paidamount_ty):

        # Create a DataFrame 'plist_details_df' with columns from 'property_list_df'
        plist_details_df = pd.DataFrame(property_list_df,columns = ['propertykey', 'propertycode',
                                       'zone', 'gat', 'usetypekey','assessmentdate',
                                        'subusetypekey', 'constructiontypekey', 'occupancykey', 'own_mobile'])
        # Merge 'property_data' with 'plist_details_df' on 'propertycode'
        property_data_list = property_data.merge(plist_details_df,on='propertycode',how='left')

        # Load mapping data and assign to corresponding variables
        zonemap,usemap,construcmap,occpmap,subusemap,gatnamemap,splownmap,splaccmap,wrong_pid = read_data.mapping_data(mappath)

        # Map values in 'usetypekey', 'constructiontypekey', etc. columns using the created mappings
        property_data_list['Use_Type'] = property_data_list['usetypekey'].map(usemap)
        property_data_list['Construction_Type'] = property_data_list['constructiontypekey'].map(construcmap)
        property_data_list['Occupancy_Type'] = property_data_list['occupancykey'].map(occpmap)
        property_data_list['Subuse_Type'] = property_data_list['subusetypekey'].map(subusemap)
        property_data_list['Zone'] = property_data_list['zone'].map(zonemap)
        property_data_list['Gat'] = property_data_list['gat'].map(gatnamemap)

        # Create a new DataFrame 'property_data_list' with selected columns in the specified order.
        property_data_list = pd.DataFrame(property_data_list,columns=['propertycode', 'propertyname',
                                                     'propertyaddress','arrearsdemand', 'currentdemand', 'totaldemand', 'arrearspaid',
                                                        'currentpaid', 'totalpaid', 'arrearsbal', 'currentbal', 'totalbal',
                                                         'propertykey','assessmentdate', 'own_mobile',
                                                         'Use_Type', 'Construction_Type', 'Occupancy_Type', 'Subuse_Type',
                                                                'Zone', 'Gat'])

        # Rename columns 'arrearsdemand', 'currentdemand', and 'totaldemand'
        property_data_list = property_data_list.rename(columns={'arrearsdemand':'Arrears', 'currentdemand':'Current Bill', 'totaldemand':'Total_Amount'})

        # Merge 'property_data_list' with 'shasti_flags' DataFrame on 'propertykey'
        # Merge 'property_data_shasti' with 'japti_flager' DataFrame on 'propertycode'
        # Merge 'property_data_japti' with 'last_receiptdate_pkey' DataFrame on 'propertykey'
        property_data_shasti = property_data_list.merge(shasti_flags,on='propertykey',how='left')
        property_data_japti = property_data_shasti.merge(japti_flager,on='propertycode',how='left')
        property_data_lyreceipts = property_data_japti.merge(last_receiptdate_pkey,on='propertykey',how='left')

        # Define a function 'convert_mobilefmt' to extract and format mobile numbers in a DataFrame column
        def convert_mobilefmt(df,col_name):
            try:
                df[col_name] = df[col_name].str.extract(r'(\d{10})')
            except:
                pass
            df[col_name] = df[col_name].fillna(0000000000).astype("int64")
            df[col_name] = np.where((df[col_name] > 5999999999)
                                                              & (df[col_name] <= 9999999999),
                                                              df[col_name], np.nan)
            return df

        # Apply 'convert_mobilefmt' function to format 'own_mobile' column in 'property_data_lyreceipts'
        cleaned_property_data = convert_mobilefmt(property_data_lyreceipts,'own_mobile')

        # Merge 'cleaned_property_data' with 'bill_distributed_details' DataFrame on 'propertycode'
        # Fill missing values in 'own_mobile' with values from 'mobileUpdated' column
        cleaned_property_billsd = cleaned_property_data.merge(bill_distributed_details,on='propertycode',how='left')
        cleaned_propertydata_bills = convert_mobilefmt(cleaned_property_billsd,'mobileUpdated')
        cleaned_propertydata_bills['own_mobile'] = \
                                cleaned_propertydata_bills['own_mobile'].fillna(cleaned_propertydata_bills['mobileUpdated'])

        # Merge 'cleaned_propertydata_bills' with 'paidamount_ly' DataFrame on 'propertycode'
        # Merge 'property_onlylypaid' with 'paidamount_ty' DataFrame on 'propertycode'
        property_onlylypaid = cleaned_propertydata_bills.merge(paidamount_ly,on='propertycode',how='left')
        property_typaid= property_onlylypaid.merge(paidamount_ty,on='propertycode',how='left')

        # Apply 'flager.partiallypaid_Flag' function to 'property_typaid' DataFrame and store the result in 'property_data_partiallypaid'
        property_data_partiallypaid = flager.partiallypaid_Flag(property_typaid)

        # Extract and format the "Quarter" from the "last payment date" column using Q-Mar frequency
        property_data_partiallypaid["Quarter"] = pd.PeriodIndex(property_data_partiallypaid["last payment date"], freq="Q-Mar").strftime("Q%q")
        property_data_partiallypaid["Quarter"] = property_data_partiallypaid["Quarter"].fillna('defaulter')

        # Fill missing values in "last payment date" with values from "last receipts date"
        property_data_partiallypaid['last payment date'] = \
                                property_data_partiallypaid['last payment date'].fillna(property_data_partiallypaid['last receipts date'])

        #---------------------------------------------------------------------------------------------------------------
        # Select only required columns and avoid sorting if not necessary
        identical_col_df = pd.DataFrame(property_data_partiallypaid, columns=['propertykey', 'propertycode', 'Zone', 'Gat', 'own_mobile',
                                                                         'Arrears', 'Current Bill', 'Total_Amount',
                                                                          'propertyLat','propertyLong','visitDate',
                                                                           'last payment date','Quarter',
                                                                            'partiallypaid_Flag','paidLY_Flag','paidTY_Flag','shasti_Flag','last payment amount',
                                                                              'assessmentdate','Use_Type', 'Construction_Type','propertyname', 'propertyaddress',
                                                                              'Japti_Flag','status',
                                                                            'This Year Paidamount'])
        # Sorting the rows using propertykey column and replace the nan value with zeros in mentioned columns
        master_data = identical_col_df.sort_values('propertykey')
        master_data[['partiallypaid_Flag','paidLY_Flag','paidTY_Flag','shasti_Flag','Japti_Flag']] = \
                    master_data[['partiallypaid_Flag','paidLY_Flag','paidTY_Flag','shasti_Flag','Japti_Flag']].fillna(0)
        #---------------------------------------------------------------------------------------------------------------
        # Save the data to CSV (or you can uncomment the Excel save line if needed)
        master_data.to_csv(outpth + "/" + f"Master_Data.csv", index=False, encoding='utf-8-sig')
        # master_data.to_excel(outpth + "/" + f"Q2_Master_Data.xlsx", index=False,sheet_name="Q2_Master")

        print("Master Data preparation completed successfully.")
