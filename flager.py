import datetime
import pandas as pd
import datetime as dt
from datetime import datetime,timedelta
import numpy as np
import warnings
warnings.filterwarnings('ignore')

def partiallypaid_Flag(after_typaid_added_df):

    # non_zero_balanceamt = after_typaid_added_df[after_typaid_added_df['Total_Amount'] != 0]
    # after_typaid_added_df['percentage'] = round(
    #     (non_zero_balanceamt['This Year Paidamount'] / non_zero_balanceamt['Total_Amount']) * 100)

    # ## 0 - Fully Paid
    # ## 1 - Paritially Paid
    # after_typaid_added_df['partiallypaid_Flag'] = np.where(after_typaid_added_df['percentage'] > 90, 0, 1)
    after_typaid_added_df['diff'] = after_typaid_added_df['This Year Paidamount'] - after_typaid_added_df['Total_Amount']
    after_typaid_added_df['partiallypaid_Flag'] = np.where((after_typaid_added_df['diff'] <= -20), 1, 0)

    return after_typaid_added_df

def paid_ly_flag(filterdata_lessthan_2023):
    ## Paid in LY
    paidLY_Flag = filterdata_lessthan_2023[filterdata_lessthan_2023['fin_year_r'] == 2022].reset_index(drop=True)
    paidLY_Flag['paidLY_Flag'] = 1
    paidLY_Flag = paidLY_Flag.drop(columns=['fin_year_r', 'fin_month_r', 'fin_yearmth_r'])
    return paidLY_Flag

def japti_flag(japti_df,wrong_pid):

    for i,j in zip(wrong_pid['Wrong_pid'],wrong_pid['pid']):
        japti_df['propertycode'] = japti_df['propertycode'].str.replace(str(i), str(j))

    japti_df['propertycode'] = japti_df['propertycode'].str.replace('1040705608.00.00', '1040705608.00') \
        .str.replace('1150406630 .00', '1150406630.00').str.replace('`', '')

    japti_df['propertycode'] = japti_df['propertycode'].astype(float)
    japti_flags = japti_df.copy()
    japti_flags = japti_flags[['propertycode','status']]
    japti_flags = japti_flags.drop_duplicates('propertycode')
    japti_flags['Japti_Flag'] = 1

    return japti_flags