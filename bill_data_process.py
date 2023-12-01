import pandas as pd


def identify_billdetails(property_bill_df, property_list_df):
    ###-----------------------------------------------------------------------------------------------------------------
    ## Starting the Property bill data
    propertybill_df = property_bill_df[['propertykey', 'propertybillkey', 'financialyearkey', 'fromdate',
                                        'billamount', 'balanceamount']]
    propertybill_df.dropna(subset=['propertykey'], how='all', inplace=True)
    propertybill_df = propertybill_df[propertybill_df['balanceamount'] > 0]
    propertybill_df['balanceamount'] = propertybill_df['balanceamount'].fillna(0)
    propertybill_non0pkey = propertybill_df[propertybill_df['propertykey'] > 0]
    ###-----------------------------------------------------------------------------------------------------------------
    # 152- 2022-2023
    # 153- 2023-2024

    ## Current Arrears till last years
    arrears_bill_details = propertybill_non0pkey[propertybill_non0pkey['financialyearkey'] != 153]
    arrears_bill = arrears_bill_details.groupby(['propertykey'])['balanceamount'].sum().reset_index()
    arrears_bill = arrears_bill.rename(columns={'balanceamount': 'arrearsdemand'})

    # Only Current Year bills
    current_yr_bill = propertybill_non0pkey[propertybill_non0pkey['financialyearkey'] == 153]
    ty_bill = current_yr_bill.groupby(['propertykey'])['balanceamount'].sum().reset_index()
    ty_bill = ty_bill.rename(columns={'balanceamount': 'currentdemand'})

    # ## Read Property list
    # plist_df = property_list_df[['propertykey', 'propertycode','usetypekey','assessmentdate',
    #                                     'subusetypekey', 'constructiontypekey',
    #                                      'occupancykey', 'own_mobile','zone','gat','propertyname','propertyaddress']]
    # plist_df['propertykey'] = plist_df['propertykey'].drop_duplicates()
    # plist_df['propertycode'] = plist_df['propertycode'].drop_duplicates()
    #
    # ## Merge Property list with cuurent arrears & arrears
    # merge_plist_wtharrears = plist_df.merge(arrears_bill, on='propertykey', how='left')
    # propertybill_detail = merge_plist_wtharrears.merge(ty_bill, on='propertykey', how='left')
    # propertybill_detail[['currentdemand','arrearsdemand']] = propertybill_detail[['currentdemand','arrearsdemand']].fillna(0)
    # propertybill_detail['totaldemand'] = propertybill_detail[['currentdemand','arrearsdemand']].sum(axis=1)
    #
    # propertybill_detail['propertycode'] = propertybill_detail['propertycode'].drop_duplicates()
    # propertybill_detail['propertykey'] = propertybill_detail['propertykey'].drop_duplicates()

    return arrears_bill,ty_bill


def property_bill_demand(property_demand,property_list_df,arrears_bill,ty_bill):
    ###-----------------------------------------------------------------------------------------------------------------
    pdemand_df = property_demand[['propertykey', 'currentdemand','arrearsdemand', 'totaldemand']]

    ## Read Property list
    plist_df = property_list_df[['propertykey', 'propertycode','usetypekey','assessmentdate',
                                        'subusetypekey', 'constructiontypekey',
                                         'occupancykey', 'own_mobile','zone','gat','propertyname','propertyaddress','totalarea']]
    plist_df['propertykey'] = plist_df['propertykey'].drop_duplicates()
    plist_df['propertycode'] = plist_df['propertycode'].drop_duplicates()

    ## Merge Property list with cuurent arrears & arrears
    plist_pdemand = plist_df.merge(pdemand_df, on='propertykey', how='left')
    pdemand_arrearsdmd = plist_pdemand.merge(arrears_bill, on='propertykey', how='left')
    pdemand_cntdmd = pdemand_arrearsdmd.merge(ty_bill, on='propertykey', how='left')
    pdemand_cntdmd['currentdemand']  = pdemand_cntdmd['currentdemand_y'].fillna(pdemand_cntdmd['currentdemand_x'])
    pdemand_cntdmd['arrearsdemand']  = pdemand_cntdmd['arrearsdemand_y'].fillna(pdemand_cntdmd['arrearsdemand_x'])
    pdemand_cntdmd = pdemand_cntdmd.drop(columns=['currentdemand_y','currentdemand_x','arrearsdemand_y','arrearsdemand_x','totaldemand'])
    pdemand_cntdmd['totaldemand'] = pdemand_cntdmd[['currentdemand','arrearsdemand']].sum(axis=1)

    pdemand_cntdmd[['currentdemand', 'arrearsdemand', 'totaldemand']] =\
                pdemand_cntdmd[['currentdemand', 'arrearsdemand', 'totaldemand']].fillna(0)

    return pdemand_cntdmd

def cleaned_propertylist(property_list_df,wrong_pid):
    property_list_df = property_list_df[property_list_df['verified'] != 'N']
    property_list_df.dropna(subset=['propertycode'], how='all', inplace=True)
    property_list_df.dropna(subset=['propertykey'], how='all', inplace=True)
    property_list_df['propertykey'] = property_list_df['propertykey'].drop_duplicates()
    property_list_df['propertycode'] = property_list_df['propertycode'].drop_duplicates()

    for i,j in zip(wrong_pid['Wrong_pid'],wrong_pid['pid']):
        property_list_df['propertycode'] = property_list_df['propertycode'].str.replace(str(i), str(j))
    property_list_df['propertycode'] = property_list_df['propertycode'].replace('`', '')

    return property_list_df