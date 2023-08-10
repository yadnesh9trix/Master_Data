# import required libraries
import pandas as pd
import os
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')
import Ptax_operation as pto
import read_data as rd
import flager as filtr_flag

# ----------------------------------------------------------------------------------------------------------------------
# Define today's date
today = datetime.today().date()

# Date format's
tday_dbyfmt = today.strftime("%d_%b_%Y")
# tday_dmyfmt = today.strftime("%d%m%Y%H%M%p")
tday_dmyfmt = datetime.today().strftime("%d%m%Y")

# Define the that day's date
day = today - timedelta(days=1)
day_ddmmyyyy = day.strftime("%d%m%Y")

# Fetching class object of property tax data process.
ptprocess = pto.ptax_activity()
read_data = rd.GatherData()
tax_procedure = pto.tax_process()

def MasterData(inppath,outpth,paidamount_file,tax_data):

    # Read all property related master data mapping files such as zone,gat,usage type ...
    zonemap, usemap, construcmap, occpmap, subusemap, \
        gatnamemap, splownmap, splaccmap, wrong_pid = read_data.mapping_data(mappath)

    # Defined the property bill details, receipt details and property list details.
    property_data,\
            property_receipt_df, \
                        shasti_flags, \
                            bill_distributed_details,\
                                        japtinotice_data = read_data.execute_data(inppath,tax_data)

    # Process Property deatails list.
    property_list_df = read_data.property_list(tax_data)

    # Identify the distinct property key using property list details.
    unique_propertykey = read_data.identify_unique_key_property(property_list_df)

    # Excecuting the Today's Paid Amount Data.
    paidamount_ty = read_data.read_paidamount_data(paidamount_file, day_ddmmyyyy)

    # Excecuting the last year's Paid Amount Data.
    paidamount_ly = read_data.read_paidamount_LY(tax_data)

    # Japti Notice Flaggers
    japti_flagger =  filtr_flag.japti_flag(japtinotice_data,wrong_pid)

    # Identify the property receipts of each property.
    last_receiptdate_pkey =  tax_procedure.identify_property_receipt(property_receipt_df, unique_propertykey)

    # Master Data process using various inputs.
    tax_procedure.data_process(tday_dmyfmt,mappath,outpth, property_data, property_list_df, last_receiptdate_pkey,
                               japti_flagger,shasti_flags,bill_distributed_details,paidamount_ly,paidamount_ty)

# Start
if __name__ == '__main__':
    main_path = r"D:/"
    std_path = r"D:\Master Data/"
    inppath = std_path + "Input/"
    outpth = std_path + "Output/" + tday_dbyfmt + "/"
    paidamount_file = main_path + "Paidamount/"
    tax_data = main_path + "/Tax_Data/"
    os.makedirs(outpth,exist_ok=True)

    mappath = std_path + "Mapping/"
    print('Your ptax model is running.\nPlease wait...\n============================='
          '===============================================================================')

    MasterData(inppath,outpth,paidamount_file,tax_data)