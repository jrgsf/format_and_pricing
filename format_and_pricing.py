import pandas as pd
import datetime
from datetime import datetime as dt
import time # for testing
import sys
import calendar ## not strictly needed, but helpful for testing to see weekday names
import numpy as np
import itertools
from string import ascii_uppercase

from openpyxl import Workbook
from openpyxl import load_workbook
from openpyxl.writer.excel import ExcelWriter

### import all excel files.
xl = pd.ExcelFile("Raw Interval Data orig.xlsx")
xlo = pd.ExcelFile("Output.xlsx")
xlaux = pd.ExcelFile("Auxiliary_data.xlsx")
print "Excel file import complete -------------------"

## creates dataframe of every sheet in raw data doc
raw_dfs = {}
for s in xl.sheet_names:
    if 'summary' not in s:  ## only have one sheet for testing -- comment out when ready
        raw_dfs["raw_df_{0}".format(s)]=xl.parse(s)
        break ## only have one sheet for testing -- comment out when ready

## creates dataframe of initial Output file
orig_output_df = xlo.parse(xlo.sheet_names[0])

## creates dataframe of every sheet in Auxiliary_data.xlsx
aux_data = {}
for u in xlaux.sheet_names:
  aux_data["{0}".format(u)]=xlaux.parse(u)

## fixes error in empty Output file by rounding off timestamps (many end before the hour, have 59.999 seconds!)
fixed_empty_output = orig_output_df
for i, row in orig_output_df.iterrows():
  tm = orig_output_df.iat[i,0]
  tm_round_down = tm - datetime.timedelta(minutes=tm.minute % 15, seconds=tm.second, microseconds=tm.microsecond)
  if tm != tm_round_down:
    tm_fixed = tm_round_down + datetime.timedelta(minutes=15)
    fixed_empty_output.iat[i,0] = tm_fixed

####### ran all checks, but commented out for performance
### checks that no timestamps are missing in empty Output file (only consecutive 15min increments)
# for i, row in fixed_empty_output.iterrows():
#   if i > 0:
#     if fixed_empty_output.iat[i,0] != fixed_empty_output.iat[i-1,0] + + datetime.timedelta(minutes=15):
#       print 'FATAL ERROR: Output.xlsx file timestamps not incrementing by 15min: ', fixed_empty_output.iat[i,0], fixed_empty_output.iat[i-1,0]
#       sys.exit()
# print 'consecutive timestamp check complete'

####### ran all checks, but commented out for performance
#### check that all 15min columns present
# def check_for_all_columns_in_raw_file(raw_file):
#   check_time = raw_file.iat[0,8] + datetime.timedelta(hours=0,minutes=15)
#   # check_time = dt.strptime('00:00:00', '%H:%M:%S') + datetime.timedelta(hours=0,minutes=15)
#   for col_name in list(raw_file)[10:106]:
#     hr = int(col_name.split("_")[1][:2])
#     min = int(col_name.split("_")[1][2:])
#     if check_time != raw_file.iat[0,8] + datetime.timedelta(hours=hr,minutes=min):
#       print "FATAL ERROR: ", check_time, raw_file.iat[0,8] + datetime.timedelta(hours=hr,minutes=min)
#       sys.exit()
#     check_time += datetime.timedelta(hours=0,minutes=15)
#
# for i, raw in raw_dfs.iteritems():
#     if 'summary' not in i:
#         check_for_all_columns_in_raw_file(raw)

#####  assigns values from raw data to output file for columns CHNL_ID 101, 102
#####  calculates net usage
#####  warns if any data non-numeric




#### if >1 row in a raw file have the same date, determines whether all data is identical
def confirm_duplicate_row(dupe_rows, day_only):
  fatal = False
  rc = len(dupe_rows)-1
  while rc > 0:
    for c in range(10, 106):
      if dupe_rows.iloc[rc][c] != dupe_rows.iloc[rc-1][c]:
        print "Multiple rows of raw data for date {0} at {1}: {2} != {3}".format(day_only, dupe_rows.columns[c], dupe_rows.iloc[rc][c], dupe_rows.iloc[rc-1][c])
        fatal = True
        break
    rc -= 1
  return fatal

def populate_output(empty_output, raw):
  print "starting, copying empty_output"
  populated_output = empty_output
  extra_row_warning = []
  for i, row in empty_output.iterrows():
    print "starting row loop of ", i, row[0]
    yesterday = False
    if row[0] == row[0] - datetime.timedelta(hours=row[0].hour, minutes=row[0].minute):
      day_only = row[0] - datetime.timedelta(days=1)
      yesterday = True
    else:
      day_only = row[0] - datetime.timedelta(hours=row[0].hour, minutes=row[0].minute)
    # hr = str(row[0])[11:13]
    # min = str(row[0])[14:16]
    import_row = raw[(raw.INTRVL_DATE == day_only) & (raw.CHNL_ID == 101)]
    export_row = raw[(raw.INTRVL_DATE == day_only) & (raw.CHNL_ID == 102)]
    ### deals with cases where >1 row per date per CHNL_ID
    if len(import_row) > 1 or len(export_row) > 1:
      if [day_only, len(import_row), len(export_row)] not in extra_row_warning:
        extra_row_warning.append([day_only, len(import_row), len(export_row)])
      confirm_import_dupe = confirm_duplicate_row(import_row, day_only)
      confirm_export_dupe = confirm_duplicate_row(export_row, day_only)
      if confirm_import_dupe or confirm_export_dupe:
        print 'FATAL ERROR: Extra rows for {0} contain different raw data.'.format(day_only)
        sys.exit()
    ### cycles through all columns with 15minute usage data
    print "starting col loop"
    for col in range (10, 106):
        try:
          float(import_row.iloc[0][col])
          print "data point: ", import_row.iloc[0][col]
          populated_output.at[i, 'CHNL_ID 101 (kW)'] = import_row.iloc[0][col] * 4
        except:
          populated_output.at[i, 'CHNL_ID 101 (kW)'] = np.nan
          print "WARNING: non-numerical raw data '{0}' at {1}, CHNL_ID 101, {2}".format(import_row.iloc[0][col], day_only, col)
        try:
          float(export_row.iloc[0][col])
          populated_output.at[i, 'CHNL_ID 102 (kW)'] = export_row.iloc[0][col] * 4
          populated_output.at[i, 'Net Usage (kWh)'] = import_row.iloc[0][col] - export_row.iloc[0][col]
        except:
          populated_output.at[i, 'CHNL_ID 102 (kW)'] = np.nan
          print "WARNING: non-numerical raw data '{0}' at {1}, CHNL_ID 102, {2}".format(export_row.iloc[0][col], day_only, col)
  ### adds final row with totals
  populated_output = populated_output.append(populated_output.sum(numeric_only=True), ignore_index=True)
  if extra_row_warning:
    for w in extra_row_warning:
      print 'WARNING: Raw data contains too many rows for {0}: {1} for CHNL_ID 101; {2} for CHNL_ID 102 \nData in duplicated rows is identical in all columns. This will not affect output.'.format(w[0], w[1], w[2])
  return populated_output



# tests:
# populate_output(tt, bad10)
# populate_output(tt, mini10)
# populate_output(empty_output_dfs["Account 2"], raw_dfs["raw_df_2"])
# populate_output(empty_output_dfs["Account 4"], raw_dfs["raw_df_4"])
# populate_output(empty_output_dfs["Account 6"], raw_dfs["raw_df_6"])
# populate_output(empty_output_dfs["Account 9"], raw_dfs["raw_df_9"])
# populate_output(empty_output_dfs["Account 10"], raw_dfs["raw_df_10"])

empty_output_dfs = {}
for k in raw_dfs.keys():
  if 'summary' not in k:
    empty_output_dfs["Account {0}".format(k[7:])]=fixed_empty_output

writer = pd.ExcelWriter('Output Part I -python.xlsx')

final_output_files = {}
for rk, rv in raw_dfs.iteritems():
  for pk, pv in empty_output_dfs.iteritems():
    if rk[7:] == pk[8:]:
      print "Begin Processing raw file for {0}".format(pk)
      print rk, pk
      final_output_files[pk] = populate_output(pv, rv)
      pv.to_excel(writer, pk)
      worksheet = writer.sheets[pk]
      for a in ascii_uppercase:
        worksheet.set_column('{0}:{1}'.format(a,a), 24)
      print "Output file for {0} ready".format(pk)
      print "#############################################"

writer.save()



#############################################
##### calculate rates


def calculate_season_and_peak_status(row, rates, cpp, holidays):
  actual_holiday = None
  if row[0].month >= 11 or row[0].month <= 4:
    winter = True
  else: winter = False
  if row[0].weekday() > 4:
    weekend = True
  else: weekend = False
  for hi, hol in holidays.iterrows():
    if row[0] - datetime.timedelta(hours=row[0].hour, minutes=row[0].minute) == hol[0]:
      holiday = True
      actual_holiday = hol[2]
    else: holiday = False
  for i, p in aux_data["peak"].iterrows():
    if p[0] == "Winter" and winter == True and weekend == False and holiday == False and row[0].time() >= p[2] and row[0].time() < p[3]:
      peak = p[1]
      break
    elif p[0] != "Winter" and winter == False and weekend == False and holiday == False and row[0].time() >= p[2] and row[0].time() < p[3]:
      peak = p[1]
      break
    else: peak = "Off-Peak"
  # print "{0} winter={1} {2} on wknd? {3} {4} {5}".format(row[0], winter, calendar.day_name[row[0].weekday()], weekend, actual_holiday, peak)
  return winter, weekend, holiday, peak

def determine_rate(winter, peak, all_rates):
  peak = peak.rstrip().lstrip()
  if winter == False:
    if peak == "On-Peak":
      summer_on_peak = aux_data["rates"][(aux_data["rates"].season == "Summer") & (aux_data["rates"].status == "On-Peak")]
      secondary_rate = summer_on_peak.iloc[0,2] # 0.11776
      primary_rate = summer_on_peak.iloc[0,3] # 0.11715
      transmission_rate = summer_on_peak.iloc[0,4] # 0.11199
    elif peak == "Semi-Peak" :
      summer_semi_peak = aux_data["rates"][(aux_data["rates"].season == "Summer") & (aux_data["rates"].status == "Semi-Peak")]
      secondary_rate = summer_semi_peak.iloc[0,2] # 0.10803
      primary_rate = summer_semi_peak.iloc[0,3] # 0.10752
      transmission_rate = summer_semi_peak.iloc[0,4] # 0.10297
    else: # peak == "Off-Peak"
      summer_off_peak = aux_data["rates"][(aux_data["rates"].season == "Summer") & (aux_data["rates"].status == "Off-Peak")]
      secondary_rate = summer_off_peak.iloc[0,2] # 0.07724
      primary_rate = summer_off_peak.iloc[0,3] # 0.07696
      transmission_rate = summer_off_peak.iloc[0,4] # 0.07384
  else: # winter = True
    if peak == "On-Peak":
      winter_on_peak = aux_data["rates"][(aux_data["rates"].season == "Winter") & (aux_data["rates"].status == "On-Peak")]
      secondary_rate = winter_on_peak.iloc[0,2] # 0.10595
      primary_rate = winter_on_peak.iloc[0,3] # 0.10543
      transmission_rate = winter_on_peak.iloc[0,4] # 0.10090
    elif peak == "Semi-Peak":
      winter_semi_peak = aux_data["rates"][(aux_data["rates"].season == "Winter") & (aux_data["rates"].status == "Semi-Peak")]
      secondary_rate = winter_semi_peak.iloc[0,2] # 0.09040
      primary_rate = winter_semi_peak.iloc[0,3] # 0.09000
      transmission_rate = winter_semi_peak.iloc[0,4] # 0.08626
    else: # peak == "Off-Peak"
      winter_off_peak = aux_data["rates"][(aux_data["rates"].season == "Winter") & (aux_data["rates"].status == "Off-Peak")]
      secondary_rate = winter_off_peak.iloc[0,2] # 0.06898
      primary_rate = winter_off_peak.iloc[0,3] # 0.06875
      transmission_rate = winter_off_peak.iloc[0,4] # 0.06598
  return secondary_rate, primary_rate, transmission_rate

def check_cpp(billing_period):
  for i, period in aux_data["CPP_events"].iterrows():
    if billing_period > period[0] and billing_period - datetime.timedelta(minutes=15) < period[1]:
      cpp = True
      # print "{0} btwn {1} and {2}".format(billing_period, period[0], period[1])
      break
    else: cpp = False
  return cpp

def calculate_cost(rates, usage, cpp):
  costs = []
  for rate in rates:
    costs.append(rate / 4 * usage)
  for n in range(0, 3):
    costs.append(costs[n] + (aux_data["CPP_adders"].iloc[n,1] / 4))
  return costs

def all_calculations(output_file_to_mod):
  worksheet_output = output_file_to_mod
  for i, output_line in worksheet_output.iterrows():
    season_and_peak_status = calculate_season_and_peak_status(output_line, aux_data["rates"], aux_data["CPP_events"], aux_data["holidays"])
    ## returns winter, weekend, holiday, peak
    rates = determine_rate(season_and_peak_status[0], season_and_peak_status[3], aux_data["rates"])
    # return secondary_rate, primary_rate, transmission_rate
    cpp = check_cpp(output_line[0])
    # return boolean
    cost = calculate_cost(rates, output_line[3], cpp)
    # return secondary_cost, primary_cost, transmission_cost, secondary_cpp_cost, primary_cpp_cost, transmission_cpp_cost
    worksheet_output.at[i, '']= np.nan  # blank column for ease of reading
    worksheet_output.at[i, 'Winter?']= season_and_peak_status[0]
    worksheet_output.at[i, 'Weekend?']= season_and_peak_status[1]
    worksheet_output.at[i, 'Holiday?']= season_and_peak_status[2]
    worksheet_output.at[i, 'Peak?']= season_and_peak_status[3]
    worksheet_output.at[i, '-']= np.nan  # blank column for ease of reading
    worksheet_output.at[i, 'Secondary rate']= rates[0]
    worksheet_output.at[i, 'Primary rate']= rates[1]
    worksheet_output.at[i, 'Transmission rate']= rates[2]
    worksheet_output.at[i, 'Secondary cost']= cost[0]
    worksheet_output.at[i, 'Primary cost']= cost[1]
    worksheet_output.at[i, 'Transmission cost']= cost[2]
    worksheet_output.at[i, '--']= np.nan # blank column for ease of reading
    worksheet_output.at[i, 'During CPP?']= cpp
    worksheet_output.at[i, 'Secondary cost + Adder']= cost[3]
    worksheet_output.at[i, 'Primary cost + Adder']= cost[4]
    worksheet_output.at[i, 'Transmission cost + Adder']= cost[5]
  worksheet_output = worksheet_output.append(worksheet_output.sum(numeric_only=True), ignore_index=True)
  return worksheet_output

writer = pd.ExcelWriter('Output Worksheet-Python.xlsx')

master_summary = pd.DataFrame()

def add_to_master_summary(name, f):
    for i in range(1, len(final_output_files)+1):
        master_summary.at[i, 'NM_CUST'] = 'Happy Customer'
        master_summary.at[i, 'ACCT_NBR'] = int(name[8:])
        master_summary.at[i, 'Net Usage (kWh)'] = f.iat[f.shape[0]-1,3]
        master_summary.at[i, 'Secondary cost'] =  f.iat[f.shape[0]-1,3]
        master_summary.at[i, 'Primary cost'] =  f.iat[f.shape[0]-1,3]
        master_summary.at[i, 'Transmission cost'] = f.iat[f.shape[0]-1,3]
        master_summary.at[i, ''] = np.nan # blank column for ease of reading
        master_summary.at[i, 'Secondary cost + Adder'] = f.iat[f.shape[0]-1,3]
        master_summary.at[i, 'Primary cost + Adder'] = f.iat[f.shape[0]-1,3]
        master_summary.at[i, 'Transmission cost + Adder'] = f.iat[f.shape[0]-1,3]

for name, outpt_f in final_output_files.items():
  print "Calculating costs for {0}".format(name)
  all_calculations(outpt_f)
  print "{0} complete".format(name)
  print "============================================================="
  outpt_f.to_excel(writer, name)
  worksheet = writer.sheets[name]
  for a in ascii_uppercase:
    worksheet.set_column('{0}:{1}'.format(a,a), 24)
  add_to_master_summary(name, outpt_f)

master_summary.to_excel(writer, "master summary")
master_worksheet = writer.sheets["master summary"]
for a in ascii_uppercase:
    master_worksheet.set_column('{0}:{1}'.format(a,a), 24)

writer.save()
