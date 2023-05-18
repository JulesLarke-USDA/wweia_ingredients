# Import packages
import pandas as pd

# Download & Read SAS Transport Files from web
# Demographic, Dietary day 1, and Food description data for cycles 01 through 18
demo_B = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2001-2002/demo_b.xpt', format='xport', encoding='utf-8')
iff_1_B = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2001-2002/drxiff_b.xpt', format='xport', encoding='utf-8')
food_desc_B = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2001-2002/drxfmt_b.xpt', format='xport', encoding='utf-8')
food_desc_B = food_desc_B.drop(columns='FMTNAME').rename(columns={'START': 'DRXFDCD', 'LABEL': 'DRXFCSD'})
food_desc_B['DRXFCLD'] = food_desc_B['DRXFCSD'] # Create duplicate column of short description labeled as long description for purpose of binding dataframes

demo_C = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2003-2004/demo_c.xpt', format='xport', encoding='utf-8')
iff_1_C = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2003-2004/dr1iff_c.xpt', format='xport', encoding='utf-8')
food_desc_C = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2003-2004/drxfcd_c.xpt', format='xport', encoding='utf-8')

demo_D = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2005-2006/demo_d.xpt', format='xport', encoding='utf-8')
iff_1_D = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2005-2006/dr1iff_d.xpt', format='xport', encoding='utf-8')
food_desc_D = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2005-2006/drxfcd_d.xpt', format='xport', encoding='utf-8')

demo_E = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2007-2008/DEMO_E.xpt', format='xport', encoding='utf-8')
iff_1_E = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2007-2008/DR1IFF_E.xpt', format='xport', encoding='utf-8')
food_desc_E = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2007-2008/drxfcd_e.xpt', format='xport', encoding='utf-8')

demo_F = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2009-2010/DEMO_f.xpt', format='xport', encoding='utf-8')
iff_1_F = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2009-2010/DR1IFF_f.xpt', format='xport', encoding='utf-8')
food_desc_F = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2009-2010/drxfcd_f.xpt', format='xport', encoding='utf-8')

demo_G = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2011-2012/DEMO_g.xpt', format='xport', encoding='utf-8')
iff_1_G = pd.read_sas('https://wwwn.cdc.gov/Nchs/Nhanes/2011-2012/DR1IFF_G.XPT', format='xport', encoding='utf-8')
food_desc_G = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2011-2012/drxfcd_g.xpt', format='xport', encoding='utf-8')

demo_H = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2013-2014/DEMO_h.xpt', format='xport', encoding='utf-8')
iff_1_H = pd.read_sas('https://wwwn.cdc.gov/nchs/nhanes/2013-2014/DR1IFF_H.XPT', format='xport', encoding='utf-8')
food_desc_H = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2013-2014/drxfcd_h.xpt', format='xport', encoding='latin-1') # error: 'utf-8' codec can't decode. using 'latin-1' works

demo_I = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2015-2016/DEMO_I.xpt', format='xport', encoding='utf-8')
iff_1_I = pd.read_sas('https://wwwn.cdc.gov/nchs/nhanes/2015-2016/DR1IFF_I.XPT', format='xport', encoding='utf-8')
food_desc_I = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2015-2016/drxfcd_i.xpt', format='xport', encoding='utf-8')

demo_J = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2017-2018/DEMO_J.xpt', format='xport', encoding='utf-8')
iff_1_J = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2017-2018/DR1IFF_J.xpt', format='xport', encoding='utf-8')
food_desc_J = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2017-2018/drxfcd_j.xpt', format='xport', encoding='utf-8')

# select variables of interest from demographic and dietary data
demo_B = demo_B[['SEQN', 'RIAGENDR', 'RIDAGEYR', 'RIDRETH1', 'DMDBORN', 'DMDYRSUS', 'DMDEDUC3', 'DMDEDUC2', 'WTINT2YR', 'WTMEC2YR', 'SDMVPSU', 'SDMVSTRA']]
iff_1_B = iff_1_B[['SEQN', 'DRXILINE', 'DRDIFDCD', 'DRD030Z', 'WTDRD1', 'DRXIGRMS', 'DRXIKCAL', 'DRXICARB', 'DRXISUGR', 'DRXIFIBE']].rename(columns={'DRXILINE':'DR1ILINE','DRDIFDCD':'DRXFDCD','DRD030Z':'DR1_030Z','DRXIGRMS':'DR1IGRMS','DRXIKCAL':'DR1IKCAL','DRXICARB':'DR1ICARB','DRXISUGR':'DR1ISUGR','DRXIFIBE':'DR1IFIBE'})
food_desc_B.drop(columns='DRXFCSD', inplace=True)
food_desc_B.loc[-1] = [94000000, 'WATER AS AN INGREDIENT'] # add this food code and description since it was not included for some reason
recall_1_B = pd.merge(food_desc_B, iff_1_B, on = 'DRXFDCD')
recall_1_B_ = pd.merge(recall_1_B, demo_B, on = 'SEQN')
recall_1_B_['CYCLE'] = '01_02'
recall_1_B_['RIAGENDR'].replace({1 : 'Male', 2 : 'Female'}, inplace=True)
recall_1_B_['RIDRETH1'].replace({1 : "Mexican_American", 2 : "Other_Hispanic", 3 : "Non-Hispanic_White", 4 : 'Non-Hispanic_Black', 5 : "Other_Multi-Racial"}, inplace=True)
recall_1_B_['DMDBORN'].replace({1 : "US", 2 : "Mexico", 3 : "Elsewhere", 7 : 'Unknown'}, inplace=True)
recall_1_B_['DR1_030Z'].replace({
  1 : 'Breakfast',
  2 : 'Lunch',
  3 : 'Dinner',
  5 : 'Brunch',
  6 : 'Snack',
  8 : 'Infant feeding',
  9 : 'Extended consumption',
  10 : 'Breakfast',
  11 : 'Breakfast',
  12 : 'Lunch',
  13 : 'Snack',
  14 : 'Dinner',
  15 : 'Snack',
  16 : 'Snack',
  17 : 'Snack',
  91 : 'Unknown',
  99 : 'Unknown'
}, inplace=True)

demo_C = demo_C[['SEQN', 'RIAGENDR', 'RIDAGEYR', 'RIDRETH1', 'DMDBORN', 'DMDYRSUS', 'DMDEDUC3', 'DMDEDUC2', 'WTINT2YR', 'WTMEC2YR', 'SDMVPSU', 'SDMVSTRA']]
iff_1_C = iff_1_C[['SEQN', 'DR1ILINE', 'DR1IFDCD', 'DR1_030Z', 'WTDRD1', 'DR1IGRMS', 'DR1IKCAL', 'DR1ICARB', 'DR1ISUGR', 'DR1IFIBE']].rename(columns={'DR1IFDCD':'DRXFDCD'})
food_desc_C.drop(columns='DRXFCSD', inplace=True)
recall_1_C = pd.merge(food_desc_C, iff_1_C, on = 'DRXFDCD')
recall_1_C_ = pd.merge(recall_1_C, demo_C, on = 'SEQN')
recall_1_C_['CYCLE'] = '03_04'
recall_1_C_['RIAGENDR'].replace({1 : 'Male', 2 : 'Female'}, inplace=True)
recall_1_C_['RIDRETH1'].replace({1 : "Mexican_American", 2 : "Other_Hispanic", 3 : "Non-Hispanic_White", 4 : 'Non-Hispanic_Black', 5 : "Other_Multi-Racial"}, inplace=True)
recall_1_C_['DMDBORN'].replace({1 : "US", 2 : "Mexico", 3 : "Elsewhere", 7 : 'Unknown'}, inplace=True)
recall_1_C_['DR1_030Z'].replace({
  1 : 'Breakfast',
  2 : 'Lunch',
  3 : 'Dinner',
  4 : 'Dinner',
  5 : 'Brunch',
  6 : 'Snack',
  7 : 'Drink',
  8 : 'Infant feeding',
  9 : 'Extended consumption',
  10 : 'Breakfast',
  11 : 'Breakfast',
  12 : 'Lunch',
  13 : 'Snack',
  14 : 'Dinner',
  15 : 'Snack',
  16 : 'Snack',
  17 : 'Snack',
  18 : 'Snack',
  19 : 'Drink',
  91 : 'Unknown',
  99 : 'Unknown'
}, inplace=True)

demo_D = demo_D[['SEQN', 'RIAGENDR', 'RIDAGEYR', 'RIDRETH1', 'DMDBORN', 'DMDYRSUS', 'DMDEDUC3', 'DMDEDUC2', 'WTINT2YR', 'WTMEC2YR', 'SDMVPSU', 'SDMVSTRA']]
iff_1_D = iff_1_D[['SEQN', 'DR1ILINE', 'DR1IFDCD', 'DR1_030Z', 'WTDRD1', 'DR1IGRMS', 'DR1IKCAL', 'DR1ICARB', 'DR1ISUGR', 'DR1IFIBE']].rename(columns={'DR1IFDCD':'DRXFDCD'})
food_desc_D.drop(columns='DRXFCSD', inplace=True)
recall_1_D = pd.merge(food_desc_D, iff_1_D, on = 'DRXFDCD')
recall_1_D_ = pd.merge(recall_1_D, demo_D, on = 'SEQN')
recall_1_D_['CYCLE'] = '05_06'
recall_1_D_['RIAGENDR'].replace({1 : 'Male', 2 : 'Female'}, inplace=True)
recall_1_D_['RIDRETH1'].replace({1 : "Mexican_American", 2 : "Other_Hispanic", 3 : "Non-Hispanic_White", 4 : 'Non-Hispanic_Black', 5 : "Other_Multi-Racial"}, inplace=True)
recall_1_D_['DMDBORN'].replace({1 : "US", 2 : "Mexico", 3 : "Elsewhere", 7 : 'Unknown'}, inplace=True)
recall_1_D_['DR1_030Z'].replace({
  1 : 'Breakfast',
  2 : 'Lunch',
  3 : 'Dinner',
  4 : 'Dinner',
  5 : 'Brunch',
  6 : 'Snack',
  7 : 'Drink',
  8 : 'Infant feeding',
  9 : 'Extended consumption',
  10 : 'Breakfast',
  11 : 'Breakfast',
  12 : 'Lunch',
  13 : 'Snack',
  14 : 'Dinner',
  15 : 'Snack',
  16 : 'Snack',
  17 : 'Snack',
  18 : 'Snack',
  19 : 'Drink',
  91 : 'Unknown',
  99 : 'Unknown'
}, inplace=True)

demo_E = demo_E[['SEQN', 'RIAGENDR', 'RIDAGEYR', 'RIDRETH1', 'DMDBORN2', 'DMDYRSUS', 'DMDEDUC3', 'DMDEDUC2', 'WTINT2YR', 'WTMEC2YR', 'SDMVPSU', 'SDMVSTRA']].rename(columns={'DMDBORN2':'DMDBORN'})
iff_1_E = iff_1_E[['SEQN', 'DR1ILINE', 'DR1IFDCD', 'DR1_030Z', 'WTDRD1', 'DR1IGRMS', 'DR1IKCAL', 'DR1ICARB', 'DR1ISUGR', 'DR1IFIBE']].rename(columns={'DR1IFDCD':'DRXFDCD'})
food_desc_E.drop(columns='DRXFCSD', inplace=True)
recall_1_E = pd.merge(food_desc_E, iff_1_E, on = 'DRXFDCD')
recall_1_E_ = pd.merge(recall_1_E, demo_E, on = 'SEQN')
recall_1_E_['CYCLE'] = '07_08'
recall_1_E_['RIAGENDR'].replace({1 : 'Male', 2 : 'Female'}, inplace=True)
recall_1_E_['RIDRETH1'].replace({1 : "Mexican_American", 2 : "Other_Hispanic", 3 : "Non-Hispanic_White", 4 : 'Non-Hispanic_Black', 5 : "Other_Multi-Racial"}, inplace=True)
recall_1_E_['DMDBORN'].replace({1 : "US", 2 : "Mexico", 4 : "Other_Spanish_Speaking_Country", 5 : 'Other_Non-Spanish_Speaking_Country', 7 : 'Unknown', 9 : 'Unknown'}, inplace=True)
recall_1_E_['DR1_030Z'].replace({
  1 : 'Breakfast',
  2 : 'Lunch',
  3 : 'Dinner',
  5 : 'Brunch',
  6 : 'Snack',
  8 : 'Infant feeding',
  9 : 'Extended consumption',
  10 : 'Breakfast',
  11 : 'Breakfast',
  12 : 'Lunch',
  13 : 'Snack',
  14 : 'Dinner',
  15 : 'Snack',
  16 : 'Snack',
  17 : 'Snack',
  91 : 'Unknown',
  99 : 'Unknown'
}, inplace=True)

demo_F = demo_F[['SEQN', 'RIAGENDR', 'RIDAGEYR', 'RIDRETH1', 'DMDBORN2', 'DMDYRSUS', 'DMDEDUC3', 'DMDEDUC2', 'WTINT2YR', 'WTMEC2YR', 'SDMVPSU', 'SDMVSTRA']].rename(columns={'DMDBORN2':'DMDBORN'})
iff_1_F = iff_1_F[['SEQN', 'DR1ILINE', 'DR1IFDCD', 'DR1_030Z', 'WTDRD1', 'DR1IGRMS', 'DR1IKCAL', 'DR1ICARB', 'DR1ISUGR', 'DR1IFIBE']].rename(columns={'DR1IFDCD':'DRXFDCD'})
food_desc_F.drop(columns='DRXFCSD', inplace=True)
recall_1_F = pd.merge(food_desc_F, iff_1_F, on = 'DRXFDCD')
recall_1_F_ = pd.merge(recall_1_F, demo_F, on = 'SEQN')
recall_1_F_['CYCLE'] = '09_10'
recall_1_F_['RIAGENDR'].replace({1 : 'Male', 2 : 'Female'}, inplace=True)
recall_1_F_['RIDRETH1'].replace({1 : "Mexican_American", 2 : "Other_Hispanic", 3 : "Non-Hispanic_White", 4 : 'Non-Hispanic_Black', 5 : "Other_Multi-Racial"}, inplace=True)
recall_1_F_['DMDBORN'].replace({1 : "US", 2 : "Mexico", 4 : "Other_Spanish_Speaking_Country", 5 : 'Other_Non-Spanish_Speaking_Country', 7 : 'Unknown', 9 : 'Unknown'}, inplace=True)
recall_1_F_['DR1_030Z'].replace({
  1 : 'Breakfast',
  2 : 'Lunch',
  3 : 'Dinner',
  4 : 'Dinner',
  5 : 'Brunch',
  6 : 'Snack',
  7 : 'Drink',
  8 : 'Infant feeding',
  9 : 'Extended consumption',
  10 : 'Breakfast',
  11 : 'Breakfast',
  12 : 'Lunch',
  13 : 'Snack',
  14 : 'Dinner',
  15 : 'Snack',
  16 : 'Snack',
  17 : 'Snack',
  18 : 'Snack',
  19 : 'Drink',
  91 : 'Unknown',
  99 : 'Unknown'
}, inplace=True)

demo_G = demo_G[['SEQN', 'RIAGENDR', 'RIDAGEYR', 'RIDRETH1', 'DMDBORN4', 'DMDYRSUS', 'DMDEDUC3', 'DMDEDUC2', 'WTINT2YR', 'WTMEC2YR', 'SDMVPSU', 'SDMVSTRA']].rename(columns={'DMDBORN4':'DMDBORN'})
iff_1_G = iff_1_G[['SEQN', 'DR1ILINE', 'DR1IFDCD', 'DR1_030Z', 'WTDRD1', 'DR1IGRMS', 'DR1IKCAL', 'DR1ICARB', 'DR1ISUGR', 'DR1IFIBE']].rename(columns={'DR1IFDCD':'DRXFDCD'})
food_desc_G.drop(columns='DRXFCSD', inplace=True)
recall_1_G = pd.merge(food_desc_G, iff_1_G, on = 'DRXFDCD')
recall_1_G_ = pd.merge(recall_1_G, demo_G, on = 'SEQN')
recall_1_G_['CYCLE'] = '11_12'
recall_1_G_['RIAGENDR'].replace({1 : 'Male', 2 : 'Female'}, inplace=True)
recall_1_G_['RIDRETH1'].replace({1 : "Mexican_American", 2 : "Other_Hispanic", 3 : "Non-Hispanic_White", 4 : 'Non-Hispanic_Black', 5 : "Other_Multi-Racial"}, inplace=True)
recall_1_G_['DMDBORN'].replace({1 : "US", 2 : "Elsewhere", 77 : 'Unknown', 99 : 'Unknown'}, inplace=True)
recall_1_G_['DR1_030Z'].replace({
  1 : 'Breakfast',
  2 : 'Lunch',
  3 : 'Dinner',
  4 : 'Dinner',
  5 : 'Brunch',
  6 : 'Snack',
  7 : 'Drink',
  8 : 'Infant feeding',
  9 : 'Extended consumption',
  10 : 'Breakfast',
  11 : 'Breakfast',
  12 : 'Lunch',
  13 : 'Snack',
  14 : 'Dinner',
  15 : 'Snack',
  16 : 'Snack',
  17 : 'Snack',
  18 : 'Snack',
  19 : 'Drink',
  91 : 'Unknown',
  99 : 'Unknown'
}, inplace=True)

demo_H = demo_H[['SEQN', 'RIAGENDR', 'RIDAGEYR', 'RIDRETH1', 'DMDBORN4', 'DMDYRSUS', 'DMDEDUC3', 'DMDEDUC2', 'WTINT2YR', 'WTMEC2YR', 'SDMVPSU', 'SDMVSTRA']].rename(columns={'DMDBORN4':'DMDBORN'})
iff_1_H = iff_1_H[['SEQN', 'DR1ILINE', 'DR1IFDCD', 'DR1_030Z', 'WTDRD1', 'DR1IGRMS', 'DR1IKCAL', 'DR1ICARB', 'DR1ISUGR', 'DR1IFIBE']].rename(columns={'DR1IFDCD':'DRXFDCD'})
food_desc_H.drop(columns='DRXFCSD', inplace=True)
recall_1_H = pd.merge(food_desc_H, iff_1_H, on = 'DRXFDCD')
recall_1_H_ = pd.merge(recall_1_H, demo_H, on = 'SEQN')
recall_1_H_['CYCLE'] = '13_14'
recall_1_H_['RIAGENDR'].replace({1 : 'Male', 2 : 'Female'}, inplace=True)
recall_1_H_['RIDRETH1'].replace({1 : "Mexican_American", 2 : "Other_Hispanic", 3 : "Non-Hispanic_White", 4 : 'Non-Hispanic_Black', 5 : "Other_Multi-Racial"}, inplace=True)
recall_1_H_['DMDBORN'].replace({1 : "US", 2 : "Elsewhere", 77 : 'Unknown', 99 : 'Unknown'}, inplace=True)
recall_1_H_['DR1_030Z'].replace({
  1 : 'Breakfast',
  2 : 'Lunch',
  3 : 'Dinner',
  4 : 'Dinner',
  5 : 'Brunch',
  6 : 'Snack',
  7 : 'Drink',
  8 : 'Infant feeding',
  9 : 'Extended consumption',
  10 : 'Breakfast',
  11 : 'Breakfast',
  12 : 'Lunch',
  13 : 'Snack',
  14 : 'Dinner',
  15 : 'Snack',
  16 : 'Snack',
  17 : 'Snack',
  18 : 'Snack',
  19 : 'Drink',
  91 : 'Unknown',
  99 : 'Unknown'
}, inplace=True)

demo_I = demo_I[['SEQN', 'RIAGENDR', 'RIDAGEYR', 'RIDRETH1', 'DMDBORN4', 'DMDYRSUS', 'DMDEDUC3', 'DMDEDUC2', 'WTINT2YR', 'WTMEC2YR', 'SDMVPSU', 'SDMVSTRA']].rename(columns={'DMDBORN4':'DMDBORN'})
iff_1_I = iff_1_I[['SEQN', 'DR1ILINE', 'DR1IFDCD', 'DR1_030Z', 'WTDRD1', 'DR1IGRMS', 'DR1IKCAL', 'DR1ICARB', 'DR1ISUGR', 'DR1IFIBE']].rename(columns={'DR1IFDCD':'DRXFDCD'})
food_desc_I.drop(columns='DRXFCSD', inplace=True)
recall_1_I = pd.merge(food_desc_I, iff_1_I, on = 'DRXFDCD')
recall_1_I_ = pd.merge(recall_1_I, demo_I, on = 'SEQN')
recall_1_I_['CYCLE'] = '15_16'
recall_1_I_['RIAGENDR'].replace({1 : 'Male', 2 : 'Female'}, inplace=True)
recall_1_I_['RIDRETH1'].replace({1 : "Mexican_American", 2 : "Other_Hispanic", 3 : "Non-Hispanic_White", 4 : 'Non-Hispanic_Black', 5 : "Other_Multi-Racial"}, inplace=True)
recall_1_I_['DMDBORN'].replace({1 : "US", 2 : "Elsewhere", 77 : 'Unknown', 99 : 'Unknown'}, inplace=True)
recall_1_I_['DR1_030Z'].replace({
  1 : 'Breakfast',
  2 : 'Lunch',
  3 : 'Dinner',
  4 : 'Dinner',
  5 : 'Brunch',
  6 : 'Snack',
  7 : 'Drink',
  8 : 'Infant feeding',
  9 : 'Extended consumption',
  10 : 'Breakfast',
  11 : 'Breakfast',
  12 : 'Lunch',
  13 : 'Snack',
  14 : 'Dinner',
  15 : 'Snack',
  16 : 'Snack',
  17 : 'Snack',
  18 : 'Snack',
  19 : 'Drink',
  91 : 'Unknown',
  99 : 'Unknown'
}, inplace=True)

demo_J = demo_J[['SEQN', 'RIAGENDR', 'RIDAGEYR', 'RIDRETH1', 'DMDBORN4', 'DMDYRSUS', 'DMDEDUC3', 'DMDEDUC2', 'WTINT2YR', 'WTMEC2YR', 'SDMVPSU', 'SDMVSTRA']].rename(columns={'DMDBORN4':'DMDBORN'})
iff_1_J = iff_1_J[['SEQN', 'DR1ILINE', 'DR1IFDCD', 'DR1_030Z', 'WTDRD1', 'DR1IGRMS', 'DR1IKCAL', 'DR1ICARB', 'DR1ISUGR', 'DR1IFIBE']].rename(columns={'DR1IFDCD':'DRXFDCD'})
food_desc_J.drop(columns='DRXFCSD', inplace=True)
recall_1_J = pd.merge(food_desc_J, iff_1_J, on = 'DRXFDCD')
recall_1_J_ = pd.merge(recall_1_J, demo_J, on = 'SEQN')
recall_1_J_['CYCLE'] = '17_18'
recall_1_J_['RIAGENDR'].replace({1 : 'Male', 2 : 'Female'}, inplace=True)
recall_1_J_['RIDRETH1'].replace({1 : "Mexican_American", 2 : "Other_Hispanic", 3 : "Non-Hispanic_White", 4 : 'Non-Hispanic_Black', 5 : "Other_Multi-Racial"}, inplace=True)
recall_1_J_['DMDBORN'].replace({1 : "US", 2 : "Elsewhere", 77 : 'Unknown', 99 : 'Unknown'}, inplace=True)
recall_1_J_['DR1_030Z'].replace({
  1 : 'Breakfast',
  2 : 'Lunch',
  3 : 'Dinner',
  4 : 'Dinner',
  5 : 'Brunch',
  6 : 'Snack',
  7 : 'Drink',
  8 : 'Infant feeding',
  9 : 'Extended consumption',
  10 : 'Breakfast',
  11 : 'Breakfast',
  12 : 'Lunch',
  13 : 'Snack',
  14 : 'Dinner',
  15 : 'Snack',
  16 : 'Snack',
  17 : 'Snack',
  18 : 'Snack',
  19 : 'Drink',
  91 : 'Unknown',
  99 : 'Unknown'
}, inplace=True)

WWEIA_ALL = pd.concat([recall_1_B_, recall_1_C_, recall_1_D_, recall_1_E_, recall_1_F_, recall_1_G_, recall_1_H_, recall_1_I_, recall_1_J_])

WWEIA_ALL_DIET = WWEIA_ALL.dropna(subset='DR1IGRMS')

# for FNDDS 09-18 we need to use a crosswalk to account for changes in food codes across cycles
# apply, combine for each cycle to correctly crosswalk codes
xwalk_FG = pd.read_csv('../data/04/fndds_crosswalk/fndds_0910_1112_crosswalk.csv')
xwalk_GH = pd.read_csv('../data/04/fndds_crosswalk/fndds_1112_1314_crosswalk.csv')
xwalk_HI = pd.read_csv('../data/04/fndds_crosswalk/fndds_1314_1516_crosswalk.csv')
xwalk_IJ = pd.read_csv('../data/04/fndds_crosswalk/fndds_1516_1718_crosswalk.csv')

xwalk_F_J = pd.concat([xwalk_FG, xwalk_GH, xwalk_HI, xwalk_IJ])

xwalk_F_J.drop_duplicates(subset='DRXFDCD', inplace=True)

# merge with cross walk to update foodcodes
wweia_xwalk = pd.merge(WWEIA_ALL_DIET, xwalk_F_J, on='DRXFDCD', how = 'left')

wweia_xwalk.drop(columns=['foodcode', 'food_description', 'DRXFCLD'],inplace=True)

wweia_xwalk.rename(columns={'DRXFDCD': 'foodcode'},inplace=True)

fndds_ingredients = pd.read_csv('../data/03/manually_curated/fndds_16_18_all_added_codes_for_discontinued.csv')
fndds_ingredients.rename(columns={'parent_foodcode': 'foodcode', 'parent_desc': 'food_description'}, inplace=True)

# string matched / manually updated discontined foodcodes
discon_update = pd.read_csv('../data/03/manually_curated/string_match_discontinued_complete.csv')
discon_update = discon_update[['DRXFDCD', 'parent_foodcode']]
discon_update.rename(columns={'DRXFDCD': 'foodcode'},inplace=True)

# merge with FNDDS ingredients
wweia_ingredients_allx = pd.merge(wweia_xwalk, fndds_ingredients, on='foodcode', how = 'left')
wweia_ingredients = pd.merge(wweia_xwalk, fndds_ingredients, on='foodcode', how = 'inner')

missing_fc = wweia_ingredients_allx[~wweia_ingredients_allx['foodcode'].isin(wweia_ingredients['foodcode'])]
missing_fc = missing_fc.drop(columns=['food_description', 'ingred_code', 'ingred_desc', 'ingred_wt'])

missing_update = pd.merge(missing_fc, discon_update, on = 'foodcode').drop(columns=['foodcode']).rename(columns={'parent_foodcode':'foodcode'})
missing_update_2 = pd.merge(missing_update, fndds_ingredients, on = 'foodcode')

wweia_complete = pd.concat([wweia_ingredients, missing_update_2])

# Load ingredient nutrient value data
ingred_nutrients = pd.read_csv('../data/01/fndds_all_ingredient_nutrient_values.csv')

ingred_nutrients.rename(columns={'Ingredient code': 'ingred_code'}, inplace=True)

wweia_complete_nutrients = pd.merge(wweia_complete, ingred_nutrients, on = 'ingred_code')

# calculate the amount of each ingredient consumed and the quantity of each nutrient consumed per ingredient
wweia_all_recalls = (wweia_complete_nutrients
  .groupby(['SEQN', 'foodcode', 'DR1ILINE'])
  .apply(lambda grp: grp.assign(Ingred_consumed_g = lambda x: x.DR1IGRMS * (x.ingred_wt / x.ingred_wt.sum())))
)

wweia_all_recalls.iloc[:,27:92] = wweia_all_recalls.iloc[:,27:92].multiply(wweia_all_recalls['Ingred_consumed_g'], axis=0) / 100

# Save dataset
#wweia_all_recalls.to_csv('../data/06/wweia_all_recalls.txt', sep = '\t', index=None)