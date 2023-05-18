# WWEIA Ingredients

The field of nutritional epidemiology attemps to link dietary intake data with health outcomes or other biomarkers associated with health or disease. However, dietary data from current publicly available databases is often aggregated at the level of food groups or mixed dishes, thereby limiting resolution and analysis of individual ingredients that may contribute to an outcome of interest. 

Recognizing these challenges, this project modifies the National Health And Nutrition Examination Survey (NHANES) What We Eat In America (WWEIA) data from 2001-2018 to generate an 'ingredientized' dataset by updating food codes and descriptions with the Food and Nutrition Database for Dietary Studies (FNDDS) database versions 2015-2016 and 2017-2018.

## Workflow

### 01: Consolidate ingredient codes
 
__Purpose__  
- Part 1: reduce the number of ingredients used by averaging commonly consumed food items. This creates a single averaged ingredient code from the multiple codes and weights found in these commonly consumed ingredients and reduces over-complexity and redundany.
- Part 2: Remaps ingredient codes represented as 8-digit foodcodes for obtaining proper ingredient weights per ingredient code.
- Part 3: Updates the ingredient codes and correponding nutrient values with ingredient descriptions from part 1. 

__Required Input Files__

  - **fndds_16** - Downloaded from: https://www.ars.usda.gov/ARSUserFiles/80400530/apps/2015-2016%20FNDDS%20At%20A%20Glance%20-%20FNDDS%20Ingredients.xlsx
  - **fndds_18** - Downloaded from: https://www.ars.usda.gov/ARSUserFiles/80400530/apps/2017-2018%20FNDDS%20At%20A%20Glance%20-%20FNDDS%20Ingredients.xlsx
  - **nutrient_values_16** - Downloaded from: https://www.ars.usda.gov/ARSUserFiles/80400530/apps/2015-2016%20FNDDS%20At%20A%20Glance%20-%20Ingredient%20Nutrient%20Values.xlsx
  - **nutrient_values_18** - Downloaded from: https://www.ars.usda.gov/ARSUserFiles/80400530/apps/2017-2018%20FNDDS%20At%20A%20Glance%20-%20Ingredient%20Nutrient%20Values.xlsx

__Output__
- **fndds_16_consolidated_ingredient_codes.csv** (Part 1)
- **fndds_18_consolidated_ingredient_codes.csv** (Part 1)
- **fndds_16_all_ingredients.csv** (Part 2)
- **fndds_18_all_ingredients.csv** (Part 2)
- **fndds_all_ingredient_nutrient_values.csv** (Part 3)

### 02: Calculate FNDDS Ingredient Weights

__Purpose__  
This script recursively calculates the ingredient weight amounts within 8-digit foodcodes. This is needed to correctly represent the relative proportions of ingredient codes using 8-digit foodcodes

__Required Input Files__

  - **fndds_16_all_ingredients.csv** - Output from 01
  - **fndds_18_all_ingredients.csv** - Output from 01
  - **fndds_16_consolidated_ingredient_codes.csv** - Output from 01
  - **fndds_18_consolidated_ingredient_codes.csv** - Output from 01

__Workflow__
    1) Recursion: preprocessing to format dataframe for and perform calucations with get_ingredient_proportions.
    2) Combine result with the remianing ingredient codes that were properly represented as ingredient codes and not foodcodes.
    3) Combine the results for FNDDS1516 and FNDDS1718 into a single dataframe.
    
__Output__
  - **fndds_16_18_all.csv**

### 03: Matching Discontinued FNDDS (WWEIA 01-13) Food Codes and Descriptions

__Required Input Files__

  - **wweia_discontinued_foodcodes.csv** - List of discontinued foodcodes from FNDDS versions corresponding to WWEIA 01-13 
  - **fndds_16_18_all.csv** - Output from 02

__Information__  
This script prepares food descriptions in discontined foodcodes from WWEIA cycles (01 - 13) corresponding to early FNDDS versions for text similarity matching. This script achieves the following:
    
- Text cleaning: removal of punctuation and stopwords, lemmatization, etc.
- Finds matches based on main food descriptions (rather than foodcodes which were used in the crosswalk before) and exports these matches
        
__Output__
  - **string_match.csv** - This file will undergo manual curation to match discontinued foodcodes to the most appropriate foodcode in fndds_16_18_all.csv

__Note:__  The output of this manual matching is: string_match_discontinued_complete.csv and string_match_discontinued_complete_with_annotation.csv for documentation on added foodcodes.
Similarly, fndds_16_18_all.csv was updated with new codes for recipes that did not exists and were needed to match discontinued foodcodes. This updated file is: fndds_16_18_all_added_codes_for_discontinued.csv
Both of these manually curated outputs [string_match_discontinued_complete.csv and fndds_16_18_all_added_codes_for_discontinued.csv] are used in the next script: 06_wweia_ingredients.py

### 04: WWEIA Ingredient

__Required Input Files__

Demographic, Dietary day 1, and Food description data for cycles 01 through 18 downloaded from https://wwwn.cdc.gov/NCHS/nhanes

  - **fndds_16_18_all_added_codes_for_discontinued.csv** - Manually curated version of fndds_16_18_all.csv with added food codes and descriptions for matching discontinued foods.
  - **string_match_discontinued_complete.csv** - Manually curated version of string_match.csv with added food codes and descriptions for matching discontinued foods.
  - **fndds_crosswalks** - Each of the crosswalk files in the fndds_crosswalk directory.

__Information__  
This script prepares combines data for each of the WWEIA cycles (01 - 18), updates the food codes to those with ingredient codes and appends nutrient values corresponding to the ingredients.

__Output__
  - **wweia_all_recalls.txt** - The complete ingredientized dataset for WWEIA cycles 01-18. Approximately 2.3M rows of diet intake with 65 nutrient estimates for each ingredient across 80275 individuals.
