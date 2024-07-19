#!/bin/bash

echo "Initializing step 1: consolidating ingredient codes"; date
python3 ../src/01_consolidate_codes.py
echo "Initializing step 2: calculating ingredient weights"; date
python3 ../src/02_calculate_ingredient_weights.py
echo "Initializing step 3: text matching discontinued codes"; date
python3 ../src/03_text_match_discontinued_codes.py
echo "Initializing step 3b: Further ingredientizing with FCID"; date
python3 ../src/03b_fcid_ingredients.py
echo "Initializing step 5: "Combining WWEIA foodcode data"; date
python3 ../src/05_wweia_foodcodes.py
echo "Success! Pipeline finished on:"; date
