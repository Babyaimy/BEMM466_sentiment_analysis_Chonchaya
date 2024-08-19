## Data Pre-processing step for 'reviews.csv'

Step 1 : translate non-English comment (test4.py)
Step 2 : The result from test4.py still faced problem with chinese language, so this file need to re-translate again (test7.py)
Step 3 :  The result from test7.py still faced problem, so this will be re-translated with another approach.
Step 4 : The result from test9.py successfully translate to English but have some problem with indexing. ‘test10.py’ is introduced to solve the index issue.
Step 5 : Convert all reviews to lower case and remove punctuation (cleaned_reviews.py)
Step 6 : Perform spelling check via Spellchecker python library and save to the file 'spelling_corrected_reviews.csv' (spell_checker.py). 
