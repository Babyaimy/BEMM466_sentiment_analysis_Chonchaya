import pandas as pd
from langdetect import detect, LangDetectException
from deep_translator import GoogleTranslator
import time

# Load the two CSV files
updated_translated_reviews_file_path = 'updated_translated_reviews.csv'  # Update with your actual file path if needed
final_updated_reviews_file_path = 'final_updated_reviews.csv'  # Update with your actual file path if needed

updated_translated_reviews_df = pd.read_csv(updated_translated_reviews_file_path)
final_updated_reviews_df = pd.read_csv(final_updated_reviews_file_path)

# Merge the dataframes on common columns (id and reviewer_id are assumed to be common identifiers)
merged_df = updated_translated_reviews_df.merge(final_updated_reviews_df, on=['id', 'reviewer_id'], suffixes=('_updated', '_final'))

# Identify rows where the comments differ
different_comments_df = merged_df[merged_df['comments_updated'] != merged_df['comments_final']]

# Display the rows with different comments
print(f"Number of differing comments: {different_comments_df.shape[0]}")
print(different_comments_df[['id', 'reviewer_id', 'comments_updated', 'comments_final']])

# Function to detect the language of comments
def detect_language(comment):
    try:
        return detect(comment)
    except LangDetectException:
        return 'unknown'

# Function to map detected language to supported language
def map_supported_language(lang):
    language_map = {
        'zh-cn': 'zh-CN',
        'zh-tw': 'zh-TW',
        'he': 'iw',  # Map Hebrew to the supported language code
        # Add other mappings if needed
    }
    return language_map.get(lang, lang)

# Function to translate non-English comments
def translate_comment(comment):
    try:
        lang = detect(comment)
        mapped_lang = map_supported_language(lang)
        if mapped_lang != 'en':
            translated = GoogleTranslator(source=mapped_lang, target='en').translate(comment)
            return translated
        else:
            return comment
    except LangDetectException:
        return comment

# Translate the differing comments in the updated file
total_comments = different_comments_df.shape[0]
processed_comments = 0

start_time = time.time()
last_logged_time = start_time

for index, row in different_comments_df.iterrows():
    translated_comment = translate_comment(row['comments_updated'])
    final_updated_reviews_df.loc[final_updated_reviews_df['id'] == row['id'], 'comments'] = translated_comment
    processed_comments += 1
    
    current_time = time.time()
    elapsed_time = current_time - last_logged_time
    
    if elapsed_time >= 60:  # Log progress every minute
        print(f"Processed {processed_comments} out of {total_comments} comments.")
        last_logged_time = current_time

# Save the corrected dataset to a new CSV file
corrected_file_path = 'corrected_final_updated_reviews.csv'
final_updated_reviews_df.to_csv(corrected_file_path, index=False)

print(f"Corrected dataset has been saved to {corrected_file_path}")
