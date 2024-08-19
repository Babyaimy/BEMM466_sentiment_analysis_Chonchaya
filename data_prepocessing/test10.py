import pandas as pd
from langdetect import detect, LangDetectException
from deep_translator import GoogleTranslator
import time

# Load the corrected final updated reviews CSV file
corrected_final_updated_reviews_file_path = 'corrected_final_updated_reviews.csv'  # Update with your actual file path if needed
corrected_final_updated_reviews_df = pd.read_csv(corrected_final_updated_reviews_file_path)

# Function to detect the language of comments
def detect_language(comment):
    try:
        return detect(comment)
    except LangDetectException:
        return 'unknown'

# Detect languages in the comments
corrected_final_updated_reviews_df['language'] = corrected_final_updated_reviews_df['comments'].apply(detect_language)

# Identify and count remaining non-English comments
non_english_comments_df = corrected_final_updated_reviews_df[corrected_final_updated_reviews_df['language'] != 'en']
non_english_comments_count = non_english_comments_df.shape[0]
print(f"Number of non-English comments still present: {non_english_comments_count}")

# Display the count of each language
language_counts = non_english_comments_df['language'].value_counts()
print("Languages still present and their counts:")
print(language_counts)

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

# Translate the remaining non-English comments
total_comments = non_english_comments_df.shape[0]
processed_comments = 0

start_time = time.time()
last_logged_time = start_time

for index, row in non_english_comments_df.iterrows():
    translated_comment = translate_comment(row['comments'])
    corrected_final_updated_reviews_df.loc[corrected_final_updated_reviews_df['id'] == row['id'], 'comments'] = translated_comment
    processed_comments += 1
    
    current_time = time.time()
    elapsed_time = current_time - last_logged_time
    
    if elapsed_time >= 60:  # Log progress every minute
        print(f"Processed {processed_comments} out of {total_comments} comments.")
        last_logged_time = current_time

# Save the updated dataset to a new CSV file
final_corrected_file_path = 'final_corrected_updated_reviews.csv'
corrected_final_updated_reviews_df.to_csv(final_corrected_file_path, index=False)

print(f"Updated dataset with re-translated comments has been saved to {final_corrected_file_path}")

# Re-check for any remaining non-English comments
corrected_final_updated_reviews_df['language'] = corrected_final_updated_reviews_df['comments'].apply(detect_language)
final_non_english_comments_df = corrected_final_updated_reviews_df[corrected_final_updated_reviews_df['language'] != 'en']
final_non_english_comments_count = final_non_english_comments_df.shape[0]
print(f"Number of non-English comments after re-translation: {final_non_english_comments_count}")

# Save any remaining non-English comments for inspection
final_non_english_comments_df.to_csv('remaining_non_english_comments_after_retranslation.csv', index=False)
