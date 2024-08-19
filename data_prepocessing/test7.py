import pandas as pd
from langdetect import detect, LangDetectException
from deep_translator import GoogleTranslator
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load the original dataset with non-English comments
original_file_path = 'updated_translated_reviews.csv'  # Update with your actual file path if needed
original_df = pd.read_csv(original_file_path)

# Load the translated non-English comments CSV file
translated_non_english_comments_file_path = 'translated_non_english_comments.csv'  # Update with your actual file path if needed
translated_non_english_comments_df = pd.read_csv(translated_non_english_comments_file_path)

# Function to detect the language of comments
def detect_language(comment):
    try:
        return detect(comment)
    except LangDetectException:
        return 'unknown'

# Check for non-English comments in the translated file
translated_non_english_comments_df['language'] = translated_non_english_comments_df['comments'].apply(detect_language)

# Filter out non-English comments
non_english_comments_df = translated_non_english_comments_df[translated_non_english_comments_df['language'] != 'en']

# Count non-English comments
non_english_comments_count = non_english_comments_df.shape[0]
print(f"Number of non-English comments still present: {non_english_comments_count}")

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

# Re-translate non-English comments
def translate_and_update(comment, index):
    translated_comment = translate_comment(comment)
    translated_non_english_comments_df.at[index, 'comments'] = translated_comment

total_comments = len(non_english_comments_df)
processed_comments = 0

with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(translate_and_update, comment, index) for index, comment in non_english_comments_df['comments'].items()]
    
    for future in as_completed(futures):
        future.result()  # Ensure all futures are completed
        processed_comments += 1
        if processed_comments % 100 == 0 or processed_comments == total_comments:
            print(f"Processed {processed_comments} out of {total_comments} comments.")

# Update the original dataset with the re-translated comments
for index, row in translated_non_english_comments_df.iterrows():
    original_df.at[index, 'comments'] = row['comments']

# Save the updated dataset to a new CSV file
updated_file_path = 'final_updated_reviews.csv'
original_df.to_csv(updated_file_path, index=False)

print(f"Final updated dataset has been saved to {updated_file_path}")

# Re-check for any remaining non-English comments
original_df['language'] = original_df['comments'].apply(detect_language)
final_non_english_comments_df = original_df[original_df['language'] != 'en']
final_non_english_comments_count = final_non_english_comments_df.shape[0]
print(f"Number of non-English comments after re-translation: {final_non_english_comments_count}")

# Save any remaining non-English comments for inspection
final_non_english_comments_df.to_csv('remaining_non_english_comments.csv', index=False)
