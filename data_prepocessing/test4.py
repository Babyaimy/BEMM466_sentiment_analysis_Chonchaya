import pandas as pd
from langdetect import detect, LangDetectException
from deep_translator import GoogleTranslator
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load the translated CSV file
translated_file_path = 'translated_reviews.csv'  # Ensure this path is correct
translated_reviews_df = pd.read_csv(translated_file_path)

# Supported languages map for GoogleTranslator
supported_languages = {
    'af': 'af', 'sq': 'sq', 'am': 'am', 'ar': 'ar', 'hy': 'hy', 'as': 'as', 'ay': 'ay', 'az': 'az', 'bm': 'bm',
    'eu': 'eu', 'be': 'be', 'bn': 'bn', 'bho': 'bho', 'bs': 'bs', 'bg': 'bg', 'ca': 'ca', 'ceb': 'ceb', 'ny': 'ny',
    'zh-CN': 'zh-CN', 'zh-TW': 'zh-TW', 'co': 'co', 'hr': 'hr', 'cs': 'cs', 'da': 'da', 'dv': 'dv', 'doi': 'doi',
    'nl': 'nl', 'en': 'en', 'eo': 'eo', 'et': 'et', 'ee': 'ee', 'tl': 'tl', 'fi': 'fi', 'fr': 'fr', 'fy': 'fy',
    'gl': 'gl', 'ka': 'ka', 'de': 'de', 'el': 'el', 'gn': 'gn', 'gu': 'gu', 'ht': 'ht', 'ha': 'ha', 'haw': 'haw',
    'iw': 'iw', 'hi': 'hi', 'hmn': 'hmn', 'hu': 'hu', 'is': 'is', 'ig': 'ig', 'ilo': 'ilo', 'id': 'id', 'ga': 'ga',
    'it': 'it', 'ja': 'ja', 'jw': 'jw', 'kn': 'kn', 'kk': 'kk', 'km': 'km', 'rw': 'rw', 'gom': 'gom', 'ko': 'ko',
    'kri': 'kri', 'ku': 'ku', 'ckb': 'ckb', 'ky': 'ky', 'lo': 'lo', 'la': 'la', 'lv': 'lv', 'ln': 'ln', 'lt': 'lt',
    'lg': 'lg', 'lb': 'lb', 'mk': 'mk', 'mai': 'mai', 'mg': 'mg', 'ms': 'ms', 'ml': 'ml', 'mt': 'mt', 'mi': 'mi',
    'mr': 'mr', 'mni-Mtei': 'mni-Mtei', 'lus': 'lus', 'mn': 'mn', 'my': 'my', 'ne': 'ne', 'no': 'no', 'or': 'or',
    'om': 'om', 'ps': 'ps', 'fa': 'fa', 'pl': 'pl', 'pt': 'pt', 'pa': 'pa', 'qu': 'qu', 'ro': 'ro', 'ru': 'ru',
    'sm': 'sm', 'sa': 'sa', 'gd': 'gd', 'nso': 'nso', 'sr': 'sr', 'st': 'st', 'sn': 'sn', 'sd': 'sd', 'si': 'si',
    'sk': 'sk', 'sl': 'sl', 'so': 'so', 'es': 'es', 'su': 'su', 'sw': 'sw', 'sv': 'sv', 'tg': 'tg', 'ta': 'ta',
    'tt': 'tt', 'te': 'te', 'th': 'th', 'ti': 'ti', 'ts': 'ts', 'tr': 'tr', 'tk': 'tk', 'ak': 'ak', 'uk': 'uk',
    'ur': 'ur', 'ug': 'ug', 'uz': 'uz', 'vi': 'vi', 'cy': 'cy', 'xh': 'xh', 'yi': 'yi', 'yo': 'yo', 'zu': 'zu'
}

# Function to detect language of comments
def detect_language(comment):
    try:
        return detect(comment)
    except LangDetectException:
        return 'unknown'

# Function to map detected language to supported language
def map_supported_language(lang):
    return supported_languages.get(lang, 'en')

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

# Detect and translate non-English comments
translated_reviews_df['language'] = translated_reviews_df['comments'].apply(detect_language)
non_english_comments_df = translated_reviews_df[translated_reviews_df['language'] != 'en']

# Use ThreadPoolExecutor to parallelize the translation process
def translate_and_update(comment, index):
    translated_comment = translate_comment(comment)
    translated_reviews_df.at[index, 'comments'] = translated_comment

total_comments = len(non_english_comments_df)
processed_comments = 0

with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(translate_and_update, comment, index) for index, comment in non_english_comments_df['comments'].items()]
    
    start_time = time.time()
    last_logged_time = start_time

    for future in as_completed(futures):
        future.result()  # Ensure all futures are completed
        processed_comments += 1
        
        current_time = time.time()
        elapsed_time = current_time - last_logged_time
        
        if elapsed_time >= 60 or processed_comments == total_comments:  # Log progress every minute or when done
            print(f"Processed {processed_comments} out of {total_comments} comments.")
            logging.info(f"Processed {processed_comments} out of {total_comments} comments.")
            last_logged_time = current_time

# Drop the language column and save the updated DataFrame to a new CSV file
translated_reviews_df.drop(columns=['language'], inplace=True)
updated_file_path = 'updated_translated_reviews.csv'
translated_reviews_df.to_csv(updated_file_path, index=False)

print(f"Updated translated reviews have been saved to {updated_file_path}")
