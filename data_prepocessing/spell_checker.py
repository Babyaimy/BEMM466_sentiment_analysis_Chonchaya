import pandas as pd
from spellchecker import SpellChecker
import time

# Load the cleaned reviews CSV file
cleaned_file_path = 'cleaned_final_corrected_reviews.csv'  # Update with your actual file path if needed
cleaned_reviews_df = pd.read_csv(cleaned_file_path)

# Initialize the spell checker outside the loop for efficiency
spell = SpellChecker()

# Function to correct spelling errors
def correct_spelling(text):
    if not isinstance(text, str) or text.strip() == '':
        return 'no comment'
    
    # Correct spelling errors
    corrected_words = []
    for word in text.split():
        if word:
            corrected_word = spell.correction(word)
            if corrected_word:
                corrected_words.append(corrected_word)
            else:
                corrected_words.append(word)
        else:
            corrected_words.append('')
    
    return " ".join(corrected_words)

# Measure time for the entire dataset
start_time = time.time()

# Track progress
total_rows = cleaned_reviews_df.shape[0]
progress_interval = 100  # Print progress every 100 rows

# Apply the spelling correction function to the 'comments' column with progress tracking
for i, row in cleaned_reviews_df.iterrows():
    # Safeguard against None values
    comment = row['comments'] if pd.notnull(row['comments']) else 'no comment'
    cleaned_reviews_df.at[i, 'comments'] = correct_spelling(comment)
    
    if (i + 1) % progress_interval == 0 or (i + 1) == total_rows:
        print(f"Processed {i + 1} / {total_rows} rows")

end_time = time.time()
total_elapsed_time = end_time - start_time

# Save the updated dataframe to a new CSV file
spelling_corrected_file_path = 'spelling_corrected_reviews.csv'
cleaned_reviews_df.to_csv(spelling_corrected_file_path, index=False)

print(f"Spelling corrected dataset has been saved to {spelling_corrected_file_path}")
print(f"Total processing time: {total_elapsed_time / 60:.2f} minutes")
