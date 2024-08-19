import pandas as pd
import string

# Load the final corrected updated reviews CSV file
final_corrected_updated_reviews_file_path = 'final_corrected_updated_reviews.csv'  # Update with your actual file path if needed
final_corrected_updated_reviews_df = pd.read_csv(final_corrected_updated_reviews_file_path)

# Function to convert text to lowercase and remove punctuation
def clean_text(text):
    if not isinstance(text, str) or text.strip() == '':
        return 'no comment'
    
    # Convert to lowercase
    text = text.lower()
    
    # Remove punctuation
    text = text.translate(str.maketrans('', '', string.punctuation))
    
    return text

# Apply the cleaning function to the 'comments' column
final_corrected_updated_reviews_df['comments'] = final_corrected_updated_reviews_df['comments'].apply(clean_text)

# Save the updated dataframe to a new CSV file
cleaned_file_path = 'cleaned_final_corrected_reviews.csv'
final_corrected_updated_reviews_df.to_csv(cleaned_file_path, index=False)

print(f"Cleaned dataset has been saved to {cleaned_file_path}")
