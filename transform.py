import pandas as pd
import numpy as np
import re
from collections import defaultdict
import os
from datetime import datetime

class FlightDataTransformer:
    def __init__(self):
        # Define classification keywords for each category
        self.category_keywords = {
            'MEAT': [
                'meat', 'nyama', 'goat meat', 'beef', 'pork', 'chicken', 'mutton',
                'lamb', 'fresh chilled meat', 'frozen meat', 'pem', 'poultry'
            ],
            'FISH': [
                'fish', 'samaki', 'salmon', 'tuna', 'tilapia', 'fresh fish', 
                'frozen fish', 'seafood', 'sardines'
            ],
            'AVOCADO': [
                'avocado', 'parachichi', 'avocados', 'fresh avocado'
            ],
            'VEGETABLES': [
                'vegetables', 'mboga', 'tomatoes', 'onions', 'carrots', 'cabbage',
                'fresh vegetables', 'green beans', 'peas', 'potatoes', 'leafy vegetables'
            ],
            'FLOWERS': [
                'flowers', 'maua', 'roses', 'chrysanthemums', 'carnations', 
                'fresh flowers', 'cut flowers', 'floral'
            ],
            'COURIER': [
                'courier', 'express', 'ems', 'dhl', 'fedex', 'ups', 'tnt',
                'documents', 'express mail'
            ],
            'P.O.MAIL': [
                'mail', 'posta', 'post', 'postal', 'letters', 'registered mail',
                'parcel post', 'airmail'
            ],
            'VALUABLES': [
                'valuables', 'gold', 'jewelry', 'diamonds', 'precious', 'val',
                'valuable cargo', 'gems', 'bullion'
            ],
            'PER/COL': [
                'perishable', 'per', 'col', 'perishables', 'cold storage',
                'temperature controlled', 'chilled'
            ],
            'DG': [
                'dangerous goods', 'dg', 'hazardous', 'dangerous', 'toxic',
                'flammable', 'corrosive', 'radioactive'
            ],
            'CRABS/LOBSTER': [
                'crab', 'lobster', 'crabs', 'lobsters', 'shellfish', 'kaa',
                'fresh crabs', 'live crabs', 'live lobster'
            ]
        }
        
        # Track unclassified words
        self.unclassified_words = set()
        
    def clean_text(self, text):
        """Clean and normalize text for classification"""
        if pd.isna(text):
            return ""
        return str(text).lower().strip()
    
    def classify_awb(self, awb, nature_goods, shcs):
        """Classify AWB into categories based on keywords and AWB number"""
        # Check for P.O.MAIL first (AWB prefix)
        if pd.notna(awb) and str(awb).upper().startswith('MAL'):
            return 'P.O.MAIL'
        
        # Combine all text fields for classification
        combined_text = f"{self.clean_text(nature_goods)} {self.clean_text(shcs)}"
        
        # If no text content, check AWB number patterns
        if not combined_text.strip():
            awb_str = str(awb) if pd.notna(awb) else ""
            # Add more AWB prefix patterns as needed
            if any(pattern in awb_str.upper() for pattern in ['VAL', 'DG', 'PER']):
                if 'VAL' in awb_str.upper():
                    return 'VALUABLES'
                elif 'DG' in awb_str.upper():
                    return 'DG'
                elif 'PER' in awb_str.upper():
                    return 'PER/COL'
            return 'G. CARGO'
        
        # Check each category for keyword matches
        for category, keywords in self.category_keywords.items():
            for keyword in keywords:
                if keyword in combined_text:
                    return category
        
        # Log unclassified words for learning
        words = re.findall(r'\b\w+\b', combined_text)
        for word in words:
            if len(word) > 2:  # Only log meaningful words
                self.unclassified_words.add(word)
        
        return 'G. CARGO'  # Default category
    
    def transform_data(self, input_file, output_file):
        """Main transformation function"""
        try:
            # Read the input Excel file
            print(f"Reading input file: {input_file}")
            df = pd.read_excel(input_file)
            
            # Standardize column names (handle variations in naming)
            column_mapping = {
                'Flight date': 'Flight_date',
                'Carrier': 'Carrier',
                'Flight No.': 'Flight_No',
                'origin': 'Origin',
                'dest': 'Dest',
                'AWB': 'AWB',
                'weight': 'Weight',
                'Rcv weight': 'Rcv_weight',
                'Nature Goods': 'Nature_Goods',
                'SHCs': 'SHCs'
            }
            
            # Rename columns if they exist
            for old_name, new_name in column_mapping.items():
                if old_name in df.columns:
                    df = df.rename(columns={old_name: new_name})
            
            print(f"Input data shape: {df.shape}")
            print(f"Columns: {list(df.columns)}")
            
            # Create SECTOR column
            df['SECTOR'] = df['Origin'].astype(str) + '-' + df['Dest'].astype(str)
            
            # Convert Flight_date to date format
            df['DATE'] = pd.to_datetime(df['Flight_date']).dt.date
            
            # Use received weight if available, otherwise use weight
            df['Final_Weight'] = df.get('Rcv_weight', df['Weight'])
            df['Final_Weight'] = pd.to_numeric(df['Final_Weight'], errors='coerce').fillna(0)
            
            # Classify each AWB
            print("Classifying AWBs...")
            df['Category'] = df.apply(
                lambda row: self.classify_awb(
                    row.get('AWB', ''),
                    row.get('Nature_Goods', ''),
                    row.get('SHCs', '')
                ),
                axis=1
            )
            
            # Group by DATE, Carrier, Flight_No, SECTOR
            print("Grouping and aggregating data...")
            grouped = df.groupby(['DATE', 'Carrier', 'Flight_No', 'SECTOR'])
            
            # Initialize result list
            results = []
            
            for (date, airline, flight_no, sector), group in grouped:
                row_data = {
                    'DATE': date,
                    'AIRLINE': airline,
                    'FLIGHT No': flight_no,
                    'SECTOR': sector,
                    'F/CATEGORY': '',  # Can be filled based on business logic
                }
                
                # Initialize all category columns
                categories = [
                    'G. CARGO', 'VEGETABLES', 'AVOCADO', 'FISH', 'MEAT', 
                    'VALUABLES', 'FLOWERS', 'PER/COL', 'DG', 'CRABS/LOBSTER', 
                    'P.O.MAIL', 'COURIER'
                ]
                
                # Initialize weight columns
                for cat in categories:
                    row_data[cat] = 0
                
                # Initialize AWB count columns
                awb_categories = [
                    'G. AWBs', 'VAL AWBs', 'VEGETABLES AWBs', 'AVOCADO AWBs',
                    'FISH AWBs', 'MEAT AWBs', 'COURIER AWBs', 'CRAB/LOBSTER AWBs',
                    'FLOWERS AWBs', 'PER/COL AWBs', 'DG AWBs', 'MAIL AWBs'
                ]
                
                for awb_cat in awb_categories:
                    row_data[awb_cat] = 0
                
                # Aggregate weights and counts by category
                category_stats = group.groupby('Category').agg({
                    'Final_Weight': 'sum',
                    'AWB': 'count'
                }).reset_index()
                
                total_weight = 0
                total_awbs = 0
                
                for _, cat_row in category_stats.iterrows():
                    category = cat_row['Category']
                    weight = cat_row['Final_Weight']
                    count = cat_row['AWB']
                    
                    # Map categories to columns
                    if category in row_data:
                        row_data[category] = weight
                    
                    # Map to AWB count columns
                    awb_col_mapping = {
                        'G. CARGO': 'G. AWBs',
                        'VALUABLES': 'VAL AWBs',
                        'VEGETABLES': 'VEGETABLES AWBs',
                        'AVOCADO': 'AVOCADO AWBs',
                        'FISH': 'FISH AWBs',
                        'MEAT': 'MEAT AWBs',
                        'COURIER': 'COURIER AWBs',
                        'CRABS/LOBSTER': 'CRAB/LOBSTER AWBs',
                        'FLOWERS': 'FLOWERS AWBs',
                        'PER/COL': 'PER/COL AWBs',
                        'DG': 'DG AWBs',
                        'P.O.MAIL': 'MAIL AWBs'
                    }
                    
                    if category in awb_col_mapping:
                        row_data[awb_col_mapping[category]] = count
                    
                    total_weight += weight
                    total_awbs += count
                
                row_data['TOTAL AWBs'] = total_awbs
                row_data['TOTAL WEIGHT'] = total_weight
                
                results.append(row_data)
            
            # Create output DataFrame
            result_df = pd.DataFrame(results)
            
            # Ensure all required columns exist in the correct order
            required_columns = [
                'DATE', 'AIRLINE', 'FLIGHT No', 'SECTOR', 'F/CATEGORY',
                'G. CARGO', 'VEGETABLES', 'AVOCADO', 'FISH', 'MEAT',
                'VALUABLES', 'FLOWERS', 'PER/COL', 'DG', 'CRABS/LOBSTER',
                'P.O.MAIL', 'COURIER', 'G. AWBs', 'VAL AWBs', 'VEGETABLES AWBs',
                'AVOCADO AWBs', 'FISH AWBs', 'MEAT AWBs', 'COURIER AWBs',
                'CRAB/LOBSTER AWBs', 'FLOWERS AWBs', 'PER/COL AWBs',
                'DG AWBs', 'MAIL AWBs', 'TOTAL AWBs', 'TOTAL WEIGHT'
            ]
            
            # Reorder columns and fill missing ones
            for col in required_columns:
                if col not in result_df.columns:
                    result_df[col] = 0
            
            result_df = result_df[required_columns]
            
            # Sort by date and airline
            result_df = result_df.sort_values(['DATE', 'AIRLINE', 'FLIGHT No'])
            
            # Write to Excel
            print(f"Writing output file: {output_file}")
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                result_df.to_excel(writer, sheet_name='Formatted Report', index=False)
            
            # Log unclassified words
            self.log_unclassified_words()
            
            print(f"Transformation completed successfully!")
            print(f"Output records: {len(result_df)}")
            print(f"Unclassified words logged: {len(self.unclassified_words)}")
            
            return result_df
            
        except Exception as e:
            print(f"Error during transformation: {str(e)}")
            raise
    
    def log_unclassified_words(self):
        """Log unclassified words to a text file for review"""
        if not self.unclassified_words:
            return
        
        log_file = 'unclassified_words.txt'
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Read existing words to avoid duplicates
        existing_words = set()
        if os.path.exists(log_file):
            with open(log_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if not line.startswith('#') and line.strip():
                        existing_words.add(line.strip().lower())
        
        # Filter out words that already exist
        new_words = self.unclassified_words - existing_words
        
        if new_words:
            with open(log_file, 'a', encoding='utf-8') as f:
                f.write(f"\n# Unclassified words logged on {timestamp}\n")
                for word in sorted(new_words):
                    f.write(f"{word}\n")
            
            print(f"Logged {len(new_words)} new unclassified words to {log_file}")
        else:
            print("No new unclassified words to log")
    
    def update_keywords(self, keyword_updates):
        """Update keyword mappings based on user review"""
        for category, new_keywords in keyword_updates.items():
            if category in self.category_keywords:
                self.category_keywords[category].extend(new_keywords)
                print(f"Added {len(new_keywords)} keywords to {category}")
            else:
                print(f"Warning: Category '{category}' not found")

def main():
    """Main execution function"""
    # Initialize transformer
    transformer = FlightDataTransformer()
    
    # Example usage
    input_file = "Book1.xlsx"  # Replace with your input file path
    output_file = "Book2.xlsx"  # Replace with your desired output file path
    
    try:
        # Transform the data
        result_df = transformer.transform_data(input_file, output_file)
        
        # Display summary
        print("\n" + "="*50)
        print("TRANSFORMATION SUMMARY")
        print("="*50)
        print(f"Total flights processed: {len(result_df)}")
        print(f"Date range: {result_df['DATE'].min()} to {result_df['DATE'].max()}")
        print(f"Airlines: {', '.join(result_df['AIRLINE'].unique())}")
        print(f"Total AWBs: {result_df['TOTAL AWBs'].sum()}")
        print(f"Total Weight: {result_df['TOTAL WEIGHT'].sum():.2f}")
        
        # Show category breakdown
        print("\nCategory breakdown:")
        categories = ['G. CARGO', 'VEGETABLES', 'AVOCADO', 'FISH', 'MEAT',
                     'VALUABLES', 'FLOWERS', 'PER/COL', 'DG', 'CRABS/LOBSTER',
                     'P.O.MAIL', 'COURIER']
        
        for category in categories:
            total_weight = result_df[category].sum()
            if total_weight > 0:
                print(f"  {category}: {total_weight:.2f} kg")
        
    except FileNotFoundError:
        print(f"Error: Input file '{input_file}' not found.")
        print("Please ensure the file exists and the path is correct.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()

# Example of how to update keywords after reviewing unclassified_words.txt
"""
# After reviewing unclassified_words.txt, you can update keywords like this:
transformer = FlightDataTransformer()

keyword_updates = {
    'VEGETABLES': ['mboga_mpya', 'fresh_produce'],
    'FISH': ['samaki_fresh', 'tuna_fresh'],
    'MEAT': ['kuku', 'chicken_fresh']
}

transformer.update_keywords(keyword_updates)
# Then run the transformation again
"""