import pandas as pd
import textwrap

#Class to read the csv files and return a dataframe

class CSVReader:
    def __init__(self, base_path):
        self.base_path = base_path

    def read_file(self, year, category):
        file_name = f"{str(year)}_{category}.csv"
        file_path = f"{self.base_path}/{file_name}"
        
        #Read what type of encondig file has to use it as an input for pandas reading
        
        enc = open(file_path).encoding
        
        #Read the csv files with pandas

        try:
            df = pd.read_csv(file_path, encoding = enc, delimiter = ';')
            return df
        except FileNotFoundError:
            print(f"File '{file_name}' not found.")
            return None
        
# Class to perform some data transformations

class DataTransformation:
    def __init__(self, df):
        self.df = df
    
    def rename_columns(self, column_names, column_renames):
        if column_names and column_renames:
            column_mapping = dict(zip(column_names, column_renames))
            self.df.rename(columns = column_mapping, inplace = True)

            
    def delete_columns(self, columns_to_delete):
        if columns_to_delete:
            self.df.drop(columns = columns_to_delete, inplace = True)

            
    def replace_characters(self, column_name, old_char, new_char):
        if column_name in self.df.columns:
            #self.df[column_name] = self.df[column_name].str.replace(old_char, new_char)
            self.df.loc[:,column_name] = self.df[column_name].str.replace(old_char, new_char)
            
            
    
    def split_column(self, column_name, delimiter, new_column_names):
        if column_name in self.df.columns and new_column_names:   
            self.df[new_column_names] = self.df[column_name].astype('string').str.split(delimiter, expand=True)
            self.df.drop(columns=[column_name], inplace=True)


    def convert_to_numeric(self, columns_to_convert):
        if columns_to_convert:
            self.df[columns_to_convert] = self.df[columns_to_convert].apply(pd.to_numeric, errors='coerce')
    

# Class to print a list in a formatted way to make it easier to visualize it

class ListFormatter:
    def __init__(self, my_list, line_width=250):
        self.my_list = my_list
        self.line_width = line_width

    def format_as_text(self):
        # Convert the list elements to strings
        list_str = [str(item) for item in self.my_list]

        # Join the list elements with spaces to create a single string
        list_text = " || ".join(list_str)

        # Use textwrap to format the text and break lines
        wrapped_text = textwrap.fill(list_text, width=self.line_width)

        return wrapped_text

    def print_formatted(self):
        formatted_text = self.format_as_text()
        print(formatted_text)

# Display unique values of a dataframe column side by side
            
class DataFrameProcessor:
    def __init__(self, df, column_names):
        self.df = df
        self.column_names = column_names

    def process_data(self):
        df_display = []

        for column in self.column_names:
            unique_df = self.df[column].drop_duplicates().dropna().reset_index(drop=True)
            df_display.append(unique_df)

        return pd.concat(df_display, axis=1).head(30)   


    


