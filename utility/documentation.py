# Import required libraries
import pandas as pd  # For data manipulation and analysis
import logging  # For logging information and debugging
from datetime import datetime  # For working with date and time

#############################################################################################

# Define the DataFrameDocumenter class
class DataFrameDocumenter:
    """
    A class for documenting DataFrame objects, along with metadata and summary, in an Excel sheet.

    Attributes:
        writer (pd.ExcelWriter): Writer object for writing into Excel.
        logic_summary (str): Summary of the logic used in the code.
        workbook (xlsxwriter.Workbook): Workbook object for Excel.
    """

    def __init__(self, file_path, logic_summary):
        """
        Initialise the DataFrameDocumenter class.

        Parameters:
            file_path (str): Path to save the Excel file.
            logic_summary (str): Summary of the logic used in the code.
        """
        self.file_path = file_path
        self.writer = pd.ExcelWriter(file_path, engine='xlsxwriter')  # Excel Writer object
        self.logic_summary = logic_summary  # Storing the logic summary
        self.workbook = self.writer.book  # Workbook object
        self.write_logic_summary()  # Write the logic summary to the Excel sheet

    def write_logic_summary(self):
        """
        Writes the logic summary in a new worksheet.
        """
        worksheet = self.writer.book.add_worksheet('logic_summary')  # Add new worksheet

        bold = self.workbook.add_format({'bold': True})  # Bold text format
        wrapped = self.workbook.add_format({'text_wrap': True})  # Text wrap format

        worksheet.write('A1', 'Logic Summary', bold)  # Write title
        worksheet.write('A3', self.logic_summary, wrapped)  # Write logic summary with text wrap
        worksheet.set_column('A:B', 100)  # Set column width

    def write_metadata(self, worksheet, workbook):
        """
        Write metadata like date and time in the worksheet.

        Parameters:
            worksheet (xlsxwriter.Worksheet): The Excel worksheet.
            workbook (xlsxwriter.Workbook): The Excel workbook.
        """
        bold = workbook.add_format({'bold': True})  # Bold text format

        current_datetime = datetime.now()  # Get current date and time
        current_datetime = current_datetime.replace(microsecond=0)  # Remove microseconds
        formatted_datetime = current_datetime.strftime('%A, %d %B %Y, %H:%M:%S')  # Format date and time

        worksheet.write('A1', 'Date and Time', bold)  # Write title
        worksheet.write('A2', formatted_datetime)  # Write formatted date and time

    def document_data_excel(self, df, stage, df_name):
        """
        Document the DataFrame and its characteristics in an Excel sheet.

        Parameters:
            df (pd.DataFrame): The DataFrame to be documented.
            stage (str): The stage of the data in the workflow.
            df_name (str): The name of the DataFrame.
        """
        sheet_name = f"{stage}_{df_name}"  # Create sheet name
        
        # Select the first two rows
        subset_df = df.head(2).copy()
        # Apply the transformation to each element of the subset
        for col in subset_df.columns:
            subset_df[col] = subset_df[col].map(lambda x: str(x)[:20] if x is not None else None)
        # Write the transformed subset to Excel
        subset_df.to_excel(self.writer, sheet_name=sheet_name, startrow=4, index=False)

        
        worksheet = self.writer.sheets[sheet_name]  # Get the worksheet
        workbook = self.writer.book  # Get the workbook

        self.write_metadata(worksheet, workbook)  # Write metadata

        bold = workbook.add_format({'bold': True})  # Bold text format

        # Write summaries
        worksheet.write('A4', 'Sample Data', bold)
        worksheet.write('A9', 'Number of Rows', bold)
        worksheet.write('A10', len(df))
        worksheet.write('A11', 'Number of Fields', bold)
        worksheet.write('A12', len(df.columns))

        # Generate and write field summary
        field_summaries = [ {'Field_Name': col, 'Data_Type': str(df[col].dtype), 'Non-NA Values': df[col].count()} for col in df.columns ]
        summary_df = pd.DataFrame(field_summaries)
        summary_df.to_excel(self.writer, sheet_name=sheet_name, startrow=13, index=False)

        logging.info(f"Successfully completed documenting {sheet_name} in Excel.")  # Log info

    def save(self):
        """
        Save the Excel workbook.
        """
        self.writer.close()  # Save the Excel file
        logging.info(f"Successfully saved file to {self.file_path}")




