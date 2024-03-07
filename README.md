# Modify-Attribute
Modify Attribute in Database Script

## Overview
This Python script connects to a SQL Server database, parses specific tables, and modifies attributes based on defined rules. It utilizes the tkinter library for the graphical user interface (GUI) and various data manipulation libraries like pandas and pyodbc.

## Table of Contents
- [Features](#Features)
- [Getting Started](#Getting-Started)
  - [Prerequisites](#Prerequisites)
  - [Installation](#Installation)
- [Usage](#Usage)
- [Contributing](#Contributing)
- [License](#License)


## Features
* ### Database Connection:
  Connects to a SQL Server database using either Windows or SQL Server authentication.
* ### Table Parsing:
  Parses specified tables, combining and processing data to generate a temporary table.
* ### Attribute Filling:
  Fills in missing attributes in the main table based on the processed temporary table.
* ### User Interface:
  Uses tkinter for a simple graphical user interface.
* ### Progress Indicators:
  Displays progress bars to track the completion of specific tasks.

## Getting Started
### Prerequisites
- * Python 3.x
- * Required Python libraries (install using pip install library_name):
  - * tkinter
  - * pandas
  - * pyodbc
  - * sqlalchemy   
### Installation
1. Clone the repository:
'''
git clone https://github.com/Rew-Son/Modify-Attribute.git '''
2. Install required Python libraries:
    ''' pip install -r requirements.txt '''
## Usage
1. Run the script:
   ' python modify_attribute_script.py '
3. Connect to the database, choose authentication type, and provide necessary details.
4. Perform actions like parsing tables, filling attributes, and saving the table.
   
## Contributing
Contributions are welcome! If you find any issues or have suggestions, please open an issue or submit a pull request.


