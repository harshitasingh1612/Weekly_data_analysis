Instructions to execute the python program for running the PART B of the project.


1. Download and Extract the Zipped Folder
    *  Extract the Files:

        * On Windows:

            * Right-click the .zip file.
            * Choose “Extract All…”.
            * Follow the prompts to choose a destination folder and extract the contents.
        * On macOS:

            * Double-click the .zip file.
            * The files will be extracted to the same location as the .zip file.
        * Using Command Line:
            ## Windows
                tar -xf Harshita_Singh_weekly_data_analysis.zip

            ## MacOS
                unzip Harshita_Singh_weekly_data_analysis.zip

## 2. Navigate to the directory of your extracted folder location
    cd path/to/extracted_folder

## 3. Set Up a Virtual Environment:
    python -m venv harshita_singh_venv source harshita_singh_venv/bin/activate

## 4. Install dependencies:

    pip install -r requirements.txt
Note - After installing the dependencies, any execution whether script or pytest would be slow due to python loading the libraries in memory.

## 5. Run the python script
    python main.py


## 6. Execute tests
    pytest


Improvements/ Enhancements : 
* As mentioned in the requirements it's a relatively small dataset (<1,000 rows, <10 columns), I am using pandas in Python for the data transformation and Oxaca blinder metric decomposition. 
* For huge datasets, pandas won't be a good option. So we can use Spark for faster execution and data processing.
