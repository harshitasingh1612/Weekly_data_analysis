Instructions to execute the python folder
1. Download and Extract the Zipped Folder
    *  Extract the Files:

        * On Windows:

            Right-click the .zip file.
            Choose “Extract All…”.
            Follow the prompts to choose a destination folder and extract the contents.
        * On macOS:

            Double-click the .zip file.
            The files will be extracted to the same location as the .zip file.
        * Using Command Line:
            ## Windows
                tar -xf yourfile.zip

            ## MacOS
                unzip yourfile.zip

## 2. Navigate to the directory of the extracted folder
    cd path/to/extracted_folder

        
## 3. Set Up a Virtual Environment:
    python -m venv venv
    source venv/bin/activate

## 4. Install dependencies:
    pip install -r requirements.txt

## 5. Run the python script
    python main.py
