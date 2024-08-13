import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

# Set Paths
tmpfile = "dealership_temp.tmp"  # file used to store all extracted data
logfile = "dealership_logfile.txt"  # all event logs will be stored in this file
targetfile = "dealership_transformed_data.csv"  # file where transformed data is stored

# CSV Extract Function
def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe

# JSON Extract Function
def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process, lines=True)
    return dataframe

# XML Extract Function
def extract_from_xml(xmlfile):
    tree = ET.parse(xmlfile)
    root = tree.getroot()
    extracted_data = []

    for person in root.findall("person"):
        # Handling car_model
        car_model_element = person.find("car_model")
        car_model = car_model_element.text if car_model_element is not None else None

        # Handling year_of_manufacture
        year_of_manufacture_element = person.find("year_of_manufacture")
        if year_of_manufacture_element is not None and year_of_manufacture_element.text.isdigit():
            year_of_manufacture = int(year_of_manufacture_element.text)
        else:
            year_of_manufacture = None

        # Handling price
        price_element = person.find("price")
        if price_element is not None:
            try:
                price = float(price_element.text)
            except ValueError:
                price = None
        else:
            price = None

        # Handling fuel
        fuel_element = person.find("fuel")
        fuel = fuel_element.text if fuel_element is not None else None

        # Append the extracted data to the list or DataFrame
        extracted_data.append({
            "car_model": car_model,
            "year_of_manufacture": year_of_manufacture,
            "price": price,
            "fuel": fuel
            
        })

    return pd.DataFrame(extracted_data)


# Extract Function
def extract():
    extracted_data = pd.DataFrame(columns=['car_model', 'year_of_manufacture', 'price', 'fuel'])
    
    # Process all csv files
    for csvfile in glob.glob("dealership_data/*.csv"):
        extracted_data = extracted_data.append(extract_from_csv(csvfile), ignore_index=True)
        
    # Process all json files
    for jsonfile in glob.glob("dealership_data/*.json"):
        extracted_data = extracted_data.append(extract_from_json(jsonfile), ignore_index=True)
    
    # Process all xml files
    for xmlfile in glob.glob("dealership_data/*.xml"):
        extracted_data = extracted_data.append(extract_from_xml(xmlfile), ignore_index=True)
        
    return extracted_data

# Transform Function
def transform(data):
    data['price'] = round(data.price, 2)
    return data

# Load Function
def load(targetfile, data_to_load):
    data_to_load.to_csv(targetfile, index=False)

# Log Function
def log(message):
    timestamp_format = '%H:%M:%S-%h-%d-%Y'  # Hour-Minute-Second-MonthName-Day-Year
    now = datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open(logfile, "a") as f:
        f.write(timestamp + ',' + message + '\n')

# Running ETL Process
log("ETL Job Started")

log("Extract phase Started")
extracted_data = extract()
log("Extract phase Ended")

log("Transform phase Started")
transformed_data = transform(extracted_data)
log("Transform phase Ended")

log("Load phase Started")
load(targetfile, transformed_data)
log("Load phase Ended")

log("ETL Job Ended")