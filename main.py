'''
Project (Assignment Task) :- Converting Sensor data to Gen-1 data as decribed in the problem statement.

The raw data is cleansed into a Gen-1 data format, here the following operations are 
performed: 
a. For Inverters 
i. For inverters, column i32 indicates the timestamp of the row. Make this as the first column in 
the Gen1 file and rename the column header to Timestamp). 
b. For Energy meters (MFM) 
i. Same rules as above, only difference is timestamp column is m63 
c. For Energy meters (MFM) 
i. Same rules as above, only difference is timestamp column is w23 

Output: Gen-1 Data file zipped and python program file.

######Task Submitted By: NIKHIL SOMANI ######
'''


#import the required modules
import zipfile #zipfile to handle the zip raw data file 
import os # os to perform directory operation and searches
import pandas as pd # pandas to handle the raw data file in a dataframe.

#open the zipped raw file as an object f
with zipfile.ZipFile('raw.zip', 'r') as f:
    #using the object extract it to a folder Gen-1 (output folder to be zipped) in the current working directory
    #although operations can be done without extracting but its easy for smaller 
    #file size.
    f.extractall('Gen-1/') #extracted in Gen-1 to peform the operation in that folder only.

print("Reading your given raw input file.................\n")
#define a general path or root path we are going to use for desired output results.
gen_path = 'Gen-1/[IN-023C]/'

#taking the list of all the year we have in this station [IN-023C] say 2018 and 2019 in our case
year_list = os.listdir(gen_path)

print("Processing your file for operations............., please wait.....\n")
#traversing each year folder from the year list
for year_path in year_list:

    print("Processing the data for year {} .......\n".format(year_path))
    path1 = gen_path + year_path + '/' #adding the slash for correcting the path
    year_month_list = os.listdir(path1) #finding out all the months list in that year
    #in our case its only one month but safer side will have 12 months in an year.

    #traversing each month folder from the month list
    for year_month_path in year_month_list:
        path2 = path1 + year_month_path + '/' #adding the slash for correcting the path
        devices_list = os.listdir(path2) #finding out all the devices list in the month folder
        #in our case 2 inverters, 1MFM and 1 WMS

        #traversing each device folder from the device list
        for device_folder in devices_list:
            path3 = path2 + device_folder + '/' #adding the slash for correcting the path
            files_path_list = os.listdir(path3) #finding put all the list of files stored 
            #in this device folder , almost one file per day for that month.
            
            #traversing each file from that selected device folder and working on it
            for file_path in files_path_list:
                path4_str =  path3  + file_path #not to add slash here, its a txt file path
                path4_str = path4_str.replace('/', '//') #path workaround for unix file access.

                #convert the file to a dataframe using the path and delimiter as \t for txt file format
                df = pd.read_csv(path4_str, delimiter='\t') #file access using pandas read_csv function
                
                #condition as if device is any of the two inverters then
                if device_folder == 'Inverter_1' or device_folder == 'Inverter_2':
                    #pop the column and store in a series
                    f_col = df.pop('i32')
                    #insert that popped column into starting with the name "Timestamp"
                    df.insert(0, 'Timestamp', f_col)
                    #finally save back to same file and separator= \t for text file format.
                    df.to_csv(path4_str, index=False, sep='\t')

                #condition as if device is MFM then
                elif device_folder == 'MFM':
                    #pop the column and store in a series
                    f_col = df.pop('m63')
                    #insert that popped column into starting with the name "Timestamp"
                    df.insert(0, 'Timestamp', f_col)
                    #finally save back to same file and separator= \t for text file format.
                    df.to_csv(path4_str, index=False, sep='\t')
                    
                #condition as if device is WMS then
                elif device_folder == 'WMS':
                    #pop the column and store in a series
                    f_col = df.pop('w23')
                    #insert that popped column into starting with the name "Timestamp"
                    df.insert(0, 'Timestamp', f_col)
                    #finally save back to same file and separator= \t for text file format.
                    df.to_csv(path4_str, index=False, sep='\t')

print("All Operations are Successfully Performed !!!")




 

