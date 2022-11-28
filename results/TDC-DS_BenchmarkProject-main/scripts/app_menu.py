import os, shutil
import csv

# Print Options
definiton_data_folder = "/data_vol_shared"
definiton_q_folder = "/queries_vol_shared"
scripts_path = "/scripts/"

def print_menu():
    print("[1] Generate the data.")
    print("[2] Create schemas and load data.")
    print("[3] Run queries.")
    print("[4] Exit.")


# Definitons of functions
def o1_generatedata():
    print(" ")
    print("Generate the data")
    scale = float(input('[PARAMS] Enter the scale factor: '))
    
    print(" ")
    try:
        os.mkdir(definiton_data_folder)
    except:
        print('File exist.')
    os.chdir("/tpcds-kit/tools")
    os.system(f"./dsdgen -SCALE {str(scale)} -DIR {definiton_data_folder}")
    # DELIMITER =  <s>         -- use <s> as output field separator |
    # SUFFIX =  <s>            -- use <s> as output file suffix
    # TERMINATE =  [Y|N]       -- end each record with a field delimiter |
    # FORCE =  [Y|N]           -- over-write data files without prompting
    dir_name = definiton_data_folder
    files = os.listdir(definiton_data_folder)
    for i in files:
        try:
            os.mkdir(os.path.join(dir_name, i.split(".")[0]))
            shutil.move(os.path.join(dir_name, i), os.path.join(dir_name , i.split(".")[0]))
        except:
            print("Folder or file exists. Remove them first.")

    print(" ")
    print("Complete: Data generation")


def o1_generatequeries():
    print(" ")
    print("Generate the queries")
    
    try:
        os.mkdir(definiton_q_folder)
    except:
        print('File exist.')
    scale = float(input('[PARAMS] Enter the scale factor: '))

    print(" ")
    os.chdir("/tpcds-kit/tools")
    os.system(f"""./dsqgen  -DIRECTORY /tpcds-kit/query_templates  -INPUT /tpcds-kit/query_templates/templates.lst  -VERBOSE Y -QUALIFY Y  -SCALE {str(scale)} -DIALECT sparksql  -OUTPUT_DIR {definiton_q_folder}""")
    # DELIMITER =  <s>         -- use <s> as output field separator |
    # SUFFIX =  <s>            -- use <s> as output file suffix
    # TERMINATE =  [Y|N]       -- end each record with a field delimiter |
    # FORCE =  [Y|N]           -- over-write data files without prompting
    
    print(" ")
    os.chdir("/home/")
    print("Complete: Queries generation")


def o2_createschemas():
    print(" ")
    print("Create schemas")
    os.system(f"python3 {scripts_path}import_data.py")
    print("Finished creating schemas and importing the data.")


def o3_generatedata():
    print(" ")
    print("Run queries")


while(True):
    print_menu()
    try:
        option = int(input('Enter your choice: '))
    except:
        print("ERROR --- Incorrect input.")
        continue
    #Check the selection
    if option == 1:
        o1_generatedata()
    elif option == 2:
        o2_createschemas()
    elif option == 3:
        o1_generatequeries()
    elif option == 4:
        print('Exit')
        exit()
    else:
        print('Invalid option. Please enter a number between 1 and 4.')
