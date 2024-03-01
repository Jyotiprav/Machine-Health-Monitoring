from Press_Universal import press_universal,excel_plotter
from datetime import datetime
import pandas as pd

pd.set_option("display.max_rows", None)

def task3():
    while True:
        try:
            press1 = (input("Which press do you want to analyze first? Input only the number of the press: "))
            print(" ")
            if press1=='5':
                sheet='Press_05_production_details'
            else:
                sheet = f'Press_{press1}_production_details'
            print(sheet)
            df = pd.read_excel('live_sensors_list.xlsx',sheet_name = sheet)
            break
        except ValueError:
            print('Invalid Press. Try again')
    for i in range(len(df.columns.to_list())):
        print(f'{i+1}: {df.columns.to_list()[i]}')
    while True:
        try:
            job = int(input("Enter index number of first job you want to analyze: "))
            print(" ")
            if job == 0:
                raise(IndexError)
            production_times = df.iloc[:,job-1].dropna().to_list()
            break
        except IndexError:
            print('Invalid index, try again')
    while True:
            try:
                for i in range(len(production_times)):
                    print(f'{i+1}: {production_times[i]}')
                time_index = int(input('Enter index of time you want to analyze: ')) - 1
                print(" ")
                if time_index < 0 or time_index >= len(production_times):
                    raise(IndexError)
                time = production_times[time_index].split(",")
                for i in range(len(time)):
                    time[i] = int(time[i])
                production_time1 = [[datetime(time[0],time[1],time[2],time[3],time[4],time[5]),datetime(time[6],time[7],time[8],time[9],time[10],time[11])]]
                break
            except (IndexError):
                print('Invalid index, try again')

    angle_start1 = 0
    angle_end1 = 90
    while True:
            try:
                if press1 != '21' or press1!= '5' or press1!='05':
                    [angle_start1,angle_end1] = input('Input press angles to be analyzed separated by a comma: ').split(",")
                    angle_start1 = int(angle_start1)
                    angle_end1 = int(angle_end1)
                    if angle_start1 >= angle_end1 or angle_start1 < 0 or angle_end1 > 360:
                        raise(ValueError)
                break
            except ValueError:
                print('Press angles are invalid')
    while True:
        try:
            press2 = (input("Which press do you want to analyze first? Input only the number of the press: "))
            if press2=='5':
                sheet = 'Press_05_production_details'
            else:
                sheet = f'Press_{press2}_production_details'
            print(" ")
            df = pd.read_excel('live_sensors_list.xlsx',sheet_name = sheet)
            break
        except ValueError:
            print('Invalid Press. Try again')
    for i in range(len(df.columns.to_list())):
        print(f'{i+1}: {df.columns.to_list()[i]}')
    while True:
        try:
            job = int(input("Enter index number of first job you want to analyze: "))
            print(" ")
            if job == 0:
                raise(IndexError)
            production_times = df.iloc[:,job-1].dropna().to_list()
            break
        except IndexError:
            print('Invalid index, try again')
    while True:
            try:
                for i in range(len(production_times)):
                    print(f'{i+1}: {production_times[i]}')
                time_index = int(input('Enter index of time you want to analyze: ')) - 1
                print(" ")
                if time_index < 0 or time_index >= len(production_times):
                    raise(IndexError)
                time = production_times[time_index].split(",")
                for i in range(len(time)):
                    time[i] = int(time[i])
                production_time2 = [[datetime(time[0],time[1],time[2],time[3],time[4],time[5]),datetime(time[6],time[7],time[8],time[9],time[10],time[11])]]
                break
            except (IndexError):
                print('Invalid index, try again')

    angle_start2 = 0
    angle_end2 = 90
    while True:
            try:
                if press2 != '21' or press2!='5' or press2!='05':
                    [angle_start2,angle_end2] = input('Input press angles to be analyzed separated by a comma: ').split(",")
                    angle_start2 = int(angle_start2)
                    angle_end2 = int(angle_end2)
                    if angle_start2 >= angle_end2 or angle_start2 < 0 or angle_end2 > 360:
                        raise(ValueError)
                break
            except ValueError:
                print('Press angles are invalid')

    while True:
        try:
            select_values = int(input("Press 0 for getting all the sensor values. Press 1 for getting specific sensor values "))
            while True:
                try:
                    if select_values not in [0,1]:
                        raise(IndexError)
                    break
                except IndexError:
                    print('Input either 0 or 1')
            if select_values == 0:
                sensor_list = []
            elif select_values == 1:
                sensor_string = input("Input sensors for which you want the data, separated by a comma if there are multiple. Don't include any whitespaces anywhere: ")
                if "," in sensor_string:
                    sensor_list = sensor_string.split(",")
                else:
                    sensor_list = [sensor_string.strip()]
            break
        except KeyError:
            raise(KeyError)
        except ValueError:
            raise(ValueError)
        
    file_name = input('Whta do you want to name your file?: ')
    if press1=='05':
        press1=5
    if press2=='05':
        press2=5
    df1 = press_universal(int(press1),production_time1,angle_start1,angle_end1,[])
    df2 = press_universal(int(press2),production_time2,angle_start2,angle_end2,[])
    excel_plotter(file_name,[df1,df2])

if __name__ == '__main__':
    analyze_again = 'y'
    while analyze_again == 'y':
        task3()
        analyze_again = input('Do you want to continue analyzing? y or n: ')