from Press_Universal import press_universal_describe
from datetime import datetime
import pandas as pd

pd.set_option("display.max_rows", None)

def task1(press):
    while True:
        try:
            #press = int(input("Which press do you want to analyze? Input only the number of the press: "))
            #print(" ")
            sheet = f'{press}_production_details'
            df = pd.read_excel('live_sensors_list.xlsx',sheet_name = sheet)
            break
        except ValueError:
            print('Invalid Press. Try again')
    for i in range(len(df.columns.to_list())):
        print(f'{i+1}: {df.columns.to_list()[i]}')
    while True:
        try:
            job = int(input("Enter index number of job you want to analyze: "))
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
            while True:
                try:
                    time_index = int(input('Enter index of time you want to analyze: ')) - 1
                    print(" ")
                    if time_index < 0 or time_index >= len(production_times):
                        raise(IndexError)
                    time = production_times[time_index].split(",")
                    break
                except (IndexError):
                    print('Invalid index, try again')
            for i in range(len(time)):
                time[i] = int(time[i])
            production_time = [[datetime(time[0],time[1],time[2],time[3],time[4],time[5]),datetime(time[6],time[7],time[8],time[9],time[10],time[11])]]
            select_values = int(input("Press 0 for getting all the sensor values. Press 1 for getting specific sensor values "))
            while True:
                try:
                    if select_values not in [0,1]:
                        raise(IndexError)
                    break
                except IndexError:
                    print('Input either 0 or 1')
            while True:
                try:
                    if select_values == 0:
                        sensor_list = []
                    elif select_values == 1:
                        sensor_string = input("Input sensors for which you want the data, separated by a comma if there are multiple. Don't include any whitespaces anywhere: ")
                        if "," in sensor_string:
                            sensor_list = sensor_string.split(",")
                        else:
                            sensor_list = [sensor_string.strip()]
                    angle_start = 0
                    angle_end = 90
                    while True:
                        try:
                            if press != "Press_21" or press!= "Press_05":
                                [angle_start,angle_end] = input('Input press angles to be analyzed separated by a comma: ').split(",")
                                angle_start = int(angle_start)
                                angle_end = int(angle_end)
                                if angle_start >= angle_end or angle_start < 0 or angle_end > 360:
                                    raise(ValueError)
                            break
                        except ValueError:
                            print('Press angles are invalid')
                    print('Fetching data... Please wait')
                    ans_df = press_universal_describe(press,production_time,angle_start,angle_end,sensor_list)
                    print(ans_df)
                    break
                except KeyError:
                    raise(KeyError)
                except ValueError:
                    raise(ValueError)
            break
        except KeyError:
            print('Time of production entered has no data. Please try again')
            # print(KeyError)
        except ValueError:
            print('Time of production entered has no data. Please try again')

if __name__ == '__main__':
    analyze_again = 'y'
    while analyze_again == 'y':
        task1()
        analyze_again = input('Do you want to continue analyzing? y or n: ')