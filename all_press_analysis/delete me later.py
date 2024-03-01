import datetime
import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient
client = MongoClient ('mongodb://128.121.34.13') # mongodb instance
db=client["Press_11"] #Database
collection = db["BATCH_1"] #collection
 
now = datetime.datetime.now()
desired_date = datetime.datetime(2024,1,11,10,0)
time_from = desired_date - datetime.timedelta(hours=4,minutes=0)
 
#result1=collection.find({"Date": {"$gte": time_from, "$lt": desired_date}},{"Date":1,"_flyWheelShaftBearing_temp": 1,"_flyWheelShaftBearing_tempRtd": 1}) # updated query

# result2=collection.find({"Date": {"$gte": time_from, "$lt": desired_date}},{"_flyWheelShaftBearing_tempRtd": 1}) -- incorrect one
result1=collection.find({"Date": {"$gte": time_from, "$lt": desired_date}},{"Date":1,"R_L_Pin_Temp": 1,"R_R_Pin_Temp": 1 , "F_R_Pin_Temp":1, "F_L_Pin_Temp":1})
df = pd.DataFrame(result1).set_index("Date")


# df['Date'] = pd.date_range(start=time_from, end=desired_date, periods)

# df1 = pd.DataFrame(result2)
# df1['Date'] = pd.date_range(start=time_from, end=desired_date, periods=len(df1))


#plotting the graph of Cl_Temp vs the Date
#plt.plot(df['_flyWheelShaftBearing_temp'],label='flyWheelShaftBearing_temp')
#plt.plot(df['_flyWheelShaftBearing_tempRtd'],label='flyWheelShaftBearing_tempRtd')

plt.plot(df['F_L_Pin_Temp'],label='F_L_Pin_Temp')
plt.plot(df['F_R_Pin_Temp'],label='F_R_Pin_Temp')
plt.plot(df['R_L_Pin_Temp'],label='R_L_Pin_Temp')
plt.plot(df['R_R_Pin_Temp'],label='R_R_Pin_Temp')
 
 
plt.title("Graph for Two sensors from 26_Jan to 29_Jan ")
plt.xlabel('Date')
plt.ylabel('Temperature measurements ')
plt.legend()
plt.grid()
plt.show()