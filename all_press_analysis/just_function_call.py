from MAplot import *
from sensorplot import *
# plotjobdata(anglestart, angleend, sensor, dates, "Press_Angle", batch_info_dict, press_db, jobdates, part)


column_list = []
batch_info_dict = {}
analyzing = True
task = 0
current_dir = os.getcwd()
# Put onedrive link
# current_dir = r"C:\Users\JSharma\OneDrive - Martinrea Inc\General - Telemetry - Analysis and findings"
#====================================================================================
#Connect to mongo client
myclient_global = pymongo.MongoClient(host = '128.121.34.13', Connect = True)
#=====================================================================================
#Ask for press and set batches
press = "Press_11"
press_db = myclient_global[press]
if press == "Press_11" or press == "Press_21" or press=="Press_05":
    batches = [1,2]
elif press == "Press_24":
    batches = [1,2,3,4]
#=====================================================================================
#Get all Sensors in a list
for batch in batches:
    collection = press_db["BATCH_" + str(batch)]
    document = collection.find().limit(1).sort("_id", -1)
    for field in [document[0].keys()]:
        column_list += field
#Turn column_list into set and back into list to avoid duplicate values and removing date and id and other unneccesary columns
column_list = set(column_list)
column_list = list(column_list)
column_list.remove("Date")
column_list.remove("_id")
if press == "Press_11":
    column_list.remove("Ram_Adjust_Chain_On/Off")
#=======================================================================================
#Sorting sensors into their batch
for column in column_list:
    for batch in batches:
        collection = "BATCH_"+str(batch)
        db = press_db[collection]
        results = db.find().limit(1).sort("_id",-1)
        recent_doc = results[0]
        if column in recent_doc.keys():
            if batch not in batch_info_dict:
                batch_info_dict[batch] = [column]
            else:
                batch_info_dict[batch].append(column)
#===================================================================
#sorting lists in batch info dict so it is easier to plot on excel
for batch in batch_info_dict:
    batch_info_dict[batch].sort()

prodsheet = pd.read_excel((current_dir + "\live_sensors_list.xlsx"), sheet_name= press + "_production_details")
jobsheet = pd.read_excel((current_dir + "\live_sensors_list.xlsx"), sheet_name= press + "_job_details")
print("Job Details (Tonnage, Counterbalance, SPM)")
print(jobsheet.to_string(index = False))
print()
(("Job Selection"))
#print out the job list
joblist = prodsheet.columns.to_list()
#print out all jobs and the correlated number
for job in enumerate(joblist):
    print(job)
print()
# jobchosen = int(input("Please enter the number correlated to what job you would like to choose: "))
# jobchosen=14 # 'MRT-68430748/49'
jobchosen=3 # MRT-CH68058140/41
jobchosen=15 # (15, 'MRT-68430782/83')
print()
#set the dates based on the job chosen and print the jobs
jobdates = prodsheet.iloc[:, jobchosen]
print(jobdates)
print()
#getting part name for plot title
part = joblist[jobchosen]
# For 4 sensor moving average
anglestart = 0
angleend = 360
# sensor1     = "F_L_Pin_Crst"
# sensor2     = "F_R_Pin_Crst"
# sensor3     = "R_L_Pin_Crst"
# sensor4     = "R_R_Pin_Crst"
# dates       = "1,4,10,12,19,21,23,29" # 'MRT-68430748/49'
dates = "20,29,30,31,32"
#Press 11
# movingmultijob(anglestart, angleend, sensor1, sensor2, sensor3, sensor4, dates, "Press_angle", batch_info_dict, press_db, jobdates, part)
sensor1 = "Cr_LBlock1_Pre"
sensor2 = "Cr_LBlock1_Pre"
sensor3 = "Lub_PreFltr_Pre"
sensor4 = "Lub_PostFltr_Pre"
movingmultijob(anglestart, angleend, sensor1, sensor2, sensor3, sensor4, dates, "Press_angle", batch_info_dict, press_db, jobdates, part)

# sensor1 = "F_L_Gibbs_Arms"
# sensor2 = "F_R_Gibbs_Arms"
# sensor3 = "R_L_Gibbs_Arms"
# sensor4 = "R_R_Gibbs_Arms"
# movingmultijob(anglestart, angleend, sensor1, sensor2, sensor3, sensor4, dates, "Press_angle", batch_info_dict, press_db, jobdates, part)

# sensor1 = "F_R_Pin_Vrms"
# sensor2 = "F_R_B_Vrms"
# sensor3 = "F_R_Gibbs_Vrms"
# sensor4 = "F_R_Nut_Vrms"
# movingmultijob(anglestart, angleend, sensor1, sensor2, sensor3, sensor4, dates, "Press_angle", batch_info_dict, press_db, jobdates, part)

# Plot raw data press 11
# plotjobdata(anglestart, angleend, sensor1, datesyes, "Press_angle", batch_info_dict, press_db, jobdates, part)