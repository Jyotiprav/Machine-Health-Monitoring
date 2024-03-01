# lIBRARY IMPORTS
import json
import pickle
import pymongo
import numpy as np
import pandas as pd
import seaborn as sns
from tensorflow import keras
from datetime import datetime
import matplotlib.pyplot as plt
# import tensorflow.keras as keras
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler, StandardScaler
# FOR DATA PREPARATION
import data_pre_processing_module as dp
scaler = StandardScaler()

sns.set()

def plot_data(X,df):
    if len(df)!=0:
        column =(list(df.columns))[0]
        print("--> Number of null values present in the dataset:",sum(df.isnull().any()))
        # Plot before scaling
        plt.figure(figsize=(15,3))
        plt.plot(df[column],label=column)
        plt.title(f"Training data {start} to {end}")
        plt.xlabel("Date Time")
        plt.ylabel(column) 
        plt.grid(True)
    else:
        print("Dataframe is empty.")
def build_model(input_shape,nodes):
    # build network topology
    model = keras.Sequential()
    # 1 LSTM layers                                                     
    model.add(keras.layers.LSTM(nodes, activation="relu", input_shape=input_shape))                                  
    #model.add(keras.layers.Dropout(rate=0.2))                                                   
    model.add(keras.layers.RepeatVector(X_train.shape[1]))                    
    #decoder                                                                                          
    model.add(keras.layers.LSTM(nodes, activation="relu",return_sequences=True))                                          
    #model.add(keras.layers.Dropout(rate=0.2))                                                         
    model.add(keras.layers.TimeDistributed(keras.layers.Dense(X_train.shape[2])))         
    model.compile(optimizer='adam', loss='mse')                                           
    return model

# ==========================================================================
#                           DATA PREPARATION
# ==========================================================================
# TRAINING DATA 
# start           = datetime(2023,3,28,3,50,0)
# end             = datetime(2023,3,28,3,59,0)
threshold_sheet = "press_threshold_list.xlsx"

#================ get current job from database=======================================
column_list     = ["hyd_unitMain_psi"] 
n_steps         = 19
ob              = dp.data_pre_processing()
# current_job     = ob.get_current_jobId()
current_job     = "GM23398597_98"
press_number    = 24
print("*"*100)
try:
    thresholds = pd.read_excel(threshold_sheet,f"Press {press_number}").set_index("Jobs").to_dict()
    start_and_end = [thresholds["Start date"][current_job], thresholds["End date"][current_job]]
    # Entering dates for testing March 19 Hyd unit pressure
    start_and_end=['2023,3,19,16,0,0', '2023,3,20,8,0,0']
    print(start_and_end)
    for date in start_and_end:
        #splitting the date
        runtime = date.split(',')
        #changing it all from str to int to be put into datetime format
        for i in range(0, len(runtime)):
            runtime[i] = int(runtime[i])
        if date == start_and_end[0]:
            job_start_date = datetime(runtime[0], runtime[1],runtime[2],runtime[3],runtime[4],runtime[5])
        elif date == start_and_end[1]:
            job_end_date = datetime(runtime[0], runtime[1],runtime[2],runtime[3],runtime[4],runtime[5])
    print("Current Job:",current_job,"Threshold start:",job_start_date,"Threshold ends:",job_end_date)
except Exception as E:
    print("Not able to read threshold sheet.",E)
ob.set_values(job_start_date,job_end_date,n_steps,column_list)
X,df = ob.mongodb_to_X()
X_train = scaler.fit_transform(X.reshape(-1, X.shape[-1])).reshape(X.shape) # Scaling of the data
plt.figure(figsize=(25,3)) # Plot after scaling
# plt.plot(df,label=df.columns)
plt.plot(X_train[:,0])

def get_thresholds():
# Get the mean and 95% percentile for each of the 4 features (0,1,2,3)
    threshold_dict = pd.DataFrame(X_train[:,0]).describe(percentiles=[0.95]).to_dict()
    for key in threshold_dict: 
        for metric in list(threshold_dict[key]):
            if metric not in ['mean','95%']:
                del threshold_dict[key][metric]
    return threshold_dict

def check_if_exceeds_thresholds():

    for i, reconstruction in enumerate(X_train[:,0]):
        feature_num = i%4
        if reconstruction > get_thresholds()[feature_num]['95%']:
            pass
            

plt.title(f"Data {job_start_date} to {job_end_date}")
plt.xlabel("Date Time")
# plt.ylabel(column) 
plt.legend()
plt.grid(True)
plt.show()


# ==============================================================================
#                           MODEL TRAINING on threshold
# ==============================================================================

#Run to train the model using LSTM autoencoders 
input_shape = (X_train.shape[1], X_train.shape[2])
# print("input_shape",input_shape)
model = build_model(input_shape,nodes=32)
model.summary()
# # fit model
history = model.fit(X_train, X_train, epochs=6, batch_size=32, validation_split=0.1, verbose=1)
plt.plot(history.history['loss'],       label='Training loss')
plt.plot(history.history['val_loss'],   label='Validation loss')
plt.legend()
plt.show()


model.save("hydpressure.keras") # Save the model

