import json
import pickle
import pymongo
import numpy as np
import pandas as pd
import seaborn as sns
from tensorflow import keras
from datetime import datetime,timedelta
import matplotlib.pyplot as plt
# import tensorflow.keras as keras
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler, StandardScaler
scaler = StandardScaler()

# FOR DATA PREPARATION
import data_pre_processing_module as dp
plot_width = 15
plot_height =10

model = keras.models.load_model('hydpressure.keras')
# Time periods for testing
#=============
# X_Train
#=============
start_time            = datetime(2023,3,19,16,0,0)
end_time = datetime(2023,3,20,8,0,0)
column_list     = ["hyd_unitMain_psi"] 
n_steps         = 19
ob              = dp.data_pre_processing()
press_number    = 24
ob.set_values(start_time,end_time,n_steps,column_list)
X,df = ob.mongodb_to_X()
X_train = scaler.fit_transform(X.reshape(-1, X.shape[-1])).reshape(X.shape) # Scaling of the data
plt.figure() 
plt.plot(df,label=df.columns)
plt.legend()
plt.grid(True)
plt.show()
plt.plot(X_train[:,0]) # Plot after scaling
plt.title(f"Test Data {start_time} to {end_time}")
plt.xlabel("Date Time")
# plt.ylabel(column) 
plt.legend()
plt.grid(True)
plt.show()

'''
# time_now = datetime.now()
# time_two_hour_before = datetime.now()-timedelta(minutes=120)
time_now = datetime(2023,4,24,0,0,0)
time_two_hour_before = datetime(2023,4,24,6,0,0)
column_list     = ["rm_frCnut_vrms","rm_flCnut_vrms","rm_rrCnut_vrms","rm_rlCnut_vrms"] 
# column_list     = ["cr_frEccPin_vrms","cr_flEccPin_vrms","cr_rrEccPin_vrms","cr_rlEccPin_vrms"]
# column_list     = ["rm_frGb_vrms","rm_flGb_vrms","rm_rrGb_vrms","rm_rlGb_vrms"]
n_steps         = 19
ob              = dp.data_pre_processing()
press_number    = 24
ob.set_values(time_two_hour_before,time_now,n_steps,column_list)
X,df = ob.mongodb_to_X()
X_train = scaler.fit_transform(X.reshape(-1, X.shape[-1])).reshape(X.shape) # Scaling of the data
plt.figure(figsize=(25,3)) # Plot after scaling
plt.plot(df,label=df.columns)
plt.legend()
plt.show()
plt.plot(X_train[:,0])
plt.title(f"Test Data {time_two_hour_before} to {time_now}")
plt.xlabel("Date Time")
# plt.ylabel(column) 
plt.legend()
plt.grid(True)
plt.show()


def plot_actual_vs_reconstruct(data):
    # plt.figure(figsize=(15,3))
    fig,ax = plt.subplots(1,2)
    fig.set_figheight(3)
    fig.set_figwidth(15)
    reconstructed = model.predict(data)
    reconstructed=reconstructed.reshape(-1, reconstructed.shape[-1])
    reconstructed = scaler.inverse_transform(reconstructed).reshape(reconstructed.shape)
    print("shape after:",reconstructed.shape)

    data=data.reshape(-1, data.shape[-1])
    print("shape after:",data.shape)
    data = scaler.inverse_transform(data).reshape(data.shape)
    print("shape after:",data.shape)
    ax[0].plot(data[:,0] ,color='g',label='Original Data')
    ax[0].set_xlim(0,500)
    ax[1].plot(reconstructed[:,0], label='Reconstructed Data')
    ax[1].set_xlim(0,500)
    # ax[1].set_ylim(0,0.010)
    plt.legend()
    # plt.xlim(0,500)
    plt.show()
# Reconstructing the data
plot_actual_vs_reconstruct(X_train)
def predict(model, test_data):
    # Use LSTM Autoencoder model to predict on test data.
    predictions = model.predict(test_data)
    return predictions

def calculate_error(test_data, predictions):
    """Calculate mean squared error between test data and predictions."""
    print("test_data shape", test_data.shape)
    print("predictions shape", predictions.shape)
    mse = np.mean(np.square(test_data - predictions), axis=1)
    return mse
def test_model(data):
    # model = keras.models.load_model('LSTMautoencoders')
    # plt.figure(figsize=(plot_width,plot_height))
    trainPredict    = predict(model, data)
    trainMSE        = calculate_error(data, trainPredict)
    df_columns_list = list(df.columns)
    range_start=0
    range_end = len(df_columns_list)
    plt.figure(figsize=(plot_width,plot_height))
    for i in range(range_start,range_end):
        plt.plot(trainMSE[:,i],label=df_columns_list[i])
    plt.show()
        

test_model(X_train)
'''