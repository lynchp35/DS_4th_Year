import pandas as pd
import numpy as np
import datetime
import os

class Resample_data():
    
    def __init__(self, path, og_path = "../data/D1NAMO/"):
        self.path = path
        self.og_path = og_path
        
        
    def create_resampled_data(self):
        colNames = ["V_Mean","L_Mean","S_Mean","V_STD","L_STD","S_STD",
        "B_WF_Mean","B_WF_STD","ECG_WF_Mean","ECG_WF_STD"]

        for subject_type in os.listdir(self.path):
            total_len = len(os.listdir(f"{self.path}/{subject_type}"))/100
            count = 0
            for subject_ID in os.listdir(f"{self.path}/{subject_type}"):
                df_list = []
                for day in os.listdir(f"{self.path}/{subject_type}/{subject_ID}/sensor_data"):
                    try:
                        df = pd.read_csv(f"{self.og_path}/{subject_type}/{subject_ID}/sensor_data/{day}/{day}_Summary.csv")

                        accel = pd.read_csv(f"{self.og_path}/{subject_type}/{subject_ID}/sensor_data/{day}/{day}_Accel.csv")
                        breathing = pd.read_csv(f"{self.og_path}/{subject_type}/{subject_ID}/sensor_data/{day}/{day}_Breathing.csv")  
                        ecg = pd.read_csv(f"{self.og_path}/{subject_type}/{subject_ID}/sensor_data/{day}/{day}_ECG.csv")
                        current_df = join_files(accel,breathing, ecg)
                        df_list.append(current_df)
                    except FileNotFoundError:
                        print(f"{self.og_path}/{subject_type}/{subject_ID}/sensor_data/{day}/")
                subject_df = pd.concat(df_list)
                subject_df.to_csv(f"{self.path}/{subject_type}/{subject_ID}/sensor_data/resampled_sensor_data.csv")
                count+= 1
                print(f"{(count/total_len):.2f}% of the way done.")


def join_files(accel,breathing, ecg):

    accel["Time"] = pd.to_datetime(accel["Time"], format="%d/%m/%Y %H:%M:%S.%f")
    breathing["Time"] = pd.to_datetime(breathing["Time"], format="%d/%m/%Y %H:%M:%S.%f")
    ecg["Time"] = pd.to_datetime(ecg["Time"], format="%d/%m/%Y %H:%M:%S.%f")

    new_df = accel.set_index("Time").resample("s").mean().copy().rename(
        columns={"Vertical":"V_Mean","Lateral":"L_Mean","Sagittal":"S_Mean"})

    accel_std = accel.set_index("Time").resample("s").std().copy().rename(
        columns={"Vertical":"V_STD","Lateral":"L_STD","Sagittal":"S_STD"})
    new_df = new_df.join(accel_std)

    breathing_mean = breathing.set_index("Time").resample("s").mean().copy().rename(
        columns={"BreathingWaveform":"B_WF_Mean"})
    new_df = new_df.join(breathing_mean)

    breathing_std = breathing.set_index("Time").resample("s").std().copy().rename(
        columns={"BreathingWaveform":"B_WF_STD"})
    new_df = new_df.join(breathing_std)

    ecg_mean = ecg.set_index("Time").resample("s").mean().copy().rename(
        columns={"EcgWaveform":"ECG_WF_Mean"})
    new_df = new_df.join(ecg_mean)

    ecg_std = ecg.set_index("Time").resample("s").std().copy().rename(
        columns={"EcgWaveform":"ECG_WF_STD"})
    new_df = new_df.join(ecg_std)


    return new_df

if __name__ == "__main__":
    rd = Resample_data(path = "../data/processed_data", og_path = "../data/D1NAMO")
    
    rd.create_resampled_data()