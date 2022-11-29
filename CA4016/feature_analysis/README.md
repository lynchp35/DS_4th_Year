# Feature Analysis

In this directory, I analysed the various features of several CSV files for each subject.

I start my analysis with the file **initial_analysis.ipynb**


In this file I mainly look at the following CSV files:

1. food.csv
2. glucose.csv
3. insulin.csv (diabetes subset)
4. annnotations.csv (healthy subset)

I do some cleaning and preprocessing of these files for each subject but after some simple analysis, I come to the conclusion that I won't be able to use a majority of the data to derive any meaningful insights. 

The most important insight I gained from this data was that some users were quite clearly more stringent at inputting the necessary data than others. Since the split of "healthy" and diabetic subjects was unbalanced I decided to use this as an opportunity to reduce the size of the data to a more manageable size and also balance the dataset. I picked the top five subjects from each group from an arbitrary calculation of how stringent the subject was. I also included somewhat of a "wildcard" with an extra diabetic subject that was treated for the same condition as the healthy subjects by the research group D1NAMO.

Another more obvious insight that I gained from this analysis was the huge difference in glucose levels between the healthy and diabetic subjects. I explore this further in the file **../base_case_prediction.ipynb**

After this, I was interested in looking into the raw sensor data. I start off with the python script **resample_raw_data.py** which reads in the files:

1. {date}_Accel.csv : 100 Hz, measured 100 times a second
2. {date}_Breathing.csv : 25 Hz, measured 25 times a second
3. {date}_ECG.csv : 250 Hz, measured 250 times a second

As you can see the three files all have different rates of frequency that the data was collected at. I wanted to do further analysis on the datasets so I came to the conclusion that I would either need to resample the data to a uniform frequency and then join them together or I could join all the data onto the ECG dataset so that all the data would have a uniform frequency of 250 Hz and use a method to fill in the null values for the Accel and Breathing data. I soon realised that the second method would not be practical for my circumstances as the ECG data for one day for one subject was already approximately 300MBs. 
So after making the decision of resampling the data up to a lower frequency of 1 Hz I created the python script **resample_raw_data.py** which read in the three files per day, resampled the data then joined them together then combined all the data for each day into one file. 

After running this script the data went from being in the structure of ../data/D1NAMO/{subject_type}/{subject_ID}/sensor_data/{day}/{file_type} to ../data/processed_data/{subject_type}/{subject_ID}/sensor_data/{resampled_data}

I then did some simple analysis on the new resampled data in the file **resample_sensor_data_analysis.ipynb** but after I did this I realise that my naive resampling approach may have lost some valuable data and for this assignment exploring this further may be futile and take up too much time which I did not have. 
I also explored the use of Fourier transformations on the raw data in the hopes of removing noise and gaining some insight into the raw sensor data but again due to my limited time I believed that my time would be best spent analysing the summary sensor data.

I was initially put off analysing the summary sensor data because I was unable to find the meaning behind each feature in the dataset from the D1NAMO paper or online. As you can see in the file **initial_sensor_summary_analysis.ipynb** I dropped features based on if they did not add any information to the data which I believed to be useful. Even after this, I was only able to reduce the size of the dataset from 35 columns to 28 columns many of which I still did not understand.

This all changed after I found the documentation for the particular sensor used for the experiment online. The documentation was extremely helpful in explaining what each feature meant and the valid range of values. I used this information in the file **further_sensor_summary_analysis.ipynb** to help me reduce the size of the dataset from 28 columns down to 18 columns and I may potentially reduce this further in ...