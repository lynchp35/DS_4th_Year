library("TTR")
library("fpp")
library("forecast")
library("zoo")
library("ggplot2")
library("readxl")

#setwd("~/GitHub/DS_4th_Year/CA4024/tutorials/code")

# Question 1 #####################################

acme = ts(c(6.17, 6.64, 7.11, 6.58, 8.87, 9.34, 8.81, 13.28, 
            14.57, 17.04, 14.51, 17.98, 20.27, 20.74, 19.21, 18.68),
          start=2010, frequency = 4)

# Plot time series data
plot(acme,ylab= "Sales/M€")
legend(2010, 15, legend=c("Orginal", "WMA4", "LM"),
       col=c("black","red", "blue"), lty=1)

# Moving Averages (2X4pt) 
acme_ma4 = ma(acme, order = 4, centre = T)
lines(acme_ma4, type="o", col="red")

# Least Squares
acme_lm = lm(acme[1:16] ~ time(acme))
lines(ts(acme_lm$fitted.values, start=2010, frequency = 4), col="blue")

# There is an upwards trend, unclear if there is a seasonal aspect. 
# Maybe a peak in Q2 and a drop in Q3. 

# Question 2 #####################################

eir = ts(c(0.45, 1.4, 0.35, -0.7, 0.95, -0.1, 0.85, 0.8, 0.45, 
           0.4, -0.65, -1.7, -2.05, -1.1, -1.15, -0.2),
          start=2018, frequency = 4)

# Plot time series data
plot(eir,ylab= "Profits /€")
legend(2018, -1, legend=c("Orginal", "WMA4", "LM"),
       col=c("black","red", "blue"), lty=1)

# Moving Averages (2X4pt) 
eir_ma4 = ma(eir, order = 4, centre = T)
lines(eir_ma4, type="o", col="red")

# Least Squares
eir_lm = lm(eir[1:16] ~ time(eir))
lines(ts(eir_lm$fitted.values, start=2018, frequency = 4), col="blue")

# There is an downwards trend up until 2021, on a longer time scale 
# it maybe a cyclical pattern. Unclear if there is a seasonal aspect, 
# maybe in the years 2018-2019 a peak in Q1 and a drop in Q3. 

# Question 3 #####################################

vix <- read_excel("../extra/02week/Vix0708.xlsx")
ts(vix)
