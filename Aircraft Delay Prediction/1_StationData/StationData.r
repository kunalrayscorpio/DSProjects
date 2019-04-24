# Reading StationData

stationdata<- read.table("AllStationsData_PHD.txt", sep="|",header = T, dec = ".")

str(stationdata)

library(sp)
library(rgeos)

stationdata$WeatherStationID<-as.factor(stationdata$WeatherStationID)
stationdata$GroundHeight<-as.numeric(stationdata$GroundHeight)
stationdata$StationHeight<-as.numeric(stationdata$StationHeight)
stationdata$BarometerHeight<-as.numeric(stationdata$BarometerHeight)
stationdata$TimeZone<-as.numeric(stationdata$TimeZone)

sp.stndata <- stationdata
coordinates(sp.stndata) <- ~GroundHeight+StationHeight+BarometerHeight+Latitude+Longitude+TimeZone

class(sp.stndata)

d <- gDistance(sp.stndata, byid=T)

# Finding closest match

min.d <- apply(d, 1, function(x) order(x, decreasing=F)[2])

newdata1 <- cbind(stationdata, stationdata[min.d,], apply(d, 1, function(x) sort(x,decreasing=F)[2]))

colnames(newdata1) <- c(colnames(stationdata), 'ClosestWS', 'n.AirportID', 'n.GroundHeight', 
                       'n.StationHeight', 'n.BarometerHeight', 'n.Latitude', 'n.Longitude',
                       'n.TimeZone','distance')

rm(min.d) # Remove temp variable

# Finding 2nd closest match

min.d <- apply(d, 1, function(x) order(x, decreasing=F)[3])

newdata2 <- cbind(stationdata, stationdata[min.d,], apply(d, 1, function(x) sort(x,decreasing=F)[3]))

colnames(newdata2) <- c(colnames(stationdata), 'Closest2ndWS', 'n.AirportID', 'n.GroundHeight', 
                        'n.StationHeight', 'n.BarometerHeight', 'n.Latitude', 'n.Longitude',
                        'n.TimeZone','distance')

rm(min.d) # Remove temp variable

# Finding 3rd closest match

min.d <- apply(d, 1, function(x) order(x, decreasing=F)[4])

newdata3 <- cbind(stationdata, stationdata[min.d,], apply(d, 1, function(x) sort(x,decreasing=F)[4]))

colnames(newdata3) <- c(colnames(stationdata), 'Closest3rdWS', 'n.AirportID', 'n.GroundHeight', 
                        'n.StationHeight', 'n.BarometerHeight', 'n.Latitude', 'n.Longitude',
                        'n.TimeZone','distance')

rm(min.d) # Remove temp variable

# Finding 4th closest match

min.d <- apply(d, 1, function(x) order(x, decreasing=F)[5])

newdata4 <- cbind(stationdata, stationdata[min.d,], apply(d, 1, function(x) sort(x,decreasing=F)[5]))

colnames(newdata4) <- c(colnames(stationdata), 'Closest4thWS', 'n.AirportID', 'n.GroundHeight', 
                        'n.StationHeight', 'n.BarometerHeight', 'n.Latitude', 'n.Longitude',
                        'n.TimeZone','distance')

rm(min.d) # Remove temp variable

# Finding 5th closest match

min.d <- apply(d, 1, function(x) order(x, decreasing=F)[6])

newdata5 <- cbind(stationdata, stationdata[min.d,], apply(d, 1, function(x) sort(x,decreasing=F)[6]))

colnames(newdata5) <- c(colnames(stationdata), 'Closest5thWS', 'n.AirportID', 'n.GroundHeight', 
                        'n.StationHeight', 'n.BarometerHeight', 'n.Latitude', 'n.Longitude',
                        'n.TimeZone','distance')

# Deriving 5 closest weather stations in single data frame

closestation<-merge(stationdata[,1:2],newdata1[,c('WeatherStationID','ClosestWS')],
                    by.x="WeatherStationID",by.y="WeatherStationID")

closestation<-merge(closestation,newdata2[,c('WeatherStationID','Closest2ndWS')],
                    by.x="WeatherStationID",by.y="WeatherStationID")

closestation<-merge(closestation,newdata3[,c('WeatherStationID','Closest3rdWS')],
                    by.x="WeatherStationID",by.y="WeatherStationID")

closestation<-merge(closestation,newdata4[,c('WeatherStationID','Closest4thWS')],
                    by.x="WeatherStationID",by.y="WeatherStationID")

closestation<-merge(closestation,newdata5[,c('WeatherStationID','Closest5thWS')],
                    by.x="WeatherStationID",by.y="WeatherStationID")

rm(d,newdata1,newdata2,newdata3,newdata4,newdata5,stationdata,min.d,sp.stndata) # Remove unrequired

saveRDS(closestation, file = "closestation.rds") # Saving externally