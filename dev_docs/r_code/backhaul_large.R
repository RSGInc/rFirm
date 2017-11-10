##Backhauling and Consolidation:using large dummy dataset
x<-read.csv("Shipment_output.csv") ##input data
y<-read.csv("Carrier_output.csv")  ##input data
#z<-read.csv("BackHaul_output.csv") ##input data
#View(x)
#View(y)
#View(z)
myvars <- c("origNuma", "DestNuma", "volume","assignedCarrierID")
x1 <- x[myvars]
View(x1)
y1<-subset(y, carrierID=='1009149')
View(y1)

##for a specific carrier
x1c1<-subset(x1, assignedCarrierID=='1009149')  
View(x1c1)
write.csv(x1c1,"carrier1.csv")
carrier1<-read.csv("carrier1.csv")
carrier1$X<-NULL
carrier1$assignedCarrierID<-NULL #assignedCarrierID=1009149
carrier1$wklyvol<-(carrier1$volume/52)
#View(carrier1)
wklyshpm1<-subset(carrier1, select=c("origNuma","DestNuma","wklyvol"))
#View(wklyshpm1)
library(reshape2)
mx<-acast(wklyshpm1, origNuma ~ DestNuma, fun.aggregate = sum, fill=NA, drop=FALSE, value.var="wklyvol")
#View(mx)
mx[is.nan(mx)] = NA
coln_mx<-colnames(mx) #adding missing columns
coln<-seq.int(4570)
mx<-mx[,match(coln,coln_mx)]
colnames(mx)<-coln
rown_mx<-rownames(mx)   #adding missing rows
rown<-seq.int(4570)
mx<-mx[match(rown,rown_mx),]
rownames(mx)<-rown
#View(mx)
mx <- cbind(mx, Total = rowSums(mx,na.rm=TRUE))
#View(mx)
rs1 <- subset(mx, select = c("Total"))
View(rs1)
totshpm<-rs1
View(totshpm)
wklyshpm<-subset(mx, select=-c(Total))
View(wklyshpm)

anncap<-1723006350 #carrier capacity
wklycap<-anncap/52
#View(wklycap)
newdata<-subset(carrier1, select=c("origNuma","DestNuma"))
library(reshape2)
cx<-acast(newdata, origNuma ~ DestNuma)
#View(cx)
for(i in 1:nrow(cx)){
  for(j in 1:ncol(cx)){
    if(cx[i,j]==1){
      cx[i,j]=150612  # 33134738/220 , i.e. dividing by no.of shipments using this carrier's capacity for the week
    }
  }
}
#View(cx)
coln_cx<-colnames(cx) #adding missing columns
coln<-seq.int(4570)
cx<-cx[,match(coln,coln_cx)]
colnames(cx)<-coln
rown_cx<-rownames(cx)   #adding missing rows
rown<-seq.int(4570)
cx<-cx[match(rown,rown_cx),]
rownames(cx)<-rown
cx <- cbind(cx, Total = rowSums(cx,na.rm=TRUE))
rs2 <- subset(cx, select = c("Total"))
#View(rs2)
totcarrcap<-rs2
View(totcarrcap)
wklycarrcap<-subset(cx, select=-c(Total))
#View(wklycarrcap)

distance_mt<-read.csv("distance_matirx_meters.csv")
#View(distance_mt)
distance_mi<-distance_mt*0.000621371
distance_mi$X<-NULL
#View(distance_mi)
coln<-seq.int(4570)
colnames(distance_mi)<-coln
#View(distance_mi)

disttocc<-round(matrix(data=runif(457000, min=100, max=1000), nrow=4570, ncol=100),0) ##consolidation center location matrix, assuming 100 cons. centers
coln<-seq.int(100)
colnames(disttocc)<-coln
rown<-seq.int(4570)
rownames(disttocc)<-rown
#View(disttocc)
disttocc<-cbind(disttocc, rmin=apply(disttocc, 1, FUN=min))
#View(disttocc)
rowmin <- subset(disttocc, select = c("rmin"))
View(rowmin)
disttocc2<-subset(disttocc, select = -c(rmin)) #to drop rmin column in disttocc1
View(disttocc2)

asgn<-matrix(0, nrow=4570, ncol=100) #creating an empty assignement matrix
coln<-seq.int(100)
colnames(asgn)<-coln
rown<-seq.int(4570)
rownames(asgn)<-rown
assignedmat<-asgn
#View(assignedmat)

attach(disttocc2) ##determine nearest transfer centers to origin/destination zones
attach(rowmin)
attach(assignedmat)
assignedmat_index<-matrix(0, nrow=nrow(assignedmat), ncol=1) #list of assigned cells
colnames(assignedmat_index)<-c("1")
View(assignedmat_index)
for(i in 1:nrow(disttocc2)){ 
  #print(i)
  for(j in 1:ncol(disttocc2)){ 
    if(disttocc2[i,j]==rowmin[i,1])
    {
      assignedmat[i,j]=1  #assigns nearest consolidation center for each origin zone
      assignedmat_index[i]=j
    } 
  }
}
View(assignedmat)
View(assignedmat_index)

wklyshpmtocc<-matrix(0, nrow=4570, ncol=100) ##weekly shipment to consolidation center
coln<-seq.int(100)
colnames(wklyshpmtocc) <-coln
rown<-seq.int(4570)
rownames(wklyshpmtocc) <-rown
#View(wklyshpmtocc)

wklycarrcaptocc<-matrix(0, nrow=4570, ncol=100) ##weekly carrier capacity to consolidation center
coln<-seq.int(100)
colnames(wklycarrcaptocc) <-coln
rown<-seq.int(4570)
rownames(wklycarrcaptocc) <-rown
#View(wklycarrcaptocc)

attach(totshpm)
attach(wklycarrcap)
attach(wklyshpmtocc)
attach(wklycarrcaptocc)
attach(assignedmat)
for(i in 1:nrow(assignedmat)){   
  for(j in 1:ncol(assignedmat)){ 
    if(assignedmat[i,j]==1)
    {
      wklyshpmtocc[i,j]=totshpm[i,1]
      wklycarrcaptocc[i,j]=totcarrcap[i,1]
    } 
  }
}
View(wklyshpmtocc)
View(wklycarrcaptocc)

wklytrip<-ceiling(wklyshpm/wklycarrcap) ##creating origin to consolidation center weekly trip matrix
View(wklytrip)

wklytriptocc<-ceiling(wklyshpmtocc/wklycarrcaptocc) ##creating origin to consolidation center weekly trip matrix
View(wklytriptocc)
wklytriptocc[is.nan(wklytriptocc)]<-0
View(wklytriptocc)

wklyshpmcc2dc<-matrix(0, nrow=100, ncol=100)   ##creating consolidation center to distribution center weekly shpm matrix
coln<-seq.int(100)
colnames(wklyshpmcc2dc) <-coln
rown<-seq.int(100)
rownames(wklyshpmcc2dc) <-rown
#View(wklyshpmcc2dc) 

wklytripcc2dc<-matrix(0, nrow=100, ncol=100)   ##creating consolidation center to distribution center weekly trip matrix
coln<-seq.int(100)
colnames(wklytripcc2dc) <-coln
rown<-seq.int(100)
rownames(wklytripcc2dc) <-rown
#View(wklytripcc2dc) 

attach(assignedmat)
attach(wklyshpm)
attach(wklytrip)
attach(wklyshpmcc2dc)
for(i in 1:nrow(wklyshpmcc2dc)){   
  for(j in 1:ncol(wklyshpmcc2dc)){ 
    sum=0
    for (m in 1:nrow(wklyshpmtocc))
    {
      if (assignedmat_index[m]==i) {
        #print(m)
        for (n in 1:ncol(wklyshpmtocc)){
          if(assignedmat_index[n]==j){
            #sum = sum + wklyshpm[m,n]
            sum = sum + wklyshpmtocc[m,n]
          }
        }
      }
    }
    wklyshpmcc2dc[i,j] = sum
  }
}
View(wklyshpmcc2dc)

attach(assignedmat_index)  ##weekly trip matrix between transfer centers (cc and dc)
attach(wklytriptocc)
attach(wklytripcc2dc)
for(p in 1:nrow(wklytripcc2dc)){   
  for(q in 1:ncol(wklytripcc2dc)){ 
    sum2=0
    for(k in 1:nrow(wklytriptocc)){
      if (assignedmat_index[k]==p){
        for(l in 1:ncol(wklytriptocc)){ 
          if(assignedmat_index[l]==q){
            sum2=sum2+wklytriptocc[k,l]
          }
        }
      }
    } 
    wklytripcc2dc[p,q]=sum2
  }
}
View(wklytripcc2dc)
  
bkhl<-matrix(0, nrow=100, ncol=100)  #Backhaul trip matrix created
coln<-seq.int(100)
colnames(bkhl) <-coln
rown<-seq.int(100)
rownames(bkhl) <-rown
View(bkhl) 
empty<-matrix(0, nrow=100, ncol=100)   #Empty trip matrix created
coln<-seq.int(100)
colnames(empty) <-coln
rown<-seq.int(100)
rownames(empty) <-rown
View(empty)

attach(wklytripcc2dc)
attach(bkhl)
attach(empty)
for(i in 1:nrow(wklytripcc2dc)) {    
  for(j in 1:ncol(wklytripcc2dc)) {
    bkhl[i,j]<-min(wklytripcc2dc[i,j],wklytripcc2dc[j,i])
    empty[i,j]<-wklytripcc2dc[i,j]-wklytripcc2dc[j,i]
    if(empty[i,j]<0)
    {
      empty[j,i]=abs(empty[i,j])
      empty[i,j]=0
    } 
    else
    {
      empty[i,j]=empty[i,j]
    }
    wklytripcc2dc[j,i]=wklytripcc2dc[j,i]-bkhl[i,j]  ##updates the forward trip matrix
    if(abs(empty[i,j])!=0)        ##checks for empty trip availability
    {
      p<-ceiling((wklytripcc2dc[j+1,i])*0.5) ##50% of the trips available for backhaul
      if(p>0)
      {
        y=min(p,abs(empty[i,j])) ##variable representing the minimum of available backhaul trips and empty trips
        bkhl[i,j]=bkhl[i,j]+y    ##updating backhaul matrix
        wklytripcc2dc[j+1,i]=wklytripcc2dc[j+1,i]-y  ##updating weekly forward haul trip matrix
        empty[i,j]=abs(empty[i,j])-y ##updating empty matrix
      }
      else 
      {
        wklytripcc2dc[j+1,i]=wklytripcc2dc[j+1,i]
      }
    }  
    #else break  
    j=j+1
  }
  i=i+1
}
View(wklytripcc2dc)
View(bkhl)
View(empty)
