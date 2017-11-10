##LTL Carrier Backhauling and Consolidation:test code
wklyshpm<-round(matrix(data=runif(25, min=50, max=1000), nrow=5, ncol=5), 2) ##weekly volume matrix and rounding to two decimals
diag(wklyshpm)<-0
#View(wklyshpm)
colnames(wklyshpm)<-c("1","2","3","4","5")
rownames(wklyshpm)<-c("1","2","3","4","5")
#View(wklyshpm)
rs<-rowSums(wklyshpm)
#View(rs)
write.csv(rs,"rs.csv")
totshpm<-read.csv("rs.csv")
#View(totshpm)
totshpm$X<-NULL
#View(totshpm)
colnames(totshpm)[colnames(totshpm) == 'x'] <- 'rs'
View(totshpm)

wklycarrcap<-round(matrix(data=runif(25, min=0, max=1000),nrow=5, ncol=5), 2) ##assigned carrier's capacity for weekly time period
diag(wklycarrcap)<-100000
#View(wklycarrcap)
colnames(wklycarrcap)<-c("1","2","3","4","5")
rownames(wklycarrcap)<-c("1","2","3","4","5")
#View(wklycarrcap)
rs2<-rowSums(wklycarrcap)
#View(rs2)
write.csv(rs2,"rs2.csv")
totcarrcap<-read.csv("rs2.csv")
#View(totcarrcap)
totcarrcap$X<-NULL
#View(totcarrcap)
colnames(totcarrcap)[colnames(totcarrcap) == 'x'] <- 'rs2'
View(totcarrcap)

x<-matrix(data=runif(n*k, min=50, max=3000), nrow=5, ncol=5) ##random distance matrix; can be substituted with actual distance between zones or state centroids
diag(x)<-0 ##diagonal of distance matrix is set to zero
colnames(x)<-c("1","2","3","4","5")
rownames(x)<-c("1","2","3","4","5")
x<-x+t(x) ##makes symmetric matix
dist_zone<-x ##zone matrix #distance matrix
View(dist_zone)

disttocc<-round(matrix(data=runif(50, min=10, max=1000), nrow=5, ncol=10),0) ##consolidation center location matrix
rownames(disttocc)<-c("1","2","3","4","5")
colnames(disttocc)<-c("1","2","3","4","5","6","7","8","9","10")
#View(disttocc)
rmin<-apply(disttocc, 1, FUN=min)
#View(rmin)
write.csv(rmin,"rmin.csv")
z<-read.csv("rmin.csv")
#View(z)
z$X<-NULL
#View(z)
colnames(z)[colnames(z) == 'x'] <- 'rm'
View(z)

y<-matrix(0, nrow=5, ncol=10)
rownames(y)<-c("1","2","3","4","5")
colnames(y)<-c("1","2","3","4","5","6","7","8","9","10")
assignedmat<-y
View(assignedmat)

attach(disttocc) ##determine nearest transfer centers to origin/destination zones
attach(z)
attach(assignedmat)
for(i in 1:nrow(disttocc)){
  for(j in 1:ncol(disttocc)){
     if(disttocc[i,j]==z[i,1])
    {
    assignedmat[i,j]=1
    }
  }
}
View(assignedmat)

wklyshpmtocc<-matrix(0, nrow=5, ncol=10) ##weekly shipment to consolidation center
#View(wklyshpmtocc)
rownames(wklyshpmtocc)<-c("1","2","3","4","5")
colnames(wklyshpmtocc)<-c("1","2","3","4","5","6","7","8","9","10")
View(wklyshpmtocc)

wklycarrcaptocc<-matrix(0, nrow=5, ncol=10) ##weekly carrier capacity to consolidation center
#View(wklycarrcaptocc)
rownames(wklycarrcaptocc)<-c("1","2","3","4","5")
colnames(wklycarrcaptocc)<-c("1","2","3","4","5","6","7","8","9","10")
View(wklycarrcaptocc)

attach(totshpm)
attach(totcarrcap)
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
#View(wklytriptocc)
wklytriptocc[is.nan(wklytriptocc)]<-0
View(wklytriptocc)

wklyshpmcc2dc<-matrix(0, nrow=10, ncol=10)   ##creating consolidation center to distribution center weekly shpm matrix
rownames(wklyshpmcc2dc)<-c("1","2","3","4","5","6","7","8","9","10")
colnames(wklyshpmcc2dc)<-c("1","2","3","4","5","6","7","8","9","10")
View(wklyshpmcc2dc)

wklytripcc2dc<-matrix(0, nrow=10, ncol=10)   ##creating consolidation center to distribution center weekly trip matrix
rownames(wklytripcc2dc)<-c("1","2","3","4","5","6","7","8","9","10")
colnames(wklytripcc2dc)<-c("1","2","3","4","5","6","7","8","9","10")
View(wklytripcc2dc)

attach(assignedmat)
attach(wklyshpm)
attach(wklytrip)
attach(wklyshpmcc2dc)
assignedmat_index<-matrix(0, nrow=nrow(assignedmat), ncol=1)
View(assignedmat_index)
colnames(assignedmat_index)<-c("1")
View(assignedmat)
View(assignedmat_index)

for ( i in 1:nrow(assignedmat)){
  for (j in 1:ncol(assignedmat)){
    if (assignedmat[i,j]==1){
      assignedmat_index[i] = j
      break
    }
  }
}
View(assignedmat_index)
for(i in 1:nrow(wklyshpmcc2dc)){
  for(j in 1:ncol(wklyshpmcc2dc)){
    sum=0
    for (m in 1:nrow(wklyshpmtocc))
    {
      if (assignedmat_index[m]==i) {
        #print(m)
        for (n in 1:ncol(wklyshpmtocc)){
          if(assignedmat_index[n]==j){
            sum = sum + wklyshpmtocc[m,n]
          }
        }
      }
    }
    wklyshpmcc2dc[i,j] = sum
  }
}
View(wklyshpmcc2dc)
attach(assignedmat)  ##weekly trip matrix between transfer centers (cc and dc)
attach(wklytrip)
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

bkhl<-matrix(0, nrow=10, ncol=10, dimnames = list(c("1","2","3","4","5","6","7","8","9","10"), c("1","2","3","4","5","6","7","8","9","10")))   #Backhaul trip matrix created
empty<-matrix(0, nrow=10, ncol=10, dimnames = list(c("1","2","3","4","5","6","7","8","9","10"), c("1","2","3","4","5","6","7","8","9","10")))   #Empty trip matrix created
#View(bkhl)
#View(empty)
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
    j=j+1
  }
  i=i+1
}
View(wklytripcc2dc)
View(bkhl)
View(empty)
