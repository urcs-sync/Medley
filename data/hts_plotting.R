# Copyright 2015 University of Rochester
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License. 


library(plyr)
library(ggplot2)

scientific_10 <- function(x) {
  parse(text=gsub("1e\\+", "10^", scales::scientific_format()(x)))
}
tests<-c("g0i50r50_thread","g50i25r25_thread","g90i5r5_thread")
for (t in tests){
read.csv(paste("./hts_",t,".csv",sep=""),row.names=NULL)->montagedata

montagedata$ds<-as.factor(gsub("NoPersistMedleyLfHashTable<uint64_t>","Medley",montagedata$ds))
montagedata$ds<-as.factor(gsub("NoPersistTxnBoostingLfHashTable<uint64_t>","Boosted",montagedata$ds))
montagedata$ds<-as.factor(gsub("nbtxMontageLfHashTable<uint64_t>","txMontage",montagedata$ds))
montagedata$ds<-as.factor(gsub("NoPersistOneFileHashTable<uint64_t>","OneFile",montagedata$ds))
montagedata$ds<-as.factor(gsub("NoPersistPOneFileHashTable<uint64_t>","POneFile",montagedata$ds))


# d1<-subset(montagedata,ds=="Boosted")
d2<-subset(montagedata,ds=="Medley")
d3<-subset(montagedata,ds=="txMontage")
d4<-subset(montagedata,ds=="OneFile")
d5<-subset(montagedata,ds=="POneFile")
lkdata = rbind(d2,d3,d4,d5)
lkdata<-subset(lkdata,thread!=90)

ddply(.data=lkdata,.(ds,thread),mutate,ops= mean(ops))->lkdata
lindata = rbind(lkdata[,c("ds","thread","ops")])
lindata$ds <- factor(lindata$ds, levels=c("Medley", "txMontage", "OneFile", "POneFile", "Boosted"))

# Set up colors and shapes (invariant for all plots)
color_key = c("#FF69B4","#C11B14",
              "#12E1EA","#1245EA",
              "#660099")
              # "#FF8C00"
              # "#6F2534","#1BC40F","#5947ff",
              # "#F86945",
              # "#191970"
              # )
names(color_key) <- levels(lindata$ds)

shape_key = c(18,20,1,0,2,25,17,15,16,62,4)
names(shape_key) <- levels(lindata$ds)

line_key = c(1,4,1,4,1)
names(line_key) <- levels(lindata$ds)

# Benchmark-specific plot formatting
legend_pos=c(0.531,0.13)
if(t=="g0i50r50_thread")
  legend_pos=c(0.531,0.4)

y_name="Throughput (txn/s)"
y_range_down = 10000

# Generate the plots
linchart<-ggplot(data=lindata,
                  aes(x=thread,y=ops,color=ds, shape=ds, linetype=ds))+
  geom_line()+xlab("Threads")+ylab(y_name)+geom_point(size=3)+
  scale_shape_manual(values=shape_key[names(shape_key) %in% lindata$ds])+
  scale_linetype_manual(values=line_key[names(line_key) %in% lindata$ds])+
  theme_bw()+ guides(shape=guide_legend(title=NULL,nrow=2))+ 
  guides(color=guide_legend(title=NULL,nrow=2))+
  guides(linetype=guide_legend(title=NULL,nrow=2))+
  scale_color_manual(values=color_key[names(color_key) %in% lindata$ds])+
  scale_x_continuous(breaks=c(0,10,20,30,40,50,60,70,80),minor_breaks=c(-10))+
  scale_y_continuous(trans='log2',label=scientific_10,breaks=c(10000,100000,1000000,1000000,10000000,100000000),
                minor_breaks=c(2000,3000,4000,5000,6000,7000,8000,9000,20000,30000,40000,50000,60000,70000,80000,90000,200000,300000,400000,500000,600000,700000,800000,900000,2000000,3000000,4000000,5000000,6000000,7000000,8000000,9000000,20000000,30000000,40000000,50000000,60000000,70000000,80000000,90000000,200000000,300000000,400000000,500000000,600000000,700000000,800000000,900000000,2000000000))+
  coord_cartesian(xlim = c(-2, 80), ylim = c(y_range_down,20000000))+
  theme(plot.margin = unit(c(.2,0,-1.5,0), "cm"))+
  theme(legend.position=legend_pos,
    legend.background = element_blank(),
    legend.key = element_rect(colour = NA, fill = "transparent"))+
  theme(text = element_text(size = 12))+
  theme(axis.title.y = element_text(margin = margin(t = 0, r = 5, b = 0, l = 2)))+
  theme(axis.title.x = element_text(hjust =-0.18,vjust = 11,margin = margin(t = 15, r = 0, b = 15, l = 0)))

# Save all four plots to separate PDFs
ggsave(filename = paste("./hashtables_",t,".pdf",sep=""),linchart,width=4, height = 3, units = "in", dpi=300, title = paste("hashtables_",t,".pdf",sep=""))
}