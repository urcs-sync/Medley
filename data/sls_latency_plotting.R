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
# library(ggpubr)

read.csv("./sls_latency.csv",row.names=NULL)->montagedata

montagedata$ds<-as.factor(gsub("NoPersistFraserSkipList<uint64_t>","DRAM",montagedata$ds))
montagedata$ds<-as.factor(gsub("NoPersistNVMFraserSkipList<uint64_t>","NVM",montagedata$ds))
montagedata$ds<-as.factor(gsub("NoPersistMedleyFraserSkipList<uint64_t>","Medley",montagedata$ds))
montagedata$ds<-as.factor(gsub("NoPersisttxMontageFraserSkipList<uint64_t>","Montage-T",montagedata$ds))
montagedata$ds<-as.factor(gsub("nbtxMontageFraserSkipList<uint64_t>","Montage-P",montagedata$ds))

# tests<-c("g0p0i50rm50","g50p0i25rm25","g90p0i5rm5")
# for (t in tests){
# d<-subset(montagedata,grepl(t,test))
read_most<-subset(montagedata,grepl("g90p0i5rm5",test))
read_most$rwratio<-"18:1:1"

read_write<-subset(montagedata,grepl("g50p0i25rm25",test))
read_write$rwratio<-"2:1:1"

write_only<-subset(montagedata,grepl("g0p0i50rm50",test))
write_only$rwratio<-"0:1:1"

d<-rbind(read_most,read_write,write_only)

d1<-subset(d,ds=="DRAM")
d1$persist<-"DRAM"
d1$mech<-"Original"
d2<-subset(d,ds=="NVM")
d2$persist<-"NVM (Transient)"
d2$mech<-"Original"

d3<-subset(d,ds=="Medley" & grepl("TxnMapChurnTest<uint64_t:None>",test))
d3$persist<-"DRAM"
d3$mech<-"TxOff"

d4<-subset(d,ds=="Medley" & grepl("TxnMapChurnTest<uint64_t:NBTC>",test))
d4$persist<-"DRAM"
d4$mech<-"TxOn"

d5<-subset(d,ds=="Montage-T" & grepl("TxnMapChurnTest<uint64_t:None>",test))
d5$persist<-"NVM (Transient)"
d5$mech<-"TxOff"

d6<-subset(d,ds=="Montage-T" & grepl("TxnMapChurnTest<uint64_t:NBTC>",test))
d6$persist<-"NVM (Transient)"
d6$mech<-"TxOn"

d7<-subset(d,ds=="Montage-P" & grepl("TxnMapChurnTest<uint64_t:None>",test))
d7$persist<-"Persistent"
d7$mech<-"TxOff"

d8<-subset(d,ds=="Montage-P" & grepl("TxnMapChurnTest<uint64_t:NBTC>",test))
d8$persist<-"Persistent"
d8$mech<-"TxOn"

total <- rbind(d1,d2,d3,d4,d5,d6,d7,d8)
total<-aggregate(ops ~ persist+mech+rwratio, data=total,FUN = mean)
ddply(.data=total,.(persist,mech,rwratio),mutate,latency = 1000000000/mean(ops))->total
lindata = rbind(total[,c("persist","rwratio","mech","latency")])
lindata$mech <- factor(lindata$mech, levels=c(
  "Original", "TxOff", "TxOn"))

lindata$rwratio <- factor(lindata$rwratio,levels = c("0:1:1","2:1:1","18:1:1"))

color_key = c(
  "#00bb38",
  "#619dff",
  "#f9766c"
)
names(color_key) <- levels(lindata$mech)

# Benchmark-specific plot formatting
legend_pos=c(0.55,0.96)
y_name="Latency (ns/txn)"

tests=c("DRAM","NVM (Transient)","Persistent")
for(t in tests){
to_plot <- subset(lindata, persist == t)
# lindata_nvm <- subset(lindata, persist != "DRAM")

# Generate the dram plot
linchart<-ggplot(data=to_plot,
                  aes(x=rwratio,y=latency,fill=mech))+
  geom_bar(stat='identity',position = "dodge")+
  geom_text(aes(label=round(latency,0)), position=position_dodge(width=0.9),vjust=-0.5, size=4)+
  scale_fill_manual(values=color_key[names(color_key) %in% to_plot$mech])+
  xlab("")+ylab(y_name)+
  coord_cartesian(ylim = c(0,1000))+
  guides(fill=guide_legend(title=NULL,nrow=1))+
  theme(plot.margin = unit(c(.2,0,-1.5,0), "cm"))+
  theme(legend.position=legend_pos,
    legend.background = element_blank(),
    legend.key = element_rect(colour = NA, fill = "transparent"),
    legend.text = element_text(margin = margin(r = 0.3, unit = 'cm')))+
  theme(text = element_text(size = 12))+
  theme(axis.title.y = element_text(margin = margin(t = 0, r = 5, b = 0, l = 2)))+
  theme(axis.title.x = element_text(hjust =-0.18,vjust = 11,margin = margin(t = 15, r = 0, b = 15, l = 0)))

# Save all four plots to separate PDFs
label<-"dram"
if(t=="NVM (Transient)"){
  label<-"nvm"
} else if (t=="Persistent") {
  label<-"p"
}
ggsave(filename = paste("./sls_latency_",label,".pdf",sep=""),linchart,width=4, height = 3, units = "in", dpi=300, title = paste("./sls_latency_",label,".pdf",sep=""))

# # Generate the nvm plot
# linchart1<-ggplot(data=subset(lindata_nvm,rwratio=="0:1:1"),
#                   aes(x=persist,persist,y=latency,fill=mech))+
#   geom_bar(stat='identity',position = "dodge")+
#   geom_text(aes(label=round(latency,0)), position=position_dodge(width=0.9),vjust=-0.5, size=4)+
#   scale_fill_manual(values=color_key[names(color_key) %in% lindata$mech])+
#   xlab("")+ylab(y_name)+
#   coord_cartesian(ylim = c(0,1000))+
#   guides(fill=guide_legend(title=NULL,nrow=1))+
#   theme(plot.margin = unit(c(.2,0,-1.5,0), "cm"))+
#   theme(legend.position="none",
#     legend.background = element_blank(),
#     legend.key = element_rect(colour = NA, fill = "transparent"),
#     legend.text = element_text(margin = margin(r = 0.3, unit = 'cm')))+
#   theme(text = element_text(size = 12))+
#   theme(axis.title.y = element_text(margin = margin(t = 0, r = -2, b = 0, l = 2)))+
#   theme(axis.title.x = element_text(hjust =-0.18,vjust = 11,margin = margin(t = 15, r = 0, b = 15, l = 0)))

# linchart2<-ggplot(data=subset(lindata_nvm,rwratio=="2:1:1"),
#                   aes(x=persist,persist,y=latency,fill=mech))+
#   geom_bar(stat='identity',position = "dodge")+
#   geom_text(aes(label=round(latency,0)), position=position_dodge(width=0.9),vjust=-0.5, size=4)+
#   scale_fill_manual(values=color_key[names(color_key) %in% lindata$mech])+
#   xlab("")+ylab("")+
#   coord_cartesian(ylim = c(0,1000))+
#   guides(fill=guide_legend(title=NULL,nrow=1))+
#   theme(plot.margin = unit(c(.2,0,-1.5,0), "cm"))+
#   theme(legend.position=c(0.5,0.96),
#     legend.background = element_blank(),
#     legend.key = element_rect(colour = NA, fill = "transparent"),
#     legend.text = element_text(margin = margin(r = 0.1, unit = 'cm')))+
#   theme(text = element_text(size = 12))+
#   theme(axis.text.y = element_blank(),
#         axis.ticks.y=element_blank())+
#   theme(axis.title.y = element_text(margin = margin(t = 0, r = 5, b = 0, l = 2)))+
#   theme(axis.title.x = element_text(hjust =-0.18,vjust = 11,margin = margin(t = 15, r = 0, b = 15, l = 0)))

# linchart3<-ggplot(data=subset(lindata_nvm,rwratio=="18:1:1"),
#                   aes(x=persist,persist,y=latency,fill=mech))+
#   geom_bar(stat='identity',position = "dodge")+
#   geom_text(aes(label=round(latency,0)), position=position_dodge(width=0.9),vjust=-0.5, size=4)+
#   scale_fill_manual(values=color_key[names(color_key) %in% lindata$mech])+
#   xlab("")+ylab("")+
#   coord_cartesian(ylim = c(0,1000))+
#   guides(fill=guide_legend(title=NULL,nrow=1))+
#   theme(plot.margin = unit(c(.2,0,-1.5,0), "cm"))+
#   theme(legend.position="none",
#     legend.background = element_blank(),
#     legend.key = element_rect(colour = NA, fill = "transparent"),
#     legend.text = element_text(margin = margin(r = 0.3, unit = 'cm')))+
#   theme(text = element_text(size = 12))+
#   theme(axis.text.y = element_blank(),
#         axis.ticks.y=element_blank())+
#   theme(axis.title.y = element_text(margin = margin(t = 0, r = 5, b = 0, l = 2)))+
#   theme(axis.title.x = element_text(hjust =-0.18,vjust = 11,margin = margin(t = 15, r = 0, b = 15, l = 0)))

# # figure <- ggarrange(linchart1, linchart2, linchart3,
# #                     labels = c("get:insert:remove=0:1:1", "2:1:1", "18:1:1"),
# #                     label.x=c(-0.2,0.4,0.4), label.y=0.02,
# #                     font.label=list(face="plain",size=12),
# #                     ncol = 3, nrow = 1,
# #                     common.legend = TRUE)+
# #   theme(legend.position=legend_pos)+
# #   theme(plot.margin = margin(0,0,0.4,0, "cm")) 
# # ggexport(figure, filename = "sls_latency_nvm.pdf",width=8, height = 3, units = "in", dpi=300, title = "sls_latency_nvm.pdf")
# # Save all four plots to separate PDFs
# ggsave(filename = "./sls_latency_nvm1.pdf",linchart1,width=3.25, height = 3, units = "in", dpi=300, title = "sls_latency_nvm1.pdf")
# ggsave(filename = "./sls_latency_nvm2.pdf",linchart2,width=3, height = 3, units = "in", dpi=300, title = "sls_latency_nvm2.pdf")
# ggsave(filename = "./sls_latency_nvm3.pdf",linchart3,width=3, height = 3, units = "in", dpi=300, title = "sls_latency_nvm3.pdf")

}