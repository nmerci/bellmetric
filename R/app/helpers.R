f1<-function(threshold, y, pred_y)
  {
  pred<-as.numeric(pred_y>=threshold)
  a<-table(pred,y)
  pr<-a[2,2]/((a[2,2]+a[2,1]))
  rec<-a[2,2]/(a[2,2]+a[1,2])
  2*pr*rec/(pr+rec)
}

roc<-function(threshold,y,pred_y)
{
  library(ROCR)
  pred<-as.numeric(pred_y>=threshold)
  pr<-prediction(pred,y)
  ROC.perf <- performance(pr, "tpr", "fpr");
  plot (ROC.perf)
  abline(0,1)
}
