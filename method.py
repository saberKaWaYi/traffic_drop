from array import array
from collections import deque

class method:

    def __init__(self):
        ############################################################
        self.time=None
        self.data_list_5_days=array('q')
        self.flag=False
        self.exist_iter=0
        self.exist_days=0
        self.mutation=None
        self.alert=None
        ############################################################
        ############################################################
        self.signal_period_level=None
        self.signal_period=None
        ############################################################
        ############################################################
        self.median=array('q')
        self.variation_diff_median=array('q',[0])
        self.median_1_day_deque_max=deque()
        self.median_1_day_deque_min=deque()
        self.history_mutations=array('q')
        self.history_alerts=array('q')
        ############################################################
        ############################################################
        self.count=0
        self.count_5_days=0
        self.count2_5_days=0
        self.count_1_day=0
        self.count2_1_day=0
        self.count_3_hours=0
        self.count2_3_hours=0
        self.last_alert_time=None
        ############################################################
        ############################################################
        self.small_window_len=30
        self.low_limit=150*1000*1000/8
        ############################################################

    def get_std(self,mode):
        if mode==1:
            return max((self.count2_3_hours-self.count_3_hours**2/180)/179,0)**0.5
        elif mode==2:
            return max((self.count2_1_day-self.count_1_day**2/1440)/1439,0)**0.5
        elif mode==3:
            return max((self.count2_5_days-self.count_5_days**2/7200)/7199,0)**0.5
    
    def get_periodic_level(self):
        self.signal_period=1440
        average=sum(self.median[-1440*2:])/2880
        covariance_sum=0
        for _ in range(1400):
            covariance_sum+=(self.median[-1440*2+_]-average)*(self.median[-1440+_]-average)
        variance_sum=0
        for _ in range(2880):
            variance_sum+=(self.median[-_]-average)**2
        if variance_sum==0:
            self.signal_period_level=1
        else:
            covariance_sum/=1440;variance_sum/=2880
            result=round(covariance_sum/variance_sum,2)
            self.signal_period_level=result

    def refresh(self,data,time_=None):
        if time_:
            self.time=time_
        self.data_list_5_days.append(data)
        ############################################################
        # 可以采用AVL树优化
        # 没有必要
        small_window=list(self.data_list_5_days[-self.small_window_len:])
        small_window.sort()
        if len(small_window)%2:
            temp=small_window[len(small_window)//2]
        else:
            temp=(small_window[len(small_window)//2-1]+small_window[len(small_window)//2])//2
        ############################################################
        self.median.append(temp)
        self.count+=temp
        if len(self.median)>=2:
            ############################################################
            # 可以用单调栈优化
            # 没有必要
            x=max(self.median[-6:-1]);y=min(self.median[-6:-1])
            ############################################################
            if temp>x:
                self.variation_diff_median.append(temp-y)
            elif temp<y:
                self.variation_diff_median.append(temp-x)
            else:
                if x-temp>temp-y:
                    self.variation_diff_median.append(temp-x)
                else:
                    self.variation_diff_median.append(temp-y)
        self.count_5_days+=self.variation_diff_median[-1]
        self.count2_5_days+=self.variation_diff_median[-1]**2
        self.count_1_day+=self.variation_diff_median[-1]
        self.count2_1_day+=self.variation_diff_median[-1]**2
        self.count_3_hours+=self.variation_diff_median[-1]
        self.count2_3_hours+=self.variation_diff_median[-1]**2
        while self.median_1_day_deque_max and temp>self.median_1_day_deque_max[-1]:
            self.median_1_day_deque_max.pop()
        self.median_1_day_deque_max.append(temp)
        while self.median_1_day_deque_min and temp<self.median_1_day_deque_min[-1]:
            self.median_1_day_deque_min.pop()
        self.median_1_day_deque_min.append(temp)
        self.exist_iter+=1
        if time_:
            return
        self.history_mutations.append(0)
        self.history_alerts.append(0)

    def add_data(self,add_data_num):
        for i in range(add_data_num):
            if not self.flag or self.signal_period_level<0.3:
                data=self.median[-1]
            else:
                data=0;count=0
                for j in range(0,7200,self.signal_period):
                    data+=self.median[-j];count+=1
                data//=count
            self.refresh(data)

    def pop_front_data(self,add_data_nums):
        len_=len(self.data_list_5_days)-self.small_window_len
        if len_>0:
            self.data_list_5_days=self.data_list_5_days[len_:]
        len_=len(self.median)-7200
        if len_>0:
            removed_items=self.median[:len_]
            self.median=self.median[len_:]
            for removed_item in removed_items:
                self.count-=removed_item
        len_=len(self.variation_diff_median)-7200
        if len_>0:
            removed_items=self.variation_diff_median[:len_]
            self.variation_diff_median=self.variation_diff_median[len_:]
            for removed_item in removed_items:
                self.count_5_days-=removed_item;self.count2_5_days-=removed_item**2
        len_=len(self.median)-1440
        if len_>0:
            nums=min(len_,add_data_nums)
            for i in range(-nums,0):
                temp=self.variation_diff_median[-1440+i]
                self.count_1_day-=temp;self.count2_1_day-=temp**2
                temp=self.median[-1440+i]
                if self.median_1_day_deque_max and temp==self.median_1_day_deque_max[0]:
                    self.median_1_day_deque_max.popleft()
                if self.median_1_day_deque_min and temp==self.median_1_day_deque_min[0]:
                    self.median_1_day_deque_min.popleft()
        len_=len(self.median)-180
        if len_>0:
            nums=min(len_,add_data_nums)
            for i in range(-nums,0):
                temp=self.variation_diff_median[-180+i]
                self.count_3_hours-=temp;self.count2_3_hours-=temp**2
        len_=len(self.history_mutations)-5
        if len_>0:
            self.history_mutations=self.history_mutations[len_:]
        len_=len(self.history_alerts)-1
        if len_>0:
            self.history_alerts=self.history_alerts[len_:]

    def get_is_mutation(self):
        max_next_value=0
        for i in range(14):
            max_next_value=max(max_next_value,self.median[-15+i])
            if self.median[-1]>=(self.median[-15+i]-self.low_limit):
                self.mutation=0
                return
        diff=max_next_value-self.median[-1]
        if diff<=(self.median_1_day_deque_max[0]-self.median_1_day_deque_min[0])*0.1:
            self.mutation=0
            return
        if diff<=self.count*0.1/7200:
            self.mutation=0
            return
        std=self.get_std(1)
        if not std or (self.variation_diff_median[-1]-self.count_3_hours/180)/std>=-3:
            self.mutation=0
            return
        std=self.get_std(2)
        if not std or (self.variation_diff_median[-1]-self.count_1_day/1440)/std>=-3:
            self.mutation=0
            return
        std=self.get_std(3)
        if not std or (self.variation_diff_median[-1]-self.count_5_days/7200)/std>=-3:
            self.mutation=0
            return
        if self.signal_period_level>=0.3:
            lt=[]
            for i in range(7199-self.signal_period,-1,-self.signal_period):
                temp=float('inf')
                for j in range(-14,15):
                    if self.median[max(i+j,0)]<temp:
                        temp=self.median[max(i+j,0)]
                lt.append(temp)
            if self.median[-1]>=sum(lt)/len(lt)*0.9:
                self.mutation=0
                return
        ####有待开发
        self.mutation=max_next_value

    def get_is_alert(self):
        if self.last_alert_time and self.time-self.last_alert_time<=30*60:
            self.alert=0
            return
        if self.history_mutations[-5]==0:
            self.alert=0
            return
        diff=self.history_mutations[-5]-self.median[-5]
        for i in range(-4,0):
            if self.history_mutations[-5]-self.median[i]<=diff*0.8:
                self.alert=0
                return
        self.last_alert_time=self.time
        self.alert=1

    def push_back_data(self,info):
        time=int(info[0]);data=int(float(info[1]))
        if self.time and time<self.time+60:
            return
        if self.time and (time-self.time)//60-1:
            self.add_data((time-self.time)//60-1)
        add_data_nums=0 if self.time==None else (time-self.time)//60-1
        self.refresh(data,time)
        self.pop_front_data(add_data_nums+1)
        if self.exist_iter>=7200:
            self.flag=True
        if self.flag and self.exist_iter//1440-self.exist_days:
            self.exist_days=self.exist_iter//1440
            self.get_periodic_level()
        if not self.flag:
            self.mutation=0;self.history_mutations.append(0)
            self.alert=0;self.history_alerts.append(0)
        else:
            self.get_is_mutation()
            self.history_mutations.pop(0);self.history_mutations.append(self.mutation)
            self.get_is_alert()
            self.history_alerts.pop(0);self.history_alerts.append(self.alert)