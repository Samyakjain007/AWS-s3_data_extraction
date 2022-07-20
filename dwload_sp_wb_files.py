from curses import echo
import os,boto3
import ujson,time
import pandas as pd
import argparse
from datetime import datetime
import os
ACCESS_KEY=''
SECRET_KEY=''
BUCKET_SENSORPROCLOGS=''
BUCKET_WEBENTRIES=''

# fileList=[]
x=os.path.abspath(os.path.dirname('pwd'))
print(x)
os.mkdir('')

parser = argparse.ArgumentParser(description='Patch Id')
parser.add_argument('-p',dest='patchId', type=str, help='Please enter patch id')
args = parser.parse_args()
pid = args.patchId
os.mkdir('/{}'.format(pid))
os.mkdir('/{}/SensorProcFiles/'.format(pid))
os.mkdir('/{}/WebUiEntries/'.format(pid))
LOCAL_PATH_SENSORPROC=x+'//{}//'.format(pid)
LOCAL_PATH_WEBENTRIES=x+'//{}//'.format(pid)

pathfile1 = '/{}'.format(pid)
pathfile2 = '/{}/.txt'.format(pid)


class S3Pull():

    # parser = argparse.ArgumentParser(description='Patch Id')
    # parser.add_argument('-p',dest='patchId', type=str, help='Please enter patch id')
    # args = parser.parse_args()
    # pid = args.patchId

    # pathfile1 = 'LSSERVER/{}'.format(pid)
    # pathfile2 = 'backup-completed-aborted/{}/Webui_entries.txt'.format(pid)

    
    def __init__(self,ACCESS_KEY,SECRET_KEY):
        sessions=boto3.Session(aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY,region_name='ap-south-1')
        self.__s3=sessions.resource('s3')
        self.__s3resbucket1=self.__s3.Bucket(BUCKET_SENSORPROCLOGS)
        self.__s3resbucket2=self.__s3.Bucket(BUCKET_WEBENTRIES)
    
    def get_s3_filelist(self,patchId):
        # pass
        print('Fetching the files... .. .\n')
        sensorproclogfiles=[]
        webentriesfiles=[]

        for myBucket in self.__s3resbucket1.objects.filter(Prefix=pathfile1):
            sensorproclogfiles.append(myBucket.key)
            # if len(sensorproclogfiles)>10:
            print('Here are your sensorproc files:\n',sensorproclogfiles)
                # break
        for myBucket in self.__s3resbucket2.objects.filter(Prefix=pathfile2):
            webentriesfiles.append(myBucket.key)
            # if len(webentriesfiles)>5:
            print('\nHere are your webui_entries:\n',webentriesfiles)
                # break
        print('SensorProc.json logs: ',sensorproclogfiles)
        print('WebEntries.txt logs: ',webentriesfiles)
        return sensorproclogfiles, webentriesfiles


    def download_sensorlog_files(self,bucket_name=BUCKET_SENSORPROCLOGS,sensorproclogfiles=[]):
        # pass
        numberOfFiles=0
        print('\nDownloading sensor proc files in this path: {}... .. .\n'.format(LOCAL_PATH_SENSORPROC))
        for myBucket in sensorproclogfiles:
            print(myBucket)
        # for myBucket in self.__s3resbucket1.objects.filter(Prefix=pathfile1):
            self.path,self.filename=os.path.split(myBucket)
            self.__s3resbucket1.download_file(myBucket, LOCAL_PATH_SENSORPROC+self.filename)
            numberOfFiles+=1
            if numberOfFiles >=2:
                break
        print('\nDownloaded SensorProcLogs into this path:\t',LOCAL_PATH_SENSORPROC)


    def download_webentries_files(self,bucket_name=BUCKET_WEBENTRIES,webentries=[]):
        # pass
        for myBucket in webentries:
        # for myBucket in self.__s3resbucket2.objects.filter(Prefix=pathfile2):
            self.path,self.filename=os.path.split(myBucket)
            self.__s3resbucket2.download_file(myBucket, LOCAL_PATH_WEBENTRIES+self.filename)
        print('\nDownloaded WebUiEntries into this path:\t',LOCAL_PATH_WEBENTRIES)

    def webentries_get_params(self):
        all_SPO2_entries=[]
        for eachEntry in os.listdir(LOCAL_PATH_WEBENTRIES):
            # print(eachEntry)
            if '.txt' in eachEntry:
                with open(LOCAL_PATH_WEBENTRIES+eachEntry)as f:
                    data=f.readlines()
                for eachLine in data:
                    if 'Start Time' in eachLine:
                        starttime=int(eachLine[-12:-2])
                for eachLine in data:
                    if 'SPO2' in eachLine:
                        all_SPO2_entries.append((int(eachLine[-18:-8]),int(eachLine[-5:-3])))
                        # print(all_SPO2_entries)
        return starttime, all_SPO2_entries

    def maxminHR(self,starttime,spo2):
        print('Calculating max-min of HR-RR')
        fileLog={}
        maxSeqHr=0
        minSeqHr=1000
        
        maxSeqRr=0
        minSeqRr=1000

        filenumber=0

        k=0#counter
        m=0#counter
        a=0#counter
        b=0#counter
        maxEntriesHr=[]
        minEntriesHr=[]
        maxEntriesRr=[]
        minEntriesRr=[]
        # files=
        files=pid+'-'+str(filenumber)
        # logfilepath='/home/samyak/scripts/Learning/s3pull/Logs/sensorproc/'
        fileLog[files]={
            'FileStartTime':0,
            'minHr':{'HR':-1,'RR':-1,'TsECG':0,'Seq':0,'EpochTime':0,'SPO2':[]},
            'maxHr':{'HR':-1,'RR':-1,'TsECG':0,'Seq':0,'EpochTime':0,'SPO2':[]},
            'maxRr':{'HR':-1,'RR':-1,'TsECG':0,'Seq':0,'EpochTime':0,'SPO2':[]},
            'minRr':{'HR':-1,'RR':-1,'TsECG':0,'Seq':0,'EpochTime':0,'SPO2':[]},
            'FileEndTime':0
            }
        for eachFiles in os.listdir(LOCAL_PATH_SENSORPROC):
            # fileLog[files].update({filenumber)
            # print(fileLog.keys())
            # files=pid+'-'+str(filenumber)
            fileStartTime=0
            fileEndTime=0
            if '.json' in eachFiles:
                print(LOCAL_PATH_SENSORPROC+eachFiles)
                data=pd.read_json(LOCAL_PATH_SENSORPROC+eachFiles,lines=True)
                print(data)
                minSeqTsECG=data.iloc[len(data)-len(data)]['TsECG']
                maxSeqTsECG=data.iloc[len(data)-1]['TsECG']
                print(minSeqTsECG,maxSeqTsECG)
                for keys in data.keys():
                    # print(data.keys())
                    if 'HR' in keys:
                        for eachHrRow in data['HR']:
                            # for eachElement in eachHrRow:
                                # if eachElement > 0 and eachElement > maxSeqHr:

                            if max(eachHrRow) > 0 and k < len(data['HR']):
                                if max(eachHrRow) > maxSeqHr:
                                    maxSeqHr=max(eachHrRow)
                                    maxEntriesHr=data.iloc[k]
                            k+=1
                            if min(eachHrRow) > 0 and m < len(data['HR']):
                                if min(eachHrRow) < minSeqHr:
                                    minSeqHr=min(eachHrRow)
                                    minEntriesHr=data.iloc[m]
                            m+=1
                    if 'RR_OUT' in keys:
                        for eachRrRow in data['RR_OUT']:
                            if max(eachRrRow) > 0 and a < len(data['RR_OUT']):
                                if max(eachRrRow) > maxSeqRr:
                                    maxSeqRr=max(eachRrRow)
                                    maxEntriesRr=data.iloc[a]
                            a+=1
                            if min(eachRrRow) > 0 and b < len(data['RR_OUT']):
                                if min(eachRrRow) < minSeqRr:
                                    minSeqRr=min(eachRrRow)
                                    minEntriesRr=data.iloc[b]
                            b+=1

                    filenumber+=1
                # fileLog.update({'filename':filenumber})
                    if 'HR' in maxEntriesHr and 'HR' in minEntriesHr:
                        fileLog[files]['maxHr']['HR']=max(maxEntriesHr['HR'])
                        fileLog[files]['minHr']['HR']=min(minEntriesHr['HR'])

                        if 'TsECG' in maxEntriesHr and 'TsECG' in minEntriesHr:
                            fileLog[files]['maxHr']['TsECG']=maxEntriesHr['TsECG']
                            fileLog[files]['minHr']['TsECG']=minEntriesHr['TsECG']
                            fileLog[files]['FileStartTime']=int(minSeqTsECG/1e6)+starttime
                            fileLog[files]['FileEndTime']=int(maxSeqTsECG/1e6)+starttime
                            fileStartTime=int(minSeqTsECG/1e6)+starttime
                            fileEndTime=int(maxSeqTsECG/1e6)+starttime
                        
                        if 'Seq' in maxEntriesHr and 'Seq' in minEntriesHr:
                            fileLog[files]['maxHr']['Seq']=maxEntriesHr['Seq']
                            fileLog[files]['minHr']['Seq']=minEntriesHr['Seq']
                            fileLog[files]['maxHr']['EpochTime']=starttime+int(maxEntriesHr['TsECG']/1e6)
                            fileLog[files]['minHr']['EpochTime']=starttime+int(maxEntriesHr['TsECG']/1e6)

                    if 'RR_OUT' in maxEntriesRr and 'RR_OUT' in minEntriesRr:
                        fileLog[files]['maxRr']['RR']=min(maxEntriesRr['RR_OUT'])
                        fileLog[files]['minRr']['RR']=min(minEntriesRr['RR_OUT'])

                        if 'TsECG' in maxEntriesRr and 'TsECG' in minEntriesRr:
                            fileLog[files]['maxRr']['TsECG']=maxEntriesRr['TsECG']
                            fileLog[files]['minRr']['TsECG']=minEntriesRr['TsECG']
                            
                            if 'Seq' in maxEntriesRr and 'Seq' in minEntriesRr:
                                fileLog[files]['maxRr']['Seq']=maxEntriesRr['Seq']
                                fileLog[files]['minRr']['Seq']=maxEntriesRr['Seq']
                                fileLog[files]['maxRr']['EpochTime']=starttime+int(maxEntriesRr['TsECG']/1e6)
                                fileLog[files]['minRr']['EpochTime']=starttime+int(minEntriesRr['TsECG']/1e6)

                                fileLog[files]['maxHr']['SPO2']=[]
                                fileLog[files]['maxRr']['SPO2']=[]
                                fileLog[files]['minHr']['SPO2']=[]
                                fileLog[files]['minRr']['SPO2']=[]
                                for eachEntry in spo2_entries:
                                    if eachEntry[0]>= fileStartTime:
                                        print(fileStartTime,fileEndTime)
                                        if eachEntry[0] <= fileEndTime:
                                                # print(eachEntry[0],eachEntry[0]>=fileStartTime,eachEntry[0]<=fileEndTime)
                                            fileLog[files]['maxHr']['SPO2'].append([eachEntry[0],eachEntry[1]])
                                            fileLog[files]['maxRr']['SPO2'].append([eachEntry[0],eachEntry[1]])
                                            fileLog[files]['minHr']['SPO2'].append([eachEntry[0],eachEntry[1]])
                                            fileLog[files]['minRr']['SPO2'].append([eachEntry[0],eachEntry[1]])
                                # for i in fileLog[files]:
            #     print(i,type(i))
            print(fileLog)  
            filePath=x+'/LifeSigns_Logs/{}/logging.txt'.format(pid)      
            try:
                with open(filePath,'a') as f:
                    print('Updating the log file')
                    f.write(str(fileLog))
                    f.write('\n')
                    # ujson.dump(fileLog,f,indent=2)
                    print('Updated the logging.json\n')
            except TypeError as T:
                print(T)
                exit()
                # with open('/home/samyak/scripts/Learning/s3pull/Logs/logging.json','w')as f:
                #     print('Logging the readings...')
                #     json.dump(fileLog,f,indent=3,separators=(',',':'))    
                #     # print(fileLog,fileStartTime,fileEndTime)
                #     print('File logiging is done')

if __name__=='__main__':

    # pathfile1 = '/{}'.format(pid)
    # pathfile2 = '/{}/.txt'.format(pid)

    print('PatchId:',pid)
    if not args.patchId:
        print('No PatchId given as arguement')
        exit()

    s3obj=S3Pull(ACCESS_KEY,SECRET_KEY)
    a,b=s3obj.get_s3_filelist(pid)
    print(a,b)
    s3obj.download_webentries_files(webentries=b)
    starttime,spo2_entries=s3obj.webentries_get_params()
    # print('here-----',starttime,spo2_entries)
    t1=time.time()
    print(datetime.fromtimestamp(t1))
    s3obj.download_sensorlog_files(sensorproclogfiles=a)
    t2=time.time()
    print(int(t2-t1))
    print(datetime.fromtimestamp(t2))
    a=s3obj.maxminHR(starttime,spo2_entries)
    # print(starttime,spo2_entries)
