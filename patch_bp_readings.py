import boto3,os
import argparse
ACCESS_KEY=''
SECRET_KEY=''
BUCKET_SENSORPROCLOGS=''
BUCKET_WEBENTRIES=''

parser = argparse.ArgumentParser(description='Patch Id')
parser.add_argument('-n',dest='numberOfPatches', type=str, help='Please enter a number')
args = parser.parse_args()
n = int(args.numberOfPatches)
pathfile2 = '/'

class Patch_Web():

    def __init__(self,ACCESS_KEY,SECRET_KEY):
        sessions=boto3.Session(aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY,region_name='ap-south-1')
        self.__s3=sessions.resource('s3')
        self.__s3resbucket1=self.__s3.Bucket(BUCKET_SENSORPROCLOGS)
        self.__s3resbucket2=self.__s3.Bucket(BUCKET_WEBENTRIES)
        

    def patchLists(self):
        k=set()
        x=os.path.abspath(os.path.dirname('pwd'))
        os.mkdir(x+'/BP_Readings')
        for myBucket in self.__s3resbucket2.objects.filter(Prefix=pathfile2):
            # print(myBucket)
            self.name,self.path=os.path.split(myBucket.key)
            # print(name)
            c=os.path.split(self.name)
            # print(c)
            # break
            k.add(c[1])
            # print(k)
            if len(k)>=n:
                # pass
                print('PatchID Lists',k)
                # print(d)
                for i in range(len(k)):
                    h=k.pop()
                    # print('------>',h)
                    # print('aaa')
                    pathfile1='LSSERVER/{}'.format(h)
                    sensorproclogfiles=[]
                    for myBucket in self.__s3resbucket1.objects.filter(Prefix=pathfile1):
                        # self.fn,self.path=os.path.split(myBucket.key)
                        sensorproclogfiles.append(myBucket.key)
                        # print('------')
                        # print(myBucket.key)
                    if os.path.isdir(x+'/BP_Readings'):
                        # os.mkdir('BP_Readings')
                        # print('IFIFIFFI')
                        os.mkdir('BP_Readings/{}'.format(h))
                        # os.mkdir('BP_Readings/{}/'.format(h))
                        LOCAL_PATH_WEBENTRIES=x+'/BP_Readings/{}/'.format(h)
                        pathfile3 ='/{}/Webui_entries.txt'.format(h)    
                        self.path,self.filename=os.path.split(pathfile3)
                        ## Downloading Webui_entries.txt
                        self.__s3resbucket2.download_file(pathfile3, LOCAL_PATH_WEBENTRIES+self.filename)
                        bp_sys=0
                        bp_dia=0
                        for eachEntry in os.listdir(LOCAL_PATH_WEBENTRIES):
                            filename=eachEntry
                            if '.txt' in eachEntry:
                                with open(LOCAL_PATH_WEBENTRIES+eachEntry) as f:
                                    data=f.readlines()
                                    # print(data)
                                    for eachLineNum in range(len(data)-1):
                                        if 'Systolic' in data[eachLineNum]:
                                            # print(data[eachLineNum])
                                            bp_sys+=1
                                    for eachLineNum in range(len(data)-1):
                                        if 'Diastolic' in data[eachLineNum]:
                                            # print(data[eachLineNum])
                                            bp_dia+=1
                                    # if bp_dia>0:
                                    #     num_of_bp_readings=bp_dia
                                    if bp_sys>0 and bp_dia>0:
                                        # print('BP Sys:{},BP Dia:{}'.format(bp_sys,bp_dia),eachEntry)
                                        print('PatchID:{}, BP Sys:{},BP Dia:{}, Sensorproc files:{}'.format(h,bp_sys,bp_dia,len(sensorproclogfiles)),eachEntry)
                                        # num_of_bp_readings=bp_sys
                                    elif bp_dia==0 and bp_sys==0:
                                        # num_of_bp_readings=0
                                        # print('No BP readings in Webui_entries.txt of the patch',eachEntry)
                                        print('PatchID:{}, BP Sys:{},BP Dia:{}, Sensorproc files:{}'.format(h,bp_sys,bp_dia,len(sensorproclogfiles)),eachEntry)
                                        # print('PatchID:{}, BP Sys:{},BP Dia:{}'.format(h,bp_sys,bp_dia),eachEntry)
                                    else:
                                        print('PatchID:{}, BP Sys:{},BP Dia:{}, Sensorproc files:{}'.format(h,bp_sys,bp_dia,len(sensorproclogfiles)),eachEntry)

                 

                    else:
                        print('ELSEE')
                        os.mkdir('BP_Readings')
                        os.mkdir('BP_Readings/{}'.format(h))
                        # os.mkdir('BP_Readings/{}/'.format(h))
                        LOCAL_PATH_WEBENTRIES=x+'/BP_Readings/{}/'.format(h)
                        pathfile3 ='/{}/Webui_entries.txt'.format(h)    
                        self.path,self.filename=os.path.split(pathfile3)
                        self.__s3resbucket2.download_file(pathfile3, LOCAL_PATH_WEBENTRIES+self.filename)
                break              
                    # for j in h#s3res2.objects.filter(Prefix=pathfile3):
                # print(h)
                # h='{}/{}/'.format(h[0],h[1])
                # print(h)
        # print('\nDownloaded WebUiEntries into this path:\t')#,LOCAL_PATH_WEBENTRIES)


if __name__ == '__main__':
    obj=Patch_Web(ACCESS_KEY,SECRET_KEY)

    print('Number of patches:',n)
    if not args.numberOfPatches:
        print('No number given as arguement')
        exit()
    
    # print('THANK YOU')
    # webentries=obj.get_s3_filelist(pid)
    # print('WEBENTRIES-----',webentries)
    # obj.download_webentries_files(webentries=webentries)
    # bp_readings,fname=obj.webentries_get_params()
    # obj.log(pid,readings=bp_readings,filename=fname)
    obj.patchLists()
