import pandas as pd
import numpy as np
from pymongo import MongoClient
from functools import reduce
from bson import ObjectId


class MyClass:
    
    def __init__(self):
        
        self.uri = 'mongodb+srv://jubo-nis:tfyVTveUu2hISK01@jubo-mongo.iiltl.gcp.mongodb.net/release?authSource=admin&retryWrites=true&w=majority'
        self.client = MongoClient(self.uri)
        self.db = self.client.release
        
        # get patient data
        self.collection = self.db.patients
        self.patient_data = pd.DataFrame(list(self.collection.find({}, {'lastName':1,
                                                                 'firstName':1,
                                                                 'organization':1})))
        self.patient_data["name"] = self.patient_data["lastName"] + self.patient_data["firstName"]
        self.patient_data.columns = ['patient', 'lastName', 'firstName',
                                     'organization', 'name']
        del self.patient_data['lastName']
        del self.patient_data['firstName']
        
        # organization
        collection = self.db.organizations
        self.organizations_table = pd.DataFrame(list(collection.find({},
                                            {'name': 1,'nickName': 1, 'solution':1})))
        self.organizations_table.columns = ['org_id', 'name', 'nickName', 'solution']
        
        '''combine patient and organization'''
        self.patient_org = self.patient_data.merge(self.organizations_table,
                left_on='organization', right_on = 'org_id', how='left', indicator=True)
        self.patient_org.columns = ['patient', 'organization', 'name', 'org_id', 
                                    'org_name', 'nickName',
                                    'solution', '_merge']
        del self.patient_org['org_id']
    
    
    def get_vitalsign(self):
        
        '''vitalsigns'''
        
        collection = self.db.vitalsigns
        vitalsigns_data = pd.DataFrame(list(collection.find({}, {'createdDate':1,
                                                                 'TP':1,
                                                                 'PR':1,
                                                                 'SYS':1,
                                                                 'DIA':1,
                                                                 'patient':1,
                                                                 'RR':1,
                                                                 'SPO2':1})))
        
        '''combine'''
        data_frames = [vitalsigns_data, self.patient_org]
        patient_org_vitalsigns = reduce(lambda left, right: pd.merge(left, right,
                                        on=['patient'], how='left'), data_frames)
        del patient_org_vitalsigns['_merge']
        return patient_org_vitalsigns

    
    def get_bloodsugar_insulin(self):
        
        '''bloodsugar'''
        
        collection = self.db.bloodsugars
        bloodsugar_data = pd.DataFrame(list(collection.find({}, {'insulin': 1,'createdDate': 1,
                                                                  'patient': 1, 'sugarValue':1,
                                                                  'sugarType':1})))
        # convert ObjectID
        
        bloodsugar_data = bloodsugar_data.explode('insulin')
        bloodsugar_data.replace(np.nan,'-', inplace=True)
        bloodsugar_data.columns = ['bloodsugar_id','sugarType','sugarValue','patient',
                                     'createdDate','insulin']
        
        '''insulins'''
        
        collection = self.db.insulins
        insulins_data = pd.DataFrame(list(collection.find({}, {'_id':0,
                                                               'createdDate':1,
                                                               'part':1,
                                                               'medicine':1,
                                                               'dose':1,
                                                               'bloodsugar':1,
                                                               'state':1})))

        insulins_data.columns = ['medicine', 'dose','part','createdDate',
                                 'state','bloodsugar_id']
        insulins_data.replace(np.nan,'-', inplace=True)
        
        
        '''combine'''

        datafrmae = [bloodsugar_data, insulins_data]
        bloodsugar_insulin_source = reduce(lambda left,right: pd.merge(left,
                                        right,on=['bloodsugar_id'], how='outer'), datafrmae)
        
        bloodsugar_insulin_source.columns = ['bloodsugar_id','sugarType','sugarValue',
                                             'patient','createdDate_bs','insulin_id',
                                             'medicine','dose','part','createdDate_insulin',
                                             'state']
        
        datafrmae = [bloodsugar_insulin_source, self.patient_org]
        bloodsugar_insulin_source = reduce(lambda left,right: pd.merge(left,
                                        right,on=['patient'], how='left'), datafrmae)
        del bloodsugar_insulin_source['_merge']
        return bloodsugar_insulin_source
    
        
    def get_weight(self, bedsores_data):
        
        '''weight'''
        
        collection = self.db.weights
        weights_data = pd.DataFrame(list(collection.find({}, {'createdDate':1,
                                                              'height':1,
                                                              'weight':1,
                                                              'patient':1,
                                                              'bmi':1,
                                                              'difference':1,
                                                              'differenceWeight':1,
                                                              'month3':1,
                                                              'monthWeight3':1,
                                                              'month6':1,
                                                              'monthWeight6':1})))
        
        '''combine'''

        weight_source = [weights_data, self.patient_org]
        weight_datasource = reduce(lambda  left,right: pd.merge(left,
                                right,on=['patient'], how='left'), weight_source)
        del weight_datasource['_merge']
        return weight_datasource


    def get_nutritionrecords(self):
    
        '''進食方式'''
    
        collection = self.db.nutritionrecords        
        nutritionrecords_data = pd.DataFrame(list(collection.find({}, {'patient':1,
                                                                       'feedingMethod':1,
                                                                       'mobility':1,
                                                                       'feedingAbility':1,
                                                                       'foodType':1,
                                                                       'foodIntake':1,
                                                                       'advice':1,
                                                                       'nutrientIntake':1,
                                                                       'createdDate':1})))
        '''combine'''

        datafrmae = [nutritionrecords_data, self.patient_org]
        nutrition_source = reduce(lambda left,right: pd.merge(left,right,on=['patient'],
                                                              how='left'), datafrmae)
        nutrition_source.replace(np.nan,'-', inplace=True)
        del nutrition_source['_merge']
        return nutrition_source
    

    def get_mna(self):
        
        ''''mna'''
        
        collection = self.db.mininutritions
        mininutritions_data = pd.DataFrame(list(collection.find({}, {'Q1':1, 'Q2':1, 'Q3':1, 'Q4':1, 'Q5':1,
                                                                     'Q6':1,'Q7':1,'Q8':1,'Q9':1,'Q10':1,'Q11':1,
                                                                     'Q12':1,'Q13':1,'Q14':1,'Q15':1,'Q16':1,'Q17':1,
                                                                     'Q18':1, 'patient':1, 'createdDate':1})))
        
        '''combine'''
        
        datafrmae = [mininutritions_data, self.patient_org]
        mininutritions_source = reduce(lambda left,right: pd.merge(left,right,on=['patient'],
                                                                   how='left'), datafrmae)
        del mininutritions_source['_merge']
        return mininutritions_source

        
    def assessment(self):
        
        '''cognitives'''
        
        collection = self.db.cognitives
        cognitives_data = pd.DataFrame(list(collection.find({}, {'patient':1,
                                                                 'createdDate':1,
                                                                 'score':1,
                                                                 'unassessable':1})))
        cognitives_data.columns = ['patient_cogniitives','createdDate_cognitives',
                                   'score_cognitives','unassessable_cognitives']
        
        
        '''depressions'''
        
        collection = self.db.depressions
        depressions_data = pd.DataFrame(list(collection.find({}, {'patient':1,
                                                                  'createdDate':1,
                                                                  'score':1,
                                                                  'unassessable':1})))
        depressions_data.columns = ['patient_depression','createdDate_depression',
                                    'score_depression','unassessable_depression']

        
        '''falls'''
    
        collection = self.db.falls        
        falls_data = pd.DataFrame(list(collection.find({}, {'patient':1,
                                                            'createdDate':1,
                                                            'score':1,
                                                            'unassessable':1})))
        falls_data.columns = ['patient_fall', 'createdDate_fall','score_fall',
                              'unassessable_fall']
        
        
        '''barthels'''
        
        collection = self.db.barthels    
        barthels_data = pd.DataFrame(list(collection.find({}, {'patient':1,
                                                               'createdDate':1,
                                                               'score':1,
                                                               'unassessable':1})))
        barthels_data.columns = ['patient_barthel', 'createdDate_barthel','score_barthel',
                                 'unassessable_barthel']
        
        
        '''iadlassessments'''
        
        collection = self.db.iadlassessments
        iadlassessments_data = pd.DataFrame(list(collection.find({}, {'patient':1,
                                                                      'createdDate':1,
                                                                      'score':1,
                                                                      'unassessable':1})))
        iadlassessments_data.columns = ['patient_iadl','createdDate_iadl','score_iadl',
                                        'unassessable_iadl']
        
        
        '''badls'''
        
        collection = self.db.badls
        badls_data = pd.DataFrame(list(collection.find({}, {'patient':1,
                                                            'createdDate':1,
                                                            'score':1,
                                                            'unassessable':1})))
        badls_data.columns = ['patient_badl','createdDate_badl','score_badl',
                              'unassessable_badl']
        
    
        '''pressures'''
    
        collection = self.db.pressures
        pressures_data = pd.DataFrame(list(collection.find({}, {'patient':1,
                                                                'createdDate':1,
                                                                'score':1,
                                                                'unassessable':1})))
        pressures_data.columns = ['patient_pressure','createdDate_pressure',
                                  'score_pressure','unassessable_pressure']
        
        
        '''eq5d'''
        
        collection = self.db.eq5ds
        eq5ds_data = pd.DataFrame(list(collection.find({}, {'patient':1,
                                                            'createdDate':1,
                                                            'score':1,
                                                            'unassessable':1,
                                                            'eqvas':1})))
        
        eq5ds_data.columns = ['patient_eq5d','createdDate_eq5d',
                              'score_eq5d','unassessable_eq5d','eqvas_eq5d']
    
        '''eat-10'''
    
        collection = self.db.swallowings
        swallowings_data = pd.DataFrame(list(collection.find({}, {'patient':1,
                                                                  'unassessable':1,
                                                                  'createdDate':1,
                                                                  'formMode':1,
                                                                  'saliva':1})))
        swallowings_data.columns = ['patient_eat10','createdDate_eat10','saliva_eat10',
                                    'unassessable_eat10']
        
        
    def get_mmse(self):
        
        '''mmses'''
    
        collection = self.db.mmses   
        mmses_data = pd.DataFrame(list(collection.find({}, {'patient':1,'unassessable':1,
                                                            'createdDate':1,'education':1,
                                                            'orientationQ1':1,'orientationQ2':1,
                                                            'orientationQ3':1,'orientationQ4':1,
                                                            'orientationQ5':1,'orientationQ6':1,
                                                            'orientationQ7':1,'orientationQ8':1,
                                                            'orientationQ9':1,'orientationQ10':1,
                                                            'memoryRecordQ1':1,'memoryRecordQ2':1,
                                                            'memoryRecordQ3':1,'attentionAndCalcQ1':1,
                                                            'attentionAndCalcQ2':1,'attentionAndCalcQ3':1,
                                                            'attentionAndCalcQ4':1,'attentionAndCalcQ5':1,
                                                            'memoryQ1':1,'memoryQ2':1,'memoryQ3':1,
                                                            'languageQ1':1,'languageQ2':1,'languageQ3':1,
                                                            'languageQ4':1,'languageQ5':1,'languageQ6':1,
                                                            'languageQ7':1,'languageQ8':1,'constructiveQ1':1
                                                            })))        
        
        mmses_data.replace({'notCorrect': 0, 'correct': 1}, inplace=True)
        mmses_data.replace({'College': '大學',
                            'Elementary': '小學',
                            'Graduate': '研究所以上',
                            'Illiterate': '不識字',
                            'JuniorHigh': '國中',
                            'SeniorHigh': '高中職'}, inplace=True)
        mmses_data.replace(np.nan,'-', inplace=True)

        '''combine'''
        
        datafrmae = [mmses_data, self.patient_org]
        mmses_source = reduce(lambda left,right: pd.merge(left,right,on=['patient'],
                                                          how='left'), datafrmae)
        del mmses_source['_merge']
        return mmses_source

            
        
    def get_phyasst(self):
        
        '''phyassts'''
        
        collection = self.db.phyassts
        phyassts_data = pd.DataFrame(list(collection.find({}, {'musUpperR':1, 'musUpperL':1,
                                                               'musLowerR':1,
                                                               'musLowerL':1, 'conscious':1,
                                                               'musTypeUpperR':1,
                                                               'musTypeUpperL':1,
                                                               'musTypeLowerL':1,
                                                               'musTypeLowerR':1,
                                                               'createdDate':1,'patient':1,
                                                               'activity':1,'constraints':1,
                                                               'intake':1})))
        
        # 資料整理
        
        phyassts_data.replace(np.nan,'-', inplace=True)
        phyassts_data.replace({'normal': '正常',
                               'sightlyShrinked': '微萎縮',
                               'loose': '鬆弛',
                               'shrinked': '萎縮'}, inplace=True)
        phyassts_data['conscious'] = [','.join(map(str, l)) for l in phyassts_data['conscious']]
        phyassts_data['activity'] = [','.join(map(str, l)) for l in phyassts_data['activity']]
        phyassts_data['constraints'] = [','.join(map(str, l)) for l in phyassts_data['constraints']]
        phyassts_data['intake'] = [','.join(map(str, l)) for l in phyassts_data['intake']]
        
        '''combine'''
        
        datafrmae = [phyassts_data, self.patient_org]
        phyasst_source = reduce(lambda left,right: pd.merge(left,right,on=['patient'],
                                                            how='left'), datafrmae)
        del phyasst_source['_merge']
        return phyasst_source
          
        
    def get_bedsore(self):
        
        ''''bedsores'''
        
        collection = self.db.bedsores
        bedsores_data = pd.DataFrame(list(collection.find({}, {'_id':0,'latestGrade':1, 'finishedNote':1,
                                                               'finished':1, 'grade':1,
                                                               'locationOccurMemo':1,
                                                               'locationOccur':1,'woundType':1,
                                                               'site':1,'createdDate':1,
                                                               'patient':1})))

        bedsores_data.replace(np.nan,'-', inplace=True)
        bedsores_data.replace({'outside': '外側',
                            'inside': '內側',
                            'bedsore': '壓力性損傷',
                            'generalWound':'一般傷口'}, inplace=True)
        
        '''bedsore detail'''
        
        collection = self.db.bedsoredetails
        bedsoredetails_data = pd.DataFrame(list(collection.find({}, {'_id':0, 'patient':1,
                                                                     'skinStatus':1,
                                                                     'createdDate':1})))

        bedsoredetails_data['skinStatus'] = [','.join(map(str, l)) for l in bedsoredetails_data['skinStatus']]
        bedsoredetails_data.replace({'': '-',
                                     'normal':'完整',
                                     'redness':'泛紅',
                                     'swelling':'水腫',
                                     'nodule':'硬核',
                                     'normal,redness': '完整,泛紅',
                                     'redness,normal': '泛紅,完整',
                                     'swelling,normal':'水腫,完整',
                                     'normal,swelling':'完整,水腫',
                                     'None':'無',
                                     'nodule,normal':'硬核,完整',
                                     'normal,nodule':'完整,硬核',
                                     'redness,nodule':'泛紅,硬核',
                                     'normal,redness,nodule':'完整,泛紅,硬核',
                                     'redness,swelling':'泛紅,水腫',
                                     'swelling,redness':'水腫,泛紅'}, inplace=True)

        '''combine'''
        
        data_frames = [bedsores_data, bedsoredetails_data, self.patient_org]
        bedsore_source = reduce(lambda left,right: pd.merge(left,right,on=['patient'],
                                                       how='outer'), data_frames)
        del bedsore_source['_merge']
        bedsore_source.rename(columns={'createdDate_x':'createdDate_bedsore',
                                       'createdDate_y':'createdDate_detail'}, inplace=True)
        
        return bedsore_source
        
        
        
    def get_marv2(self):
    
        '''marv2'''
    
        collection = self.db.patients
        marv2s_data = pd.DataFrame(list(collection.find({}, {'_id':1,'sex':1,
                                                             'birthday':1, 'checkInDate':1,
                                                             'room':1, 'branch':1, 'bed':1,
                                                             'medications':1, 'drugAllergies':1,
                                                             'foodAllergies':1,
                                                             'medicalHistory':1,
                                                             'tube':1, 'DNRConsent':1,
                                                             'familyHealthProblems':1,
                                                             'doctorDivision':1,'status':1,
                                                             'hospital':1,'emgcyContPhone':1,
                                                             'emgcyCont':1,'idNumber':1,'phone':1,
                                                             'emgcyContRelation':1,
                                                             'emgcyContAddress':1})))
        
        marv2s_data.rename(columns={'_id':'patient'}, inplace=True)
        marv2s_data.replace({'female': '女性',
                             'male':'男性',
                             'child':'子女',
                             'spouse':'配偶',
                             'others':'其他',
                             'parent':'父母',
                             'sibling':'手足',
                             'socialWorker':'社工',
                             'grandchild':'孫子女',
                             'friend':'朋友',
                             '':'-'}, inplace=True)
        marv2s_data.replace(np.nan,'-', inplace=True)
        marv2s_data['familyHealthProblems'] = [','.join(map(str, l)) for l in marv2s_data['familyHealthProblems']]
        marv2s_data['medications'] = [','.join(map(str, l)) for l in marv2s_data['medications']]
        marv2s_data['medicalHistory'] = [','.join(map(str, l)) for l in marv2s_data['medicalHistory']]
        marv2s_data['tube'] = [','.join(map(str, l)) for l in marv2s_data['tube']]
        marv2s_data['drugAllergies'] = [','.join(map(str, l)) for l in marv2s_data['drugAllergies']]
        marv2s_data['foodAllergies'] = [','.join(map(str, l)) for l in marv2s_data['foodAllergies']]
        marv2s_data = marv2s_data.loc[marv2s_data['status'] == 'present']
        
        '''combine'''
        
        data_frames = [marv2s_data, self.patient_org]
        marv2s_source = reduce(lambda left,right: pd.merge(left,right,on=['patient'],
                                                       how='left'), data_frames)
        
        del marv2s_source['_merge']
        return marv2s_source
          
        
    def get_smartschedule(self):
        
        '''smartschedules'''
        
        collection = self.db.smartschedules
        smartschedules_data = pd.DataFrame(list(collection.find({}, {'patientSelected':1,
                                                                     'completed':1,
                                                                     'deleted':1,
                                                                     'startTime':1,
                                                                     'endTime':1,
                                                                     'category':1,
                                                                     'task':1,
                                                                     'createdDate':1,
                                                                     'subCategory':1})))
        smartschedules_data = smartschedules_data.explode('patientSelected')
            
        # rename column
        smartschedules_data.columns = ['_id','patient','completed','deleted','task',
                                       'startTime','endTime','category',
                                       'createdDate','subCategory']
        
        # delect patient值為空者
        smartschedules_data.replace(np.nan,'-', inplace=True)
        smartschedules_data = smartschedules_data.loc[smartschedules_data['patient'] != '-']
        
        '''combine'''
        
        data_frames = [smartschedules_data, self.patient_org]
        smartschedule_source = reduce(lambda left,right: pd.merge(left,right,on=['patient'],
                                                                  how='left'), data_frames)
        del smartschedule_source['_merge']
        return smartschedule_source

        
    def get_shifts(self):
        
        '''交班'''
    
        collection = self.db.shifts        
        shifts_data = pd.DataFrame(list(collection.find({}, {'_id':0,'patient':1,
                                                            'notes':1,
                                                            'createdDate':1,
                                                            'shiftname':1,
                                                            'notification':1})))
        # 資料處理
        
        shifts_data.replace(np.nan,'-', inplace=True)
        shifts_data.replace({'dayShift': '日班',
                            'eveningShift': '午班',
                            'nightShift': '晚班'}, inplace=True)
        
        '''combine'''
        
        data_frames = [shifts_data, self.patient_org]
        shift_source = reduce(lambda  left,right: pd.merge(left,right,on=['patient'],
                                                           how='left'), data_frames)
        del shift_source['_merge']
        return shift_source

        
        
    def get_nursingrecord(self):
    
        '''護理紀錄'''
    
        collection = self.db.nursingdiagnosisrecords
        nursingdiagnosisrecords_data = pd.DataFrame(list(collection.find({}, {'evaluation':1,
                                                                              'patient':1,
                                                                              'createdDate':1,
                                                                              'goals':1,
                                                                              'nursingDiagnosis':1,
                                                                              'changePlan':1})))
        # 資料處理
        nursingdiagnosisrecords_data.replace(np.nan,'-', inplace = True)
        
        '''combine'''
        
        data_frames = [nursingdiagnosisrecords_data, self.patient_org]
        nursingdiagnosisrecords_source = reduce(lambda left,right: pd.merge(left,
                                            right,on=['patient'], how='left'), data_frames)
        del nursingdiagnosisrecords_source['_merge']
        return nursingdiagnosisrecords_source



# '_id':0
# return
# two df merge
# init class: a = demo_class()
# b = a.Get_mna()
