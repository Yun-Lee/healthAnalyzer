import pandas as pd
from pymongo import MongoClient
from functools import reduce
from bson import ObjectId


class demo_class:
    
    def __init__(self):
        
        self.uri = 'privateURI'
        self.client = MongoClient(self.uri)
        self.db = self.client.release
        
        # get patient data
        self.collection = self.db.patients
        self.patient_table = pd.DataFrame(list(self.collection.find()))
        self.patient_data = self.patient_table[['_id', 'lastName', 'firstName', 'organization']]
        self.patient_data.columns = ['a', 'b']
        self.patient_data["name"] = self.patient_data["lastName"] + self.patient_data["firstName"]
        self.patient_data.columns = ['patient', 'lastName', 'firstName', 'organization', 'name']
        self.patient_data.to_csv('patient.csv', encoding='utf-8')
        
        # organization
        collection = self.db.organizations
        self.organizations_table = pd.DataFrame(list(collection.find({}, {'name': 1,'nickName': 1, 'solution':1})))
        self.organizations_table['org_id'] = self.organizations_table['_id']
        self.organizations_table.to_csv('organizations_table.csv', encoding='utf-8')
    
    
    def Get_vitalsign(self):
        
        '''vitalsigns'''
        
        collection = self.db.vitalsigns
        vitalsigns_data = pd.DataFrame(list(collection.find({}, {'createdDate':1,
                                                                 'TP':1,
                                                                 'PR':1,
                                                                 'SYS':1,
                                                                 'DIA':1,
                                                                 'patient':1,
                                                                 'organization':1,
                                                                 'RR':1,
                                                                 'SPO2':1})))
        patient_vitalsign = self.patient_data.merge(vitalsigns_data, left_on='_id', right_on = 'patient', how='left', indicator=True)
        patient_vitalsign.to_csv('patient_vitalsign.csv', encoding='utf-8')
    
    
    def Get_bloodsugar(self):
        
        '''bloodsugar'''
        
        collection = self.db.bloodsugars
        bloodsugar_data = pd.DataFrame(list(collection.find({}, {'insulin': 1,'createdDate': 1,
                                                                  'patient': 1, 'sugarValue':1,
                                                                  'sugarType':1, 'organization':1})))
        # convert ObjectID
        for i in range(len(bloodsugar_data)):
            bloodsugar_data['insulin'][i] = str(bloodsugar_data['insulin'][i])
            bloodsugar_data['insulin'][i] = bloodsugar_data['insulin'][i].replace("ObjectId('",'').replace("')",'').replace('[','').replace(']','').split(', ')
        
        # bloodsugar_data_2 = bloodsugar_data.explode('insulin')
        patient_bloodsugar = self.patient_data.merge(bloodsugar_data, left_on='_id', right_on = 'patient', how='left', indicator=True)
        patient_bloodsugar.to_csv('patient_bloodsugar.csv', encoding='utf-8')
        
        
    def Get_insulin(self):
        
        '''insulins'''
        
        collection = self.db.insulins
        insulins_data = pd.DataFrame(list(collection.find({}, {'_id':1,
                                                               'createdDate':1,
                                                               'patient':1,
                                                               'part':1,
                                                               'medicine':1,
                                                               'dose':1,
                                                               'bloodsugar':1,
                                                               'state':1})))
        patient_insulin = self.patient_data.merge(insulins_data, left_on='_id', right_on = 'patient', how='left', indicator=True)
        patient_insulin.to_csv('patient_insulin.csv', encoding='utf-8')
        
        
    def Get_weight(self):
        
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
        
        patient_weight = self.patient_data.merge(weights_data, left_on='_id', right_on = 'patient', how='left', indicator=True)
        patient_weight.to_csv('patient_weight.csv', encoding='utf-8')
        
        
    def Get_cognitive(self):
        
        '''cognitives'''
        
        collection = self.db.cognitives
        cognitives_data = pd.DataFrame(list(collection.find({}, {'patient':1,
                                                                 'createdDate':1,
                                                                 'score':1,
                                                                 'unassessable':1})))
        patient_cognitive = self.patient_data.merge(cognitives_data, left_on='_id', right_on = 'patient', how='left', indicator=True)
        patient_cognitive.to_csv('patient_cognitive.csv', encoding='utf-8')
        
        
    def Get_depression(self):
        
        '''depressions'''
        
        collection = self.db.depressions
        depressions_data = pd.DataFrame(list(collection.find({}, {'patient':1,
                                                                 'createdDate':1,
                                                                 'score':1,
                                                                 'unassessable':1})))
        patient_depression = self.patient_data.merge(depressions_data, left_on='_id', right_on = 'patient', how='left', indicator=True)
        patient_depression.to_csv('patient_depression.csv', encoding='utf-8')
            
        
    def Get_fall(self):
        
        '''falls'''
    
        collection = self.db.falls        
        falls_data = pd.DataFrame(list(collection.find({}, {'patient':1,
                                                            'unassessable':1,
                                                            'createdDate':1,
                                                            'score':1,
                                                            'type':1})))
        patient_fall = self.patient_data.merge(falls_data, left_on='_id', right_on = 'patient', how='left', indicator=True)
        patient_fall.to_csv('patient_fall.csv', encoding='utf-8')  
        
        
    def Get_barthel(self):
        
        '''barthels'''
        
        collection = self.db.barthels    
        barthels_data = pd.DataFrame(list(collection.find({}, {'patient':1,
                                                               'unassessable':1,
                                                               'createdDate':1,
                                                               'score':1})))
        patient_barthel = self.patient_data.merge(barthels_data, left_on='_id', right_on = 'patient', how='left', indicator=True)
        patient_barthel.to_csv('patient_barthel.csv', encoding='utf-8')
        
        
    def Get_iadl(self):
        
        '''iadlassessments'''
        
        collection = self.db.iadlassessments
        iadlassessments_data = pd.DataFrame(list(collection.find({}, {'patient':1,
                                                                      'unassessable':1,
                                                                      'createdDate':1,
                                                                      'score':1})))
        patient_iadl = self.patient_data.merge(iadlassessments_data, left_on='_id', right_on = 'patient', how='left', indicator=True)
        patient_iadl.to_csv('patient_iadl.csv', encoding='utf-8')
            
        
    def Get_badl(self):
        
        '''badls'''
        
        collection = self.db.badls
        badls_data = pd.DataFrame(list(collection.find({}, {'patient':1,
                                                            'unassessable':1,
                                                            'createdDate':1,
                                                            'score':1})))
        patient_badl = self.patient_data.merge(badls_data, left_on='_id', right_on = 'patient', how='left', indicator=True)
        patient_badl.to_csv('patient_badl.csv', encoding='utf-8')
        
        
    def Get_pressure(self):
    
        # pressures
    
        collection = self.db.pressures
        pressures_data = pd.DataFrame(list(collection.find({}, {'patient':1,
                                                                'unassessable':1,
                                                                'createdDate':1,
                                                                'score':1,
                                                                'assessmentType':1})))
        patient_pressure = self.patient_data.merge(pressures_data, left_on='_id', right_on = 'patient', how='left', indicator=True)
        patient_pressure.to_csv('patient_pressure.csv', encoding='utf-8')
        
        
    def Get_mmse(self):
        
        '''mmses'''
    
        collection = self.db.mmses   
        mmses_data = pd.DataFrame(list(collection.find({}, {'patient':1,'unassessable':1,
                                                            'createdDate':1,'organization':1,
                                                            'education':1,'orientationQ1':1,'orientationQ2':1,
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
        
        for col in mmses_data:
            mmses_data[col] = mmses_data[col].replace(['notCorrect'], 0)
            mmses_data[col] = mmses_data[col].replace(['correct'], 1)
            
        patient_mmse = self.patient_data.merge(mmses_data, left_on='patient', right_on = 'patient', how='left', indicator=True)
        patient_mmse.to_csv('patient_mmse.csv', encoding='utf-8')
        
        
    def Get_mna(self):
        
        ''''mna'''
        
        collection = self.db.mininutritions
        mininutritions_data = pd.DataFrame(list(collection.find({}, {'Q1':1, 'Q2':1, 'Q3':1, 'Q4':1, 'Q5':1,
                                                                     'Q6':1,'Q7':1,'Q8':1,'Q9':1,'Q10':1,'Q11':1,
                                                                     'Q12':1,'Q13':1,'Q14':1,'Q15':1,'Q16':1,'Q17':1,
                                                                     'Q18':1, 'patient':1, 'createdDate':1})))
        patient_mininutritions = self.patient_data.merge(mininutritions_data, left_on='patient', right_on = 'patient', how='left', indicator=True)
        patient_mininutritions.to_csv('patient_mininutritions.csv', encoding='utf-8')
            
        
    def Get_phyasst(self):
        
        '''phyassts'''
        
        collection = self.db.phyassts
        phyassts_data = pd.DataFrame(list(collection.find({}, {'musUpperR':1, 'musUpperL':1, 'musLowerR':1, 'musLowerL':1, 'conscious':1,
                                                               'musTypeUpperR':1,'musTypeUpperL':1,'musTypeLowerL':1,
                                                               'musTypeLowerR':1,'createdDate':1,'patient':1,
                                                               'organization':1,'activity':1,'constraints':1,'intake':1})))
        phyassts_data = self.patient_data.merge(phyassts_data, left_on='patient', right_on = 'patient', how='left', indicator=True)
        phyassts_data.to_csv('patient_phyassts.csv', encoding='utf-8')
            
        
    def Get_bedsore(self):
        
        ''''bedsores'''
        
        collection = self.db.bedsores
        bedsores_data = pd.DataFrame(list(collection.find({}, {'latestGrade':1, 'finishedNote':1, 'finished':1, 'grade':1,
                                                               'locationOccurMemo':1,'locationOccur':1,'woundType':1,
                                                               'site':1,'createdDate':1,'patient':1, 'organization':1})))
        
        '''bedsore detail'''
        
        collection = self.db.bedsoredetails
        bedsoredetails_table = pd.DataFrame(list(collection.find()))
        bedsoredetails_data = bedsoredetails_table[['patient','skinStatus','createdDate']]
            
        '''combine'''
        bedsores_data = self.patient_data.merge(bedsores_data, left_on='_id', right_on = 'patient', how='left', indicator=True)
        
        # compile the list of dataframes you want to merge
        data_frames = [self.patient_data, bedsoredetails_data, bedsores_data]
        df_merged = reduce(lambda  left,right: pd.merge(left,right,on=['patient'], how='outer'), data_frames)
        
        df_merged.to_csv('bedsore_detail.csv', encoding='utf-8')
        
        
    def Get_marv2(self):
    
        '''marv2'''
    
        collection = self.db.patients
        marv2s_data = pd.DataFrame(list(collection.find({}, {'_id':1, 'organization':1, 'lastName':1, 'firstName':1, 'sex':1,
                                                             'birthday':1, 'checkInDate':1, 'room':1, 'branch':1, 'bed':1,
                                                             'medications':1, 'drugAllergies':1, 'foodAllergies':1, 'medicalHistory':1,
                                                             'tube':1, 'DNRConsent':1, 'familyHealthProblems':1,
                                                             'doctorDivision':1,'status':1,
                                                             'hospital':1,'emgcyContPhone':1,
                                                             'emgcyCont':1,'idNumber':1,'phone':1,
                                                             'emgcyContRelation':1,
                                                             'emgcyContAddress':1})))
        
        marv2s_data["name"] = marv2s_data["lastName"] + marv2s_data["firstName"]
        marv2s_data.to_csv('marv2s_data.csv', encoding='utf-8')
        
        
    def Get_eq5d(self):
        
        '''eq5d'''
        
        collection = self.db.eq5ds
        eq5ds_data = pd.DataFrame(list(collection.find({}, {'patient':1,
                                                            'unassessable':1,
                                                            'createdDate':1,
                                                            'score':1,
                                                            'organization':1,
                                                            'eqvas':1})))
        
        patient_eq5d = self.patient_data.merge(eq5ds_data, left_on='_id', right_on = 'patient', how='left', indicator=True)
        patient_eq5d.to_csv('patient_eq5d.csv', encoding='utf-8')
        
        
    def Get_eat10(self):
    
        '''eat-10'''
    
        collection = self.db.swallowings
        swallowings_data = pd.DataFrame(list(collection.find({}, {'patient':1,
                                                                  'unassessable':1,
                                                                  'createdDate':1,
                                                                  'organization':1,
                                                                  'formMode':1,
                                                                  'saliva':1})))
        
        patient_swallowing = self.patient_data.merge(swallowings_data, left_on='_id', right_on = 'patient', how='left', indicator=True)
        patient_swallowing.to_csv('patient_swallowing.csv', encoding='utf-8')
            
        
    def Get_smartschedule(self):
        
        '''smartschedules'''
        
        collection = self.db.smartschedules
        smartschedules_data = pd.DataFrame(list(collection.find({}, {'patientSelected':1,
                                                                     'completed':1,
                                                                     'deleted':1,
                                                                     'startTime':1,
                                                                     'endTime':1,
                                                                     'category':1,
                                                                     'task':1,
                                                                     'organization':1,
                                                                     'createdDate':1,
                                                                     'subCategory':1})))
            
        # convert ObjectId
        for i in range(len(smartschedules_data)):
            smartschedules_data['patientSelected'][i] = str(smartschedules_data['patientSelected'][i])
            smartschedules_data['patientSelected'][i] = smartschedules_data['patientSelected'][i].replace("ObjectId('",'').replace("')",'')
            smartschedules_data['patientSelected'][i] = smartschedules_data['patientSelected'][i].replace("['",'').replace("']",'')
        
        # rename column
        smartschedules_data.columns = ['patient','completed','deleted','startTime','endTime','category','task',
                                               'organization','createdDate','subCategory']
        smartschedules_data.to_csv('patient_smartschedule.csv', encoding='utf-8')
        
        
    def Get_shifts(self):
        
        '''交班'''
    
        collection = self.db.shifts        
        shifts_data = pd.DataFrame(list(collection.find({}, {'patient':1,
                                                            'notes':1,
                                                            'createdDate':1,
                                                            'organization':1,
                                                            'shiftname':1,
                                                            'notification':1})))
        
        patient_shifts = self.patient_data.merge(shifts_data, left_on='patient', right_on = 'patient', how='left', indicator=True)
        patient_shifts.to_csv('patient_shifts.csv', encoding='utf-8')
        
        
    def Get_nursingrecord(self):
    
        '''護理紀錄'''
    
        collection = self.db.nursingdiagnosisrecords
        nursingdiagnosisrecords_data = pd.DataFrame(list(collection.find({}, {'evaluation':1,
                                                                              'patient':1,
                                                                              'createdDate':1,
                                                                              'goals':1,
                                                                              'nursingDiagnosis':1,
                                                                              'organization':1,
                                                                              'changePlan':1})))
        
        patient_nursingrecords = self.patient_data.merge(nursingdiagnosisrecords_data, left_on='patient', right_on = 'patient', how='left', indicator=True)
        patient_nursingrecords.to_csv('patient_nursingrecords.csv', encoding='utf-8')
        
        
    def Get_feedingtype(self):
        
        '''進食方式'''
    
        collection = self.db.nutritionrecords        
        nutritionrecords_data = pd.DataFrame(list(collection.find({}, {'organization':1,
                                                                       'patient':1,
                                                                       'feedingMethod':1,
                                                                       'mobility':1,
                                                                       'feedingAbility':1,
                                                                       'foodType':1,
                                                                       'foodIntake':1,
                                                                       'advice':1,
                                                                       'nutrientIntake':1,
                                                                       'createdDate':1})))
        
        patient_nutritionrecord = self.patient_data.merge(nutritionrecords_data, left_on='patient', right_on = 'patient', how='left', indicator=True)
        patient_nutritionrecord.to_csv('patient_nutritionrecord.csv', encoding='utf-8')
        
