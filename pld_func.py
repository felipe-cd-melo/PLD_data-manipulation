import requests
import pandas as pd
from io import StringIO
import os

class pld:

    pld_tipo = 'HORARIO'

    def __init__(self, initial_date, final_date= None):
        self.initial_date = initial_date
        if final_date is not None:
            self.final_date = final_date
        else: #get the next day
            daytnew = []
            for n in initial_date.split('/'):
                if n.isdigit() == True:
                    n = int(n)
                    daytnew.append(n)
            daytnew[0] = daytnew[0]+1
            self.final_date = '{}/{}/{}'.format(daytnew[0],daytnew[1],daytnew[2])
    
    # Connecting to CCEE API to get the pld data with defined date and price type
    def extract(self):
        pld_param = {
            'p_p_id': 'br_org_ccee_pld_historico_PLDHistoricoPortlet_INSTANCE_lzsn',
            'p_p_lifecycle': 2, 'p_p_state': 'normal',
            'p_p_mode': 'view',
            'p_p_cacheability': 'cacheLevelPage',
            '_br_org_ccee_pld_historico_PLDHistoricoPortlet_INSTANCE_lzsn_inputInitialDate': self.initial_date,
            '_br_org_ccee_pld_historico_PLDHistoricoPortlet_INSTANCE_lzsn_tipoPreco': self.pld_tipo,
            '_br_org_ccee_pld_historico_PLDHistoricoPortlet_INSTANCE_lzsn_inputFinalDate': self.final_date
            }
        pld_ccee = requests.get('https://www.ccee.org.br/web/guest/precos/painel-precos', params = pld_param, timeout = 200)

        return(pld_ccee.text)

    # Change de data schema for a better data visualization and easy of use
    def newschema(self,pldccee,save=False):
        self.pldccee = pd.read_csv(StringIO(pldccee), delimiter= ';')
        self.pldccee['Hora'] = self.pldccee['Hora'].astype(str).str.zfill(2)
        df_c = len(self.pldccee.columns)
        df_r = len(self.pldccee)
        pld_list = []
        
        def val():
            for c in range(2,df_c):
                for r in range(df_r):
                    sub = self.pldccee.iloc[r][1]
                    day = self.pldccee.columns[c]
                    day_right = []
                    for d in day.split('/'):
                        if d.isdigit() == True:
                            d = str(d).zfill(2)
                            day_right.append(d)
                    day = '{}/{}/{}'.format(day_right[0],day_right[1],day_right[2][2:])
                    hr = self.pldccee.iloc[r][0]
                    value = float(self.pldccee.iloc[r][c].replace(',','.'))
                    dict1 = {'DATA': day, 'SUBMERCADO': sub, hr: value}
                    pld_list.append(dict1)
            return()

        #check if data will be saved
        if save==False:
            val()
        elif not os.path.exists('pld_bd.csv'):
            val()

        else: #if data will be saved and there is values already get only de values that are new to the table
            df_bd = pd.read_csv('pld_bd.csv', delimiter= ';')
            df_bd['check'] = df_bd['DATA'] + df_bd['SUBMERCADO']
            
            for c in range(2,df_c):
                for r in range(df_r):
                    sub = self.pldccee.iloc[r][1]
                    day = self.pldccee.columns[c]
                    day_right = []
                    for d in day.split('/'):
                        if d.isdigit() == True:
                            d = str(d).zfill(2)
                            day_right.append(d)
                    day = '{}/{}/{}'.format(day_right[0],day_right[1],day_right[2][2:])
                    hr = self.pldccee.iloc[r][0]
                    value = float(self.pldccee.iloc[r][c].replace(',','.'))
                    if (day+sub) not in df_bd['check'].values:
                        dict1 = {'DATA': day, 'SUBMERCADO': sub, hr: value}
                        pld_list.append(dict1)

        df_newpld = pd.DataFrame(pld_list, columns=['DATA', 'SUBMERCADO', '00','01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23'])
        '''df_newpld['DATA'] = pd.to_datetime(df_newpld['DATA'],format= '%d/%m/%y')'''
        df_newpld = df_newpld.groupby(['DATA','SUBMERCADO']).first().reset_index()# removing the nan values
        df_newpld['MEDIA'] = df_newpld.iloc[:,2:].mean(axis=1)
        df_newpld['MEDIA'] = df_newpld['MEDIA'].round(2)
        df_newpld = df_newpld[['DATA', 'SUBMERCADO','MEDIA', '00','01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23']]

        def savetobd():
            if not os.path.exists('pld_bd.csv'):
                df_newpld.to_csv('pld_bd.csv',index=False, sep=';',date_format='%d/%m/%y')
            else:
                df_newpld.to_csv('pld_bd.csv',mode='a',index=False, sep=';',date_format='%d/%m/%y',header=False)
            return()
        savetobd()    
             
        return(df_newpld)


    




pld1 = pld('01/01/2022','25/01/2022')
pld1 = pld1.newschema(pld1.extract(),save=True)
print(pld1)
