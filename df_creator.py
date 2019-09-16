from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
from datetime import datetime, timedelta
import time
import json

class DFCreator:

    def __init__(self,subscription_id,rg_name,df_name,client_id,secret,tenant,mapping,sap_source,ir_reference,sap_con_string,sql_con_string):
        self.subscription_id = subscription_id
        self.rg_name = rg_name
        self.df_name = df_name
        self.client_id = client_id
        self.secret = secret
        self.tenant = tenant
        self.mapping = mapping
        self.schema, self.source = sap_source.split('.')
        self.rg_params = {'location':'centralus'}
        self.df_params = {'location':'centralus'}
        self.ds_name = 'RelationalTable1'
        self.dsOut_name = 'sap_hana_db'
        self.ir_reference = ir_reference = 'integrationRuntime3'

        # '{"host":"example.com","port":30015,"user":"user","password":"secret"}'
        self.sap_con = json.loads(sap_con_string)
        self.sap_host = self.sap_con['host']
        self.sap_username = self.sap_con['user']
        self.sep_password = self.sap_con['password']
        self.sap_port = int(self.sap_con['port'])

        #'Server=tcp:<<fqdbservername>>;Database=<<dbname>>;Uid=<<username>>@<<dbservername>>;Pwd=<<password>>;Encrypt=yes;Connection Timeout=30;'
        self.sql_con = sql_con_string

        credentials = ServicePrincipalCredentials(client_id=self.client_id, secret=self.secret, tenant=self.tenant)
        resource_client = ResourceManagementClient(credentials, subscription_id)
        self.adf_client = DataFactoryManagementClient(credentials, subscription_id)

    def __str__(self):
        return f'subscription_id: {self.subscription_id}\nrg_name: {self.rg_name}\ndf_name: {self.df_name}\nclient_id: {self.client_id}\nsecret: {self.secret}\ntenant: {self.tenant}\n'

    def make_translator(self)->TabularTranslator:
        mappings = []
        for key in self.mapping:
            obj = {}
            obj['source'] = {"name":key}
            obj['sink'] = {"name":source[key]}
            mappings.append(obj)
        return TabularTranslator.from_dict({"type":"TabularTranslator","mappings":mappings})

    def create_linked_services(self):
        self.sap_ls_name = 'sapHanaLinkedService'
        ir_reference = IntegrationRuntimeReference(reference_name=self.ir_reference)
        sap_pass = SecureString(value=self.sap_password)
        ls_sap_hana = SapHanaLinkedService(server=self.sap_host,user_name=self.sap_username,password=sap_pass,authentication_type='Basic',connect_via=ir_reference)
        ls = adf_client.linked_services.create_or_update(self.rg_name, self.df_name, self.sap_ls_name, ls_sap_hana)

        self.sql_ls_name = 'sqlLinkedService'
        sql_string = SecureString(value=self.sql_con)
        ls_azure_sql = AzureSqlDatabaseLinkedService(connection_string=sql_string)
        sql_ls = adf_client.linked_services.create_or_update(self.rg_name, self.df_name, self.sql_ls_name, ls_azure_sql)

    def create_datasets(self):
        self.sap_ds_name = 'SAP_Dataset'
        ds_ls = LinkedServiceReference(reference_name=self.sap_ls_name)
        ds_sap_table = RelationalTableDataset(linked_service_name=ds_ls)
        ds = adf_client.datasets.create_or_update(self.rg_name, self.df_name, self.sap_ds_name, ds_sap_table)

        self.sql_ds_name = 'SQL_Dataset'
        dsOut_def = {
            "linkedServiceName": {
                "referenceName": f"{self.sql_ls_name}",
                "type": "LinkedServiceReference"
            },
            "parameters": {
                "TABLENAME": {
                    "type": "string"
                }
            },
            "type": "AzureSqlTable",
            "schema": [],
            "typeProperties": {
                "tableName": {
                    "value": "@dataset().TABLENAME",
                    "type": "Expression"
                }
            }
        }
        dsOut_azure_sql = AzureSqlTableDataset.from_dict(dsOut_def)
        dsOut = adf_client.datasets.create_or_update(self.rg_name, self.df_name, self.sql_ds_name, dsOut_azure_sql)

    def create_dataset_reference(self):
        self.dsin_ref = DatasetReference(self.sap_ds_name)
        self.dsOut_ref = DatasetReference(self.sql_ds_name,parameters={
            "TABLENAME":f"[dbo].[{self.source}]"
        })

    def create_copy_activity(self):
        source_type = RelationalSource(query=f'SELECT {",".join(self.mapping.keys())} from {self.schema}.{self.source}')
        destination_type = SqlSink()
        self.copy_activity = CopyActivity('S4Hana to SQL Copy',inputs=[self.dsin_ref], outputs=[self.dsOut_ref],source=source_type, sink=destination_type,translator=self.make_translator())
    
    def create_pipeline(self):
        self.create_linked_services()
        self.create_datesets()
        self.create_dataset_reference()
        self.create_copy_activity()
        p_name = f'replicate_{self.source}'
        p_obj = PipelineResource(activities=[self.copy_activity])
        p = adf_client.pipelines.create_or_update(self.rg_name, self.df_name, p_name, p_obj)
