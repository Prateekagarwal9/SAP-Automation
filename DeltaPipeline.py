import pandas as pd
from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *

class DeltaPipline:
    def __init__(self):
        self.schema = '_SYS_BIC'
        self.source = 'Temp/VBAP_DEMO'
        self.source_type = 'VIEW'
        self.sap_con = '{"host" : "40.87.84.72","port" : 30215,"user" : "system","password" : "Metro#123"}'
        self.sql_con = 'Server=tcp:yashtesting.database.windows.net;Database=test;Uid=yash@yashtesting;Pwd=Myageis@20;Encrypt=yes;Connection Timeout=30;'
        self.sql_odbc_con = 'Driver={ODBC Driver 17 for SQL Server};'+self.sql_con
        self.subscription_id = '938ace66-9598-4029-b6bb-429929b03761'
        self.rg_name = 'celebal_rnd'
        self.df_name = 'celebaladf'
        self.client_id = 'b628371b-654f-4848-b214-c8553f2fc665'
        self.secret = '/JCA4now2LAn1/L4aa+ICfmTumPRryW.'
        self.tenant = 'e4e34038-ea1f-4882-b6e8-ccd776459ca0'
        self.rg_params = {'location': 'eastus'}
        self.df_params = {'location': 'eastus'}
        self.credentials = ServicePrincipalCredentials(
            client_id=self.client_id, secret=self.secret, tenant=self.tenant)
        self.resource_client = ResourceManagementClient(
            self.credentials, self.subscription_id)
        self.adf_client = DataFactoryManagementClient(
            self.credentials, self.subscription_id)
        self.blob_dataset = 'AzureBlob1'
        self.input_dataset = 'RelationalTable2'
        self.output_dataset = 'AzureSqlTable3'
        self.staging_ls = 'LS_Sap_Hana'
        self.staging_path = 'testazure'
        self.creation_date = "ERDAT"
        self.change_date = "AEDAT"
        self.timestamp_staging_table = '[dbo].[Sap_hana_db_timestamp_staging]'
        self.timestamp_table = '[dbo].[Sap_hana_db_timestamp]'
        self.mapping = {
            "VBELN": "SalesDocument",
            "POSNR": "SalesDocumentItem",
            "MATNR": "Material",
            "MATKL": "MaterialGroup",
            "PSTYV": "SalesDocumentItemCat",
            "FKREL": "RelevantforBilling",
            "NETWR": "NetValue",
            "WAERK": "Currency",
            "KWMENG": "OrderQuantity",
            "LSMENG": "RequiredDelQuantity",
            "KBMENG": "ConfirmedDelQuantiy",
            "WERKS": "Plant",
            "PRCTR": "ProfitCenter",
            "ABSTA": "RejectionStatus",
            "GBSTA": "OverallStatus",
            "LFSTA": "DeliveryStatus",
            "ERDAT": "CreatedDate",
            "WAVWR": "Cost",
            "AEDAT": "UpdateDate"
        }
        self.translator = self.make_translator()

    def make_translator(self) -> TabularTranslator:
        source = self.mapping
        mappings = []
        for key in source:
            obj = {}
            obj['source'] = {"name": key}
            obj['sink'] = {"name": source[key]}
            mappings.append(obj)
        return TabularTranslator.from_dict({"type": "TabularTranslator", "mappings": mappings})

    def create_insert_new_records(self):
        template = {
            "name": "INSERT NEW RECORDS",
            "type": "Copy",
            "policy": {
                "timeout": "7.00:00:00",
                "retry": 0,
                "retryIntervalInSeconds": 30,
                "secureOutput": False,
                "secureInput": False
            },
            "typeProperties": {
                "source": {
                    "type": "RelationalSource",
                    "query": {
                        "value": f'SELECT {",".join(self.mapping.keys())}  FROM "{self.schema}"."{self.source}" WHERE {self.creation_date} > @{{item().MAXINSERTIONDATE}} and {self.change_date} = 00000000',
                        "type": "Expression"
                    }
                },
                "sink": {
                    "type": "SqlSink"
                },
                "enableStaging": True,
                "stagingSettings": {
                    "linkedServiceName": {
                        "referenceName": self.staging_ls,
                        "type": "LinkedServiceReference"
                    },
                    "path": self.staging_path
                },
                "translator": self.translator
            },
            "inputs": [
                {
                    "referenceName": self.input_dataset,
                    "type": "DatasetReference"
                }
            ],
            "outputs": [
                {
                    "referenceName": self.output_dataset,
                    "type": "DatasetReference",
                    "parameters": {
                        "SQLTABLENAME": self.timestamp_table
                    }
                }
            ]
        }
        return CopyActivity.from_dict(template)

    def create_insert_updated_records_in_staging(self):
        template = {
            "name": "INSERT UPDATED RECORDS IN STAGING",
            "type": "Copy",
            "dependsOn": [
                {
                    "activity": "INSERT NEW RECORDS",
                    "dependencyConditions": [
                        "Succeeded"
                    ]
                }
            ],
            "policy": {
                "timeout": "7.00:00:00",
                "retry": 0,
                "retryIntervalInSeconds": 30,
                "secureOutput": False,
                "secureInput": False
            },
            "typeProperties": {
                "source": {
                    "type": "RelationalSource",
                    "query": {
                        "value": f'SELECT {",".join(self.mapping.keys())}  FROM "{self.schema}"."{self.source}" WHERE {self.change_date} > @{{item().MAXUPDATEDDATE}}',
                        "type": "Expression"
                    }
                },
                "sink": {
                    "type": "SqlSink",
                    "preCopyScript": f"TRUNCATE TABLE {self.timestamp_staging_table}"
                },
                "enableStaging": True,
                "stagingSettings": {
                    "linkedServiceName": {
                        "referenceName": self.staging_ls,
                        "type": "LinkedServiceReference"
                    },
                    "path": self.staging_path
                },
                "translator": self.translator
            },
            "inputs": [
                {
                    "referenceName": self.input_dataset,
                    "type": "DatasetReference"
                }
            ],
            "outputs": [
                {
                    "referenceName": self.output_dataset,
                    "type": "DatasetReference",
                    "parameters": {
                        "SQLTABLENAME": self.timestamp_staging_table
                    }
                }
            ]
        }
        return CopyActivity.from_dict(template)

    def create_insert_updated_records_to_main_table(self):
        template = {
            "name": "INSERT UPDATED RECORDS TO MAIN TABLE",
            "type": "Copy",
            "dependsOn": [
                {
                    "activity": "INSERT UPDATED RECORDS IN STAGING",
                    "dependencyConditions": [
                        "Succeeded"
                    ]
                }
            ],
            "policy": {
                "timeout": "7.00:00:00",
                "retry": 0,
                "retryIntervalInSeconds": 30,
                "secureOutput": False,
                "secureInput": False
            },
            "typeProperties": {
                "source": {
                    "type": "SqlSource"
                },
                "sink": {
                    "type": "SqlSink",
                    "preCopyScript": f"DELETE FROM {self.timestamp_table} WHERE salesdocument IN ( SELECT salesdocument  FROM {self.timestamp_staging_table} )"
                },
                "enableStaging": True,
                "stagingSettings": {
                    "linkedServiceName": {
                        "referenceName": self.staging_ls,
                        "type": "LinkedServiceReference"
                    },
                    "path": self.staging_path
                }
            },
            "inputs": [
                {
                    "referenceName": self.output_dataset,
                    "type": "DatasetReference",
                    "parameters": {
                        "SQLTABLENAME": self.timestamp_staging_table
                    }
                }
            ],
            "outputs": [
                {
                    "referenceName": self.output_dataset,
                    "type": "DatasetReference",
                    "parameters": {
                        "SQLTABLENAME": self.timestamp_table
                    }
                }
            ]
        }
        return CopyActivity.from_dict(template)

    def create_date_lookup_activity(self):
        template = {
            "name": "GET LATEST DATE",
            "type": "Lookup",
            "policy": {
                "timeout": "7.00:00:00",
                "retry": 0,
                "retryIntervalInSeconds": 30,
                "secureOutput": False,
                "secureInput": False
            },
            "typeProperties": {
                "source": {
                    "type": "BlobSource",
                    "recursive": True
                },
                "dataset": {
                    "referenceName": self.blob_dataset,
                    "type": "DatasetReference"
                }
            }
        }
        return LookupActivity.from_dict(template)

    def create_copy_data_using_maxdate(self):
        a1 = self.create_insert_new_records()
        a2 = self.create_insert_updated_records_in_staging()
        a3 = self.create_insert_updated_records_to_main_table()
        template = {
            "name": "copy data using max date",
            "type": "ForEach",
            "dependsOn": [
                {
                    "activity": "GET LATEST DATE",
                    "dependencyConditions": [
                        "Succeeded"
                    ]
                }
            ],
            "typeProperties": {
                "items": {
                    "value": "@activity('GET LATEST DATE').output.value",
                    "type": "Expression"
                },
                "activities": [a1, a2, a3]
            }
        }
        return ForEachActivity.from_dict(template)

    def create_copy_data_activity(self):
        template = {
            "name": "Copy Data1",
            "type": "Copy",
            "dependsOn": [
                {
                    "activity": "copy data using max date",
                    "dependencyConditions": [
                        "Succeeded"
                    ]
                }
            ],
            "policy": {
                "timeout": "7.00:00:00",
                "retry": 0,
                "retryIntervalInSeconds": 30,
                "secureOutput": False,
                "secureInput": False
            },
            "typeProperties": {
                "source": {
                    "type": "RelationalSource",
                    "query": f'SELECT MAX("{self.creation_date}") AS MAXINSERTIONDATE, MAX("{self.change_date}") AS MAXUPDATEDATE FROM "{self.schema}"."{self.source}"'
                },
                "sink": {
                    "type": "BlobSink"
                },
                "enableStaging": False
            },
            "inputs": [
                {
                    "referenceName": self.input_dataset,
                    "type": "DatasetReference"
                }
            ],
            "outputs": [
                {
                    "referenceName": self.blob_dataset,
                    "type": "DatasetReference"
                }
            ]
        }
        return CopyActivity.from_dict(template)

    def create_pipeline(self, name="YashDeltaPipeline"):
        p_name = name
        a1 = self.create_date_lookup_activity()
        a2 = self.create_copy_data_using_maxdate()
        a3 = self.create_copy_data_activity()
        p_obj = PipelineResource(activities=[a1, a2, a3])
        p = self.adf_client.pipelines.create_or_update(
            self.rg_name, self.df_name, p_name, p_obj)