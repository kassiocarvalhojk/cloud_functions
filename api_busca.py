import functions_framework
import json
import pandas as pd
from datetime import datetime
import pytz
from google.cloud import bigquery
from google.cloud import storage
import os

# VARIAVEIS UTEIS
client = bigquery.Client()
procedure = os.environ.get('SUA_VARIAVEL_DE_AMBIENTE') # variavel de ambiente cadastrada com uma procedure, mas pode ser usada uma query normal


# FUNÇÃO QUE CHAMA PROCEDURE
def call_procedure(sua_variavel1,suavariavel2):

    print("variavel da busca recebido: ",sua_variavel1)
    print("variavel de busca recebida: ",suavariavel2)
    query = f"CALL `{procedure}`('{sua_variavel1}','{suavariavel2}');"
    print(f"Query executada: {query}")
    result = client.query(query).result()
    

    for line in result:
        var1 = line["coluna1"]
        var2 = line["coluna2"]
        var3 = line["coluna3"]
        var4 = line["coluna4"]
        var5 = line["coluna5"]

        result_final = {
        "coluna1":var1,
        "coluna2":var2,
        "coluna3":var3,
        "coluna4":f"{var4}",
        "coluna5":f"{var5}"
    }
    
    result_final = json.dumps(result_final,default=str)
    print(f"Json de retorno da procedure {procedure} gerado: ", result_final)

    return  f"{result_final}"


# FUNÇÃO QUE RECEBE O REQUEST DO METODO POST DO HTTPS
@functions_framework.http
def request_get(request):
    reqBytesToString = bytes.decode(request.data)
    jsonDict = json.loads(reqBytesToString)
    sua_variavel1 = jsonDict["request1"]
    suavariavel2 = jsonDict["request2"]
    final_result = call_procedure(sua_variavel1,suavariavel2)
    return final_result
    
