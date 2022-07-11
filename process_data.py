import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import argparse 
from google.cloud import bigquery
from apache_beam.runners.runner import PipelineState

parser = argparse.ArgumentParser()

parser.add_argument('--input',
                        dest='input',
                        required=True,
                        help='Input Loan Applications file to process.')


# pipeline_args: Enviroment related arguments like runner type, job type, temporary files, etc
# path_args: inputs location
path_args, pipeline_args = parser.parse_known_args()

input_loanApplication = path_args.input
options = PipelineOptions(pipeline_args)

p = beam.Pipeline(options = options)

# Functions to be used on beam pipeline
def enrich_loan_data(element):
    loan_data = {}
    loan_data["COD_CLIENTE"] = element[0]
    loan_data["CODMES"] = element[1] + "0" + element[2] #YYYYMM
    loan_data["PLAZO"] = element[3]
    loan_data["MTO_SOLICITADO"] = 1000 if float(element[4]) < 1000 else float(element[4])
    loan_data["MONEDA"] = "USD" if element[5] != "USD" else element[5]
    loan_data["CANAL"] =  element[6]
    loan_data["SCORE"] = 400 if int(element[7]) < 400 else int(element[7])
    loan_data["SALARIO_BRUTO"] = 0 if (element[8] is None or element[8] == '')  else float(element[8])
    loan_data["TEA"] = float(element[9])
    loan_data["DEUDA_VIGENTE"] = 0 if (element[10] == '' or element[10] is None) else float(element[10])
    loan_data["SUBSEGMENTO"] = element[11]
    loan_data["NIVEL_EDUCACIONAL"] = element[12]
    
    return loan_data

# ear: efective annual rate
def fix_ear_assigned(element):
    tea = element["TEA"]
    if tea <= 0 or tea >= 0.339: 
        element["TEA"] = 0.339
    
    return element

cleaned_loan_data = (
    p
    | beam.io.ReadFromText(input_loanApplication, skip_header_lines=1)
    | beam.Map(lambda line: line.split(';'))
    | beam.Map(enrich_loan_data)
    | beam.Map(fix_ear_assigned)
)


# LOAD TO A BIGQUERY TABLE
client = bigquery.Client()
dataset_name = "loans_bronze"
dataset_id = '{}.{}'.format(client.project, dataset_name)

# CREATING DATASET 
try:
    client.get_dataset(dataset_id)
except:
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    dataset.description = "Dataset created from Python"
    dataset_ref = client.create_dataset(dataset, timeout=30)


# SETTING TABLE NAME AND SCHEMA
loan_table_spec = "{}:{}.loanApplications".format(client.project, dataset_name)
loan_table_schema = table_schema = {
                                    'fields': [
                                        {'name': 'COD_CLIENTE', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                                        {'name': 'CODMES', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                                        {'name': 'PLAZO', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                                        {'name': 'MTO_SOLICITADO', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                                        {'name': 'MONEDA', 'type': 'STRING', 'mode': 'NULLABLE'},
                                        {'name': 'CANAL', 'type': 'STRING', 'mode': 'NULLABLE'},
                                        {'name': 'SCORE', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                                        {'name': 'SALARIO_BRUTO', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                                        {'name': 'TEA', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                                        {'name': 'DEUDA_VIGENTE', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                                        {'name': 'SUBSEGMENTO', 'type': 'STRING', 'mode': 'NULLABLE'},
                                        {'name': 'NIVEL_EDUCACIONAL', 'type': 'STRING', 'mode': 'NULLABLE'}
                                    ]
                                }

(cleaned_loan_data
    | 'writing loans to bigquery' >> beam.io.WriteToBigQuery(
        loan_table_spec,
        schema=loan_table_schema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
    )
)

ret = p.run()
if ret.state == PipelineState.DONE:
    print("Success!!")
else:
    print("Error running beam pipeline")



