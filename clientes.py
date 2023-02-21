import apache_beam as beam
import os
from apache_beam.options.pipeline_options import PipelineOptions

# Fazendo a conexão com o google cloud
serviceAccount = r'teste-dataflow-beam-5fd7f05c5f1f.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= serviceAccount


# configuração de Cluster, minimo a ser declarado
pipeline_options = {
    'project': 'teste-dataflow-beam' ,
    'runner': 'DataflowRunner',
    'region': 'southamerica-east1',
    'staging_location': 'gs://teste-dataflow-beam/temp',
    'temp_location': 'gs://teste-dataflow-beam/temp',
    'template_location': 'gs://teste-dataflow-beam/template/batch_job_clientes' }
    
pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
p1 = beam.Pipeline(options=pipeline_options)



def texto_para_lista(elemento, delimitador=','):
    """
    Recebe um texto e um delimitador
    Retorna uma lista de elementos pelo delimitador
    """
    return elemento.split(delimitador)


##Pardo processamento parelalo
class tira_espaco(beam.DoFn):
  def process(self,elemento):
        """
        Recebe um texto com espaços no inicio e fim ' Felipe ' 
        Retorna um texto sem os espaços extras 'felipe'
        """
        nome, sobrenome, cpf, nascimento, telefone, email, cep, rua, bairro, cidade, estado = elemento
        nome = nome.strip()
        sobrenome = sobrenome.strip()
        return [(nome, sobrenome, cpf, nascimento, telefone, email, cep, rua, bairro, cidade, estado)]



def trata_data(elemento):
    """
    Recebe uma data na formatação '19991205'
    Retorna uma data na formatação '1999-12-25'
    """
    nome, sobrenome, cpf, nascimento, telefone, email, cep, rua, bairro, cidade, estado = elemento
    nascimento = nascimento[:4] + '-' + nascimento[4:6] + '-' + nascimento[6:]
    return [nome, sobrenome, cpf, nascimento, telefone, email, cep, rua, bairro, cidade, estado]



def cria_dicionario(elemento):
    dicionario = {} 
    dicionario['nome'] = elemento[0]
    dicionario['sobrenome'] = elemento[1]
    dicionario['cpf'] = elemento[2]
    dicionario['nascimento'] = elemento[3]
    dicionario['telefone'] = elemento[4]
    dicionario['email'] = elemento[5]
    dicionario['cep'] = elemento[6]
    dicionario['rua'] = elemento[7]
    dicionario['bairro'] = elemento[8]
    dicionario['cidade'] = elemento[9]
    dicionario['estado'] = elemento[10]
 
    return(dicionario)



table_schema = 'nome:STRING, sobrenome:STRING, cpf:STRING, nascimento:STRING, telefone:STRING, email:STRING, cep:STRING, rua:STRING, bairro:STRING, cidade:STRING, estado:STRING'
tabela = 'teste-dataflow-beam.clientes_dataflow.cliente_dataflow'


cliente = (
   p1
   | "Leitura do dataset de clientes" >> beam.io.ReadFromText(r"gs://teste-dataflow-beam/entrada/clientes.csv", skip_header_lines = 1)
   | "transforma texto para lista" >> beam.Map(texto_para_lista)
   | "Tira espaços extras" >>  beam.ParDo(tira_espaco())
   | "Trata data de nascimento" >> beam.Map(trata_data)
   | "Transformando em dicionario" >> beam.Map(cria_dicionario)
   | beam.io.WriteToBigQuery(
                              tabela,
                              schema=table_schema,
                              write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                              create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                              custom_gcs_temp_location = 'gs://teste-dataflow-beam/temp' )

)


p1.run()