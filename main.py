from pyspark.sql.functions import *
from pyspark.sql.window import Window 
import pyspark.sql.types as T
import pyspark.sql.functions as F
from datetime import timedelta
from pyspark.sql import SQLContext
import ibmos2spark, os
# @hidden_cell

if os.environ.get('RUNTIME_ENV_LOCATION_TYPE') == 'external':
    endpoint_5c01c72033524580912da0ce793726ea = 'https://s3-api.us-geo.objectstorage.softlayer.net'
else:
    endpoint_5c01c72033524580912da0ce793726ea = 'https://s3-api.us-geo.objectstorage.service.networklayer.com'

credentials = {
    'endpoint': endpoint_5c01c72033524580912da0ce793726ea,
    'service_id': 'iam-ServiceId-12277505-7a8f-463c-a6d7-031e9d06b129',
    'iam_service_endpoint': 'https://iam.cloud.ibm.com/oidc/token',
    'api_key': 'I02-7jFOaw_vQo5Yf8a5i0urmMuAHlLg_HawQOusr4I-'
}

configuration_name = 'os_5c01c72033524580912da0ce793726ea_configs'
cos = ibmos2spark.CloudObjectStorage(sc, credentials, configuration_name, 'bluemix_cos')

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window as W
spark = SparkSession.builder.getOrCreate()

class CatSellers:
    def __init__(self):
        self.camposCompra = ['idcompra','campanha',
                             'data','valortotalcomdesconto',
                             'valortotalcomdesconto_devido','flagativa',
                             'codigopromocao','infocomplcodigopromocao',
                             'idopcaoentregaexpressa','idcanalvenda',
                             'flag_cupom','flaggerapontos',
                             'flagaprovado','dataaprovacao',
                             'idbandeira']
        self.camposCompraEntrega = ['idcompraentrega','idcompra',
                                    'idfreteentregatipo','idcompraentregastatus',
                                    'datasaida','dataprevisao','dataentrega',
                                    'datastatus','prazoentregamaisdisponibilidade',
                                    'prazoestoque','dataemissaonotafiscal',
                                    'idtransportadora','dataentregacorrigida',
                                    'dataentregaagendada','dataprometidaoriginal',
                                    'idlojista','prazotransportadora',
                                    'sequencialcompra','datalimitesaidacd',
                                    'dataordemvendaerp','dataentregaajustada',
                                    'idbandeira']
        
        self.camposCompraEntregaSku = ['idcompraentrega','idsku',
                                       'valorvendaunidadesemdesconto','valorvendaunidade',
                                       'valorcupomnominal','valorvendaunidademenoscupomnominal',
                                       'valorfretecomdesconto','valorfrete','datacriacaoregistro',
                                       'valorfreteoriginalnaorentavel','prazogarantiafornecedor',
                                       'precoiofvalorvenda','valorcomissao','percentualComissao','idbandeira']
        self.camposLojista =['idlojista','classificacao',
                             'porcentagempositivo','quantidadereviews','estado',
                             'flagrestricaovendapj','idseller','idsubseller','idbandeira']
        
        self.camposSkuCategoria = ['idsku','iddepartamento','nomedepartamento',
                                   'idsetor','nomesetor']
        
        self.camposSkuLojista = ['idlojista','precoanterior','precovenda','idbandeira']
        self.exec()
        
    def readTable(self,table):
        return spark\
                .read\
                    .format('org.apache.spark.sql.execution.datasources.csv.CSVFileFormat')\
                        .option('header', 'true')\
                            .load(cos.url('{}.csv'.format(table), 'projetoviagrupo5-donotdelete-pr-dfpwbg3aqqhgo5'))    
    def joinned(self):
        # função criada para resolver relacionamento 
        # entre as tabelas e seleção dos campos que serão disponibilizados na estrutura
        #retorno de um dataframe
        compra = self\
            .readTable('compra')\
                .select(self.camposCompra)
        compraEntrega = self\
            .readTable('compraentrega')\
                .select(self.camposCompraEntrega) 
        compraEntregaSku = self\
            .readTable('compraentregasku')\
                .select(self.camposCompraEntregaSku)
        lojista = self\
            .readTable('lojista')\
                .select(self.camposLojista)
        skuCategoria = self\
            .readTable('skucategoria')\
                .select(self.camposSkuCategoria)
        compraEntregaSku = compraEntregaSku\
                            .join(skuCategoria,on='idsku',how='left')      
        joinned = compra\
                .join(compraEntrega,on=['idcompra','idbandeira'],how='inner')\
                 .join(compraEntregaSku,on=['idcompraentrega','idbandeira'],how='left')

        return joinned
    
    def filterRule(self,df):
        # funcão com finalidade de filtrar dado dos ultimos 6 meses e compras aprovadas
        df = df.filter(F.col('data') >= '20-07-2020')\
        .filter(F.col('flagaprovado') == 1)
        return df
    
    def Columns(self,df):
        #função para modelagem de campos de data
        #diferença entre data de aprovação e data de entrega
        #diferença entre data de aprovação e previsão
        # diferença entre recebimento e data prevista
        df = df.withColumn('dif_recebimento',F.datediff(F.col('dataentrega'),F.col('dataaprovacao')))\
               .withColumn('dif_previsao',F.datediff(F.col('dataprevisao'),F.col('dataaprovacao')))\
               .withColumn('difPrazo',(F.col('dif_recebimento') - F.col('dif_previsao')))
        return df
    
    def group(self,df):
        #agrupamento com soma de vendas
        #quantidade de vendas
        # média de diferença entre recebimento
        # média entre a diferença do prazo
        df = df\
            .groupBy('idlojista','iddepartamento')\
            .agg(
                F.sum('valortotalcomdesconto').alias('sumVenda'),
                F.count(F.lit(1)).alias('qntVenda'),
                F.mean('dif_recebimento').alias('media_recebimento'),
                F.mean('difPrazo').alias('media_prazo'))
        return df
    
    def medias(self,df):
        #função para média de recebimento
        #função para média de prazo
        #função para ticket médio de venda
        df = df.withColumn('media_recebimento',
                F.when(
                    F.col('media_recebimento').isNull(),0)\
                .otherwise(F.col('media_recebimento')))\
        .withColumn('media_prazo',F.when(F.col('media_prazo').isNull(),0).otherwise(F.col('media_prazo')))\
        .withColumn('ticketMedio',
                (F.col('sumVenda')/F.col('qntVenda')))\
        .orderBy(
            F.desc('media_recebimento'))\
        .withColumn('sumVenda',
                F.round("sumVenda", 2))\
        .withColumn('ticketMedio',
                F.round("ticketMedio", 2))
        return df
    
    def limiares(self,df):
        #função para resolução de limiares que separarão os percentis que classificarão os sellers
        df = df.groupBy('iddepartamento')\
        .agg(F.expr("percentile(qntVenda, 0.85)").alias('Venda50P'),
             F.expr("percentile(qntVenda, 0.93)").alias('Venda75P'),
             F.expr("percentile(qntVenda, 0.97)").alias('Venda95P'),
             F.expr("percentile(media_recebimento, 0.25)").alias('recebimento25P'),
             F.expr("percentile(media_recebimento, 0.50)").alias('recebimento50P'),
             F.expr("percentile(media_recebimento, 0.75)").alias('recebimento75P'),
             F.expr("percentile(media_prazo, 0.50)").alias('prazo25menores'),
             F.expr("percentile(media_prazo, 0.75)").alias('prazomeio'),
             F.expr("percentile(media_prazo, 0.95)").alias('prazo25maiores'))
        return df
    def classificador(self,df):
        df = df.withColumn('classVenda',
                       F.when(
                               (F.col('qntVenda') <= F.col('Venda50P')),'1'
                       )\
                        .when(
                               (F.col('qntVenda') > F.col('Venda50P')) & (F.col('sumVenda') <= F.col('Venda75P')),'2'
                       ).otherwise('3')
                   )\
        .withColumn('classEntrega',
                       F.when(
                               (F.col('media_prazo') <= F.col('prazo25menores')),'3'
                       )\
                        .when(
                               (F.col('media_prazo') <= F.col('prazo25maiores')) & (F.col('sumVenda') > F.col('prazomeio')),'2'
                       ).otherwise('1')
                   )\
        .withColumn('proxNiv',
                    F.when(F.col('classVenda') == '3',F.lit(0))\
                       .when(F.col('classVenda') == '2',(F.col('Venda75P') - F.col('qntVenda')))\
                            .otherwise(F.col('Venda50P') - F.col('qntVenda')))\
        .select('idlojista','iddepartamento','qntVenda','proxNiv','classVenda','media_recebimento','media_prazo','prazo25menores','prazomeio','prazo25maiores','classEntrega')\
        .filter(~F.col('iddepartamento').isNull())\
        .orderBy('iddepartamento')
        return df
    def exec(self):
        estrutura = self.joinned()
        estrutura.coalesce(1).write.option("header","true").csv(cos.url('{}'.format('estrutura'), 'projetoviagrupo5-donotdelete-pr-dfpwbg3aqqhgo5'))        
        estruturaFiltrada = self.filterRule(estrutura)
        estruturaDatasModeladas = self.Columns(estruturaFiltrada)
        estruturaAgrupada = self.group(estruturaDatasModeladas)
        estruturaMedias = self.medias(estruturaAgrupada)
        limiares = self.limiares(estruturaMedias)
        estruturaComLimiares = estruturaMedias.join(limiares,'iddepartamento',how='left')
        classificadorFinal = self.classificador(estruturaComLimiares)
        classificadorFinal.write.option("header","true").csv(cos.url('{}'.format('rank_v2'), 'projetoviagrupo5-donotdelete-pr-dfpwbg3aqqhgo5'))

CatSellers()
