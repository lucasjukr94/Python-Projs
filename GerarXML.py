#classes NFe
class FazendaNfe:
    StatusChave='';
    ChaveNfe='';
    VersaoNfe='';
    ModeloNfe='';
    SerieNfe='';
    NumeroNfe='';
    #...
        
class FazendaNfeCobranca:
    FazendaNfeId='';
    TipoCobranca='';
    Numero='';
    Vencimento='';
    Valor='';

class FazendaNfeProduto:
    FazendaNfeId='';
    NumeroProduto='';
    DescricaoProduto='';
    QuantidadeProduto='';
    UnidadeComercialProduto='';
    #...

class FazendaNfeVolume:
    FazendaNfeId='';
    Volume='';
    Quantidade='';
    Especie='';
    MarcaVolumes='';
    Numeracao='';
    PesoLiquido='';
    PesoBruto='';

class FazendaNfeEvento:
    FazendaNfeId='';
    AutorizacaoUso='';
    Protocolo='';
    DataHoraAutorizacao='';
    DataAutorizacao='';
    DataHoraInclusaoAn='';
    DataInclusaoAn='';
    CteAutorizado='';
    MdfeAutorizado='';

class FazendaNfeInformacoesCompra:
    FazendaNfeId='';
    NotaEmpenho='';
    Pedido='';
    Contrato='';

#Script/easy_install pip
#pip install pyodbc
import pyodbc

#Fornecer os arquivos csv a serem lidos(devem estar no formato certo)
listaArquivosCSV = list(str(input("Digite o nome dos arquivos separando-os com ';' :\n")).split(';'));
while('' in listaArquivosCSV):
    listaArquivosCSV.remove('');
    
#Leitura dos dados dos arquivos csv
i=0;
while(i<len(listaArquivosCSV)):
    #parse CSV para Matriz
    print("Convertendo o arquivo CSV para a matriz");
    f = open(listaArquivosCSV[i],errors='ignore');
    csvMatrix = [];
    j=0;
    #Iniciando a leitura dos arquivos
    print("Iniciando a leitura do arquivo");
    print("Lendo o arquivo "+listaArquivosCSV[i]);
    while(f.readline()):
        try:
            vecData = f.readline().split(';');
            csvMatrix.append(vecData);
        except:
            print("Erro na linha "+str(j)+" do arquivo "+listaArquivosCSV[i]);
        j+=1;
    
    #Conexão com o banco de dados
    print("Conectando com o banco de dados");
    connectionString = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                                      "Server=LOTESTX8\SQL2012;"
                                      "Database=Darwin;"
                                      "Trusted_Connection=yes;");
    cursor = connectionString.cursor();
    
    #Buscando chaves das NFe's
    j=0;
    while(j<len(csvMatrix)):
        k=0;
        while(k<len(csvMatrix[j])):
            print("Chave:"+csvMatrix[j][0]);
            
            #Instanciando as classes
            print("Instanciando as classes");
            fazendaNfe = FazendaNfe();
            fazendaNfeProduto = FazendaNfeProduto();
            fazendaNfeVolume = FazendaNfeVolume();
            
            listaFazendaNfeCobranca = [];#append(FazendaNfe())
            listaFazendaNfeEvento = [];#append(FazendaNfeEvento())
            listaFazendaNfeInformacoesCompra = [];#append(FazendaNfeInformacoesCompra)

            #TODO: Melhorar esse codigo
            
            #Buscando Nfe
            #Fazenda Nfe
            print("Buscando a Fazenda Nfe");
            cursor.execute("select top 1 * from LotusIntegracao.dbo.tbl_fazenda_nfe where 1=1 and ChaveNfe = '"+csvMatrix[j][0]+"'");
            dbDataVec = [];
            for row in cursor:
                dbDataVec = list(str(row).split(','));
            fazendaNfe.StatusChave = dbDataVec[0];
            fazendaNfe.ChaveNfe = dbDataVec[1];
            fazendaNfe.VersaoNfe = dbDataVec[2];
            fazendaNfe.ModeloNfe = dbDataVec[3];
            #...
                
            #Fazenda Nfe Produto
            print("Buscando a Fazenda Nfe Produto");
            cursor.execute("select * from LotusIntegracao.dbo.tbl_fazenda_nfe_produto p left join LotusIntegracao.dbo.tbl_fazenda_nfe f on f.Id = p.FazendaNfeId where 1=1 and f.ChaveNfe = '"+csvMatrix[j][0]+"'");
            dbDataVec = [];
            for row in cursor:
                dbDataVec = list(str(row).split(','));
            fazendaNfeProduto.FazendaNfeId = dbDataVec[0];
            fazendaNfeProduto.NumeroProduto = dbDataVec[1];
            fazendaNfeProduto.DescricaoProduto = dbDataVec[2];
            fazendaNfeProduto.QuantidadeProduto = dbDataVec[3];
            fazendaNfeProduto.UnidadeComercialProduto = dbDataVec[4];

            #Fazenda Nfe Volume
            print("Buscando a Fazenda Nfe Volume");
            cursor.execute("select top 1 v.* from LotusIntegracao.dbo.tbl_fazenda_nfe_volume v left join LotusIntegracao.dbo.tbl_fazenda_nfe f on f.Id = v.FazendaNfeId where 1=1 and f.ChaveNfe = '"+csvMatrix[j][0]+"'");
            dbDataVec = [];
            for row in cursor:
                dbDataVec = list(str(row).split(','));
            fazendaNfeVolume.FazendaNfeId = dbDataVec[4];
            fazendaNfeVolume.Volume = dbDataVec[4];
            fazendaNfeVolume.Quantidade = dbDataVec[4];
            fazendaNfeVolume.Especie = dbDataVec[4];
            fazendaNfeVolume.MarcaVolumes = dbDataVec[4];
            fazendaNfeVolume.Numeracao = dbDataVec[4];
            fazendaNfeVolume.PesoLiquido = dbDataVec[4];
            fazendaNfeVolume.PesoBruto = dbDataVec[4];

            #Fazenda Nfe Cobranca
            print("Buscando a Fazenda Nfe Cobranca");
            cursor.execute("select c.* from LotusIntegracao.dbo.tbl_fazenda_nfe_cobranca c left join LotusIntegracao.dbo.tbl_fazenda_nfe f on f.Id = c.FazendaNfeId where 1=1 and f.ChaveNfe = '"+csvMatrix[j][0]+"'");
            dbMatrix = [];
            for row in cursor:
                dbMatrix.append(list(str(row).split(',')));
            p=0;
            while(p<len(dbMatrix)):
                listaFazendaNfeCobranca.FazendaNfeId = dbmatrix[p][0];
                listaFazendaNfeCobranca.TipoCobranca = dbmatrix[p][1];
                listaFazendaNfeCobranca.Numero = dbmatrix[p][2];
                listaFazendaNfeCobranca.Vencimento = dbmatrix[p][3];
                listaFazendaNfeCobranca.Valor = dbmatrix[p][4];
                p+=1;

            #Fazenda Nfe Evento
            print("Buscando a Fazenda Nfe Evento");
            cursor.execute("select e.* from LotusIntegracao.dbo.tbl_fazenda_nfe_evento e left join LotusIntegracao.dbo.tbl_fazenda_nfe f on f.Id = e.FazendaNfeId where 1=1 and f.ChaveNfe = '"+csvMatrix[j][0]+"'");
            dbMatrix = [];
            for row in cursor:
                dbMatrix.append(list(str(row).split(',')));
            p=0;
            while(p<len(dbMatrix)):
                listaFazendaNfeEvento.FazendaNfeId = dbmatrix[p][0];
                listaFazendaNfeEvento.AutorizacaoUso = dbmatrix[p][1];
                listaFazendaNfeEvento.Protocolo = dbmatrix[p][2];
                listaFazendaNfeEvento.DataHoraAutorizacao = dbmatrix[p][3];
                listaFazendaNfeEvento.DataAutorizacao = dbmatrix[p][4];
                listaFazendaNfeEvento.DataHoraInclusaoAn = dbmatrix[p][5];
                listaFazendaNfeEvento.DataInclusaoAn = dbmatrix[p][6];
                listaFazendaNfeEvento.CteAutorizado = dbmatrix[p][7];
                listaFazendaNfeEvento.MdfeAutorizado = dbmatrix[p][8];
                p+=1;
            
            #Fazenda Nfe Informacoes Compra
            print("Buscando a Fazenda Nfe Compra");
            cursor.execute("select i.* from LotusIntegracao.dbo.tbl_fazenda_nfe_informacoescompra i left join LotusIntegracao.dbo.tbl_fazenda_nfe f on f.Id = i.FazendaNfeId where 1=1 and f.ChaveNfe = '"+csvMatrix[j][0]+"'");
            dbMatrix = [];
            for row in cursor:
                dbMatrix.append(list(str(row).split(',')));
            p=0;
            while(p<len(dbMatrix)):
                listaFazendaNfeInformacoesCompra.FazendaNfeId = dbmatrix[p][0];
                listaFazendaNfeInformacoesCompra.NotaEmpenho = dbmatrix[p][1];
                listaFazendaNfeInformacoesCompra.Pedido = dbmatrix[p][2];
                listaFazendaNfeInformacoesCompra.Contrato = dbmatrix[p][3];
                p+=1;

            #Gerando arquivo XML
            print("Gerando o arquivo XML");
    
            k+=1;
        #Gerando arquivo zipado das NFe's
        print("Zipando os arquivos XML");
        j+=1;
    i+=1;


