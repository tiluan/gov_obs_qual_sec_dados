# Projeto: Governança de Dados com Docker e AWS

## Descrição
Este projeto implementa uma pipeline de dados para garantir a governança e automação de processos. Ele abrange etapas que incluem geração de dados de amostra, upload para S3, validação de qualidade, observabilidade, enriquecimento com novas informações e mascaramento de dados sensíveis para conformidade com regulações.

## Requisitos
- Docker: Para conteinerização do ambiente.
- Conta AWS configurada: Necessária para operações de armazenamento em nuvem.
- Python 3.x: Linguagem base para execução dos scripts.
- Bibliotecas Python:
  - `pandas`: Manipulação e análise de dados.
  - `boto3`: Integração com AWS.
  - `logging`: Registro estruturado de eventos.
  - `great_expectations`: Validação de dados.

## Configuração do Ambiente

1. **Construir a imagem Docker**
   ```bash
   docker build -t image_prj_governanca .
   ```

2. **Criar e iniciar o container**
   ```bash
   docker run -dit --name prj_gov -v c:\Users\{seu_user}\{seu_projeto}/modulos:/prjgov image_prj_governanca
   ```

3. **Configurar credenciais AWS dentro do container**
   - Gere uma chave de acesso AWS seguindo as etapas abaixo:
     1. Acesse sua conta AWS.
     2. Navegue até "Credenciais de Segurança" em seu perfil de usuário.
     3. Crie uma nova chave de acesso e salve-a em local seguro.

   - Configure as credenciais no ambiente do container:
     1. Abra o terminal do container.
     2. Execute o seguinte comando:
        ```bash
        aws configure
        ```
     3. Forneça as credenciais geradas.

## Execução dos Notebooks

Para executar os notebooks dentro do container, utilize o seguinte comando:
```bash
python {nome_do_arquivo}.py
```

## Pipeline de Dados

### Passos para Execução Completa da Pipeline

1. **Tornar o script executável**
   ```bash
   chmod +x pipeline_load_full.sh
   ```

2. **Executar o script de automação**
   ```bash
   ./pipeline_load_full.sh
   ```

### Estrutura da Pipeline

1. **Geração de dados**: `01-data-generate.py`
   - Responsável por gerar dados de amostra, introduzindo variações para fins de teste.

2. **Upload para S3**: `02-upload_file_s3.py`
   - Realiza o envio dos arquivos gerados para o bucket S3 especificado, garantindo a integridade e segurança.

3. **Observabilidade**: `03_observability.py`
   - Calcula métricas chave e gera relatórios para monitoramento detalhado da qualidade dos dados.

4. **Validação de dados brutos**: `04_validates_raw_data_quality.py`
   - Aplica validações estruturadas para assegurar a consistência inicial dos dados.

5. **Aplicação de qualidade**: `05_quality_apply.py`
   - Executa correções e tratamentos específicos nos dados com base em regras predefinidas.

6. **Validação de dados limpos**: `06_validates_clean_data_quality.py`
   - Garante que os dados tratados estejam conformes e aptos para uso.

7. **Enriquecimento de dados**: `07_enrichment.py`
   - Complementa os dados com atributos adicionais, como categorizações salariais.

8. **Mascaramento de dados**: `08_security.py`
   - Implementa medidas de privacidade e anonimização antes da publicação.

## Estrutura do Repositório
- `Dockerfile`: Contém as instruções para criação da imagem Docker.
- `01-data-generate.py`: Script para geração de dados de amostra.
- `02-upload_file_s3.py`: Script para upload dos dados no S3.
- `03_observability.py`: Realiza análise de observabilidade.
- `04_validates_raw_data_quality.py`: Valida a qualidade dos dados brutos.
- `05_quality_apply.py`: Aplica correções e validações nos dados tratados.
- `06_validates_clean_data_quality.py`: Revalidação e relatórios de qualidade.
- `07_enrichment.py`: Adiciona atributos complementares aos dados.
- `08_security.py`: Realiza mascaramento e anonimização.
- `pipeline_load_full.sh`: Orquestra a execução de todos os scripts na sequência correta.

## Logs e Monitoramento
Os eventos de execução serão registrados em `pipeline_execution.log`.

Utilize este arquivo para rastrear operações, identificar erros e realizar depuração.
