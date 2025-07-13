# 📊 Notebooks Baliza - Análise de Dados PNCP

Esta pasta contém notebooks Jupyter prontos para análise dos dados PNCP preservados no Internet Archive pelo projeto Baliza.

## 🚀 Notebook Principal

### 📈 [Análise PNCP no Google Colab](analise_pncp_colab.ipynb)

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/franklinbaldo/baliza/blob/main/notebooks/analise_pncp_colab.ipynb)

**Um clique e você está analisando milhões de contratos públicos!**

### 🎯 O que o notebook oferece:

#### ✅ **Acesso Direto aos Dados**
- 🌐 Conecta automaticamente ao Internet Archive
- 📦 Carrega dados PNCP preservados pelo Baliza
- 🔄 Sempre atualizado com os dados mais recentes
- 💾 Sem necessidade de download ou configuração local

#### ✅ **Análises Pré-Configuradas**
- 📈 **Análise Temporal**: Evolução das contratações
- 🗺️ **Análise Geográfica**: Distribuição por estados
- 🏢 **Análise de Fornecedores**: Concentração e padrões
- 💰 **Análise de Valores**: Distribuição e outliers
- 🚨 **Detecção de Fraudes**: Algoritmos de anomalias

#### ✅ **Visualizações Interativas**
- 📊 Gráficos Plotly interativos
- 🗺️ Mapas coropléticos
- 📈 Dashboards dinâmicos
- 🔍 Filtros e drill-downs

#### ✅ **Funcionalidades Avançadas**
- 🧹 Limpeza automática de dados
- 🔍 Detecção de padrões suspeitos
- 📉 Análise estatística descritiva
- 💾 Exportação para CSV
- 📋 Relatórios automatizados

## 🎓 Para Pesquisadores e Analistas

### 👥 **Público-Alvo**
- 🏛️ Pesquisadores em transparência pública
- 📊 Analistas de dados governamentais
- 🎓 Estudantes de administração pública
- 📰 Jornalistas investigativos
- 🔍 Auditores e controladores

### 📚 **Casos de Uso**
- 🔬 Pesquisa acadêmica em contratações públicas
- 📊 Análise de transparência governamental
- 🚨 Investigação de irregularidades
- 📈 Estudos de eficiência de compras públicas
- 🗺️ Análises regionais e comparativas

### 📖 **Como Usar**

1. **📱 Acesso Rápido**: Clique no botão "Open in Colab" acima
2. **▶️ Execute as Células**: Rode célula por célula ou "Runtime > Run All"
3. **🔧 Personalize**: Modifique filtros e parâmetros conforme necessário
4. **💾 Exporte**: Baixe os resultados em CSV para uso posterior
5. **🤝 Compartilhe**: Compartilhe descobertas com a comunidade

### 💡 **Dicas de Uso**

#### 🎯 **Para Iniciantes**
- Execute o notebook completo primeiro para ver todas as análises
- Use os dados de exemplo se não conseguir acessar o IA imediatamente
- Leia os comentários para entender cada análise

#### 🚀 **Para Usuários Avançados**
- Modifique os parâmetros de `load_sample_data()` para datasets maiores
- Adicione novas análises usando as funções existentes como base
- Integre com suas próprias bases de dados complementares

#### 🔧 **Personalização**
```python
# Exemplo: Focar apenas em contratos de alto valor
df_filtered = df_clean[df_clean['valor_contrato'] > 1_000_000]

# Exemplo: Análise de um estado específico  
df_ro = df_clean[df_clean['uf'] == 'RO']

# Exemplo: Período específico
df_2024 = df_clean[df_clean['ano'] == 2024]
```

## 🔍 **Análises Disponíveis**

### 📊 **1. Análise Exploratória**
- Estatísticas descritivas
- Distribuições de valores
- Qualidade dos dados
- Campos disponíveis

### 📅 **2. Análise Temporal**
- Evolução mensal/anual
- Sazonalidade
- Tendências de crescimento
- Picos e vales

### 🗺️ **3. Análise Geográfica**
- Distribuição por UF
- Concentração regional
- Mapas coropléticos
- Rankings estaduais

### 🏢 **4. Análise de Fornecedores**
- Top fornecedores por valor
- Concentração de mercado
- Índice HHI
- Fornecedores frequentes

### 💰 **5. Análise de Valores**
- Distribuição de valores
- Percentis e outliers
- Categorização automática
- Contratos de alto valor

### 🚨 **6. Detecção de Anomalias**
- Valores suspeitos
- Padrões temporais anômalos
- Fornecedores com alta frequência
- Contratos em fins de semana

## 📈 **Resultados Esperados**

### 📊 **Outputs Típicos**
- 20+ gráficos interativos
- 5+ tabelas de análise
- Arquivos CSV para download
- Relatório de suspeições
- Métricas de concentração

### 🎯 **Insights Comuns**
- Sazonalidade no final do ano fiscal
- Concentração em poucos fornecedores grandes
- Variação regional significativa
- Padrões de valores redondos
- Contratos de emergência frequentes

## 🛠️ **Requisitos Técnicos**

### ✅ **Funciona 100% no Google Colab**
- Python 3.7+
- Pandas, Plotly, DuckDB
- Conexão com Internet
- Navegador moderno

### 🚫 **Não Requer**
- Instalação local
- Configuração de ambiente
- Credenciais especiais
- Downloads manuais

## 📚 **Recursos Adicionais**

### 🔗 **Links Úteis**
- [Projeto Baliza](https://github.com/franklinbaldo/baliza)
- [Portal PNCP](https://pncp.gov.br)
- [Internet Archive](https://archive.org)
- [Documentação DuckDB](https://duckdb.org)

### 🤝 **Como Contribuir**
1. Use o notebook e compartilhe feedback
2. Sugira novas análises via issues
3. Fork e adicione suas próprias análises
4. Cite o projeto em pesquisas acadêmicas

### 📄 **Licença e Citação**
```
@software{baliza2024,
  title={Baliza: Sistema de Coleta e Análise de Dados PNCP},
  author={Franklin Baldo},
  year={2024},
  url={https://github.com/franklinbaldo/baliza}
}
```

---

**🌟 Dê uma estrela no projeto se este notebook foi útil!**

**📧 Dúvidas? Abra uma [issue](https://github.com/franklinbaldo/baliza/issues) no GitHub.**