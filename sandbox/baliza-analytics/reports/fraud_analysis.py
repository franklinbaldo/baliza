import marimo

__generated_with = "0.8.0"
app = marimo.App(width="medium")


@app.cell
def __():
    import marimo as mo
    import sys
    import os
    
    # Add the package to path for imports
    sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
    
    from baliza_analytics.data_connector import BalizaDataConnector
    from baliza_analytics.fraud_detection import ContractFraudDetector
    import pandas as pd
    import plotly.express as px
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    return BalizaDataConnector, ContractFraudDetector, go, make_subplots, mo, os, pd, px, sys


@app.cell
def __(mo):
    mo.md("""
    # 🚨 Baliza Analytics - Relatório de Análise de Fraudes
    
    Este relatório apresenta uma análise abrangente de contratos públicos para identificação de padrões suspeitos e anomalias no Portal Nacional de Contratações Públicas (PNCP).
    
    ## Configurações
    """)
    return


@app.cell
def __(mo):
    # Configuration inputs
    date_range = mo.ui.date_range(
        start="2024-01-01",
        end="2024-12-31",
        label="📅 Período de Análise"
    )
    
    min_score_slider = mo.ui.slider(
        start=0,
        stop=100, 
        value=50,
        step=5,
        label="🎯 Score Mínimo de Suspeição"
    )
    
    uf_selector = mo.ui.multiselect(
        options=['AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 
                'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 
                'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'],
        value=['RO'],  # Default to Rondônia
        label="🗺️ Estados para Análise"
    )
    
    min_value_input = mo.ui.number(
        value=100000,
        label="💰 Valor Mínimo do Contrato (R$)"
    )
    
    mo.hstack([date_range, min_score_slider], justify="space-around")
    return date_range, min_score_slider, min_value_input, uf_selector


@app.cell
def __(mo, uf_selector, min_value_input):
    mo.hstack([uf_selector, min_value_input], justify="space-around")
    return


@app.cell
def __(mo):
    load_button = mo.ui.button(
        value="🔄 Carregar e Analisar Dados",
        kind="success"
    )
    load_button
    return load_button,


@app.cell
def __(BalizaDataConnector, ContractFraudDetector, date_range, load_button, min_value_input, mo, uf_selector):
    # Load and analyze data when button is clicked
    if load_button.value:
        try:
            connector = BalizaDataConnector()
            detector = ContractFraudDetector()
            
            # Load data with filters
            contracts_df = connector.get_contracts(
                start_date=str(date_range.value[0]) if date_range.value else None,
                end_date=str(date_range.value[1]) if date_range.value else None,
                uf_filter=list(uf_selector.value) if uf_selector.value else None,
                min_value=min_value_input.value
            )
            
            if contracts_df.empty:
                mo.md("⚠️ **Nenhum contrato encontrado com os filtros especificados.**")
            else:
                # Generate fraud analysis
                fraud_report = detector.generate_fraud_report(contracts_df)
                contracts_with_scores = detector.calculate_suspicious_score(contracts_df)
                
                mo.md(f"""
                ✅ **Dados carregados com sucesso!**
                
                - 📊 **{len(contracts_df):,} contratos** analisados
                - 🚨 **{fraud_report['summary']['suspicious_contracts']:,} contratos suspeitos** ({fraud_report['summary']['suspicion_rate']:.1f}%)
                - 🤖 **{fraud_report['summary']['ml_anomalies']:,} anomalias ML** ({fraud_report['summary']['anomaly_rate']:.1f}%)
                """)
                
        except Exception as e:
            mo.md(f"❌ **Erro ao carregar dados:** {str(e)}")
            contracts_df = None
            fraud_report = None
            contracts_with_scores = None
    else:
        contracts_df = None
        fraud_report = None 
        contracts_with_scores = None
    return contracts_df, contracts_with_scores, detector, fraud_report


@app.cell
def __(contracts_with_scores, fraud_report, go, make_subplots, min_score_slider, mo, px):
    # Display analysis results
    if fraud_report is not None and contracts_with_scores is not None:
        
        # Summary metrics
        summary = fraud_report['summary']
        
        mo.md(f"""
        ## 📈 Resumo da Análise
        """)
        
        # Create metrics cards
        metrics_cards = mo.hstack([
            mo.stat(
                value=f"{summary['total_contracts']:,}",
                label="Total de Contratos",
                caption="Contratos analisados no período"
            ),
            mo.stat(
                value=f"{summary['suspicious_contracts']:,}",
                label="Contratos Suspeitos", 
                caption=f"{summary['suspicion_rate']:.1f}% do total"
            ),
            mo.stat(
                value=f"{summary['ml_anomalies']:,}",
                label="Anomalias ML",
                caption=f"{summary['anomaly_rate']:.1f}% detectadas"
            ),
            mo.stat(
                value=f"{summary['high_risk_suppliers']:,}",
                label="Fornecedores Alto Risco",
                caption="Score de risco ≥ 50"
            )
        ], justify="space-around")
        
        metrics_cards
        
    else:
        mo.md("👆 **Clique em 'Carregar e Analisar Dados' para visualizar os resultados.**")
    return metrics_cards, summary


@app.cell
def __(contracts_with_scores, fraud_report, mo, px):
    # Risk distribution chart
    if fraud_report is not None:
        mo.md("## 📊 Distribuição de Risco")
        
        # Risk distribution data
        risk_dist = fraud_report['risk_distribution']
        risk_df = pd.DataFrame([
            {'Nível de Risco': 'Baixo (0-24)', 'Quantidade': risk_dist['low_risk'], 'Cor': '#2E8B57'},
            {'Nível de Risco': 'Médio (25-49)', 'Quantidade': risk_dist['medium_risk'], 'Cor': '#FFA500'}, 
            {'Nível de Risco': 'Alto (50-74)', 'Quantidade': risk_dist['high_risk'], 'Cor': '#FF6347'},
            {'Nível de Risco': 'Crítico (75-100)', 'Quantidade': risk_dist['critical_risk'], 'Cor': '#DC143C'}
        ])
        
        # Pie chart for risk distribution
        fig_risk = px.pie(
            risk_df, 
            values='Quantidade', 
            names='Nível de Risco',
            title='Distribuição de Contratos por Nível de Risco',
            color_discrete_sequence=['#2E8B57', '#FFA500', '#FF6347', '#DC143C']
        )
        fig_risk.update_traces(textposition='inside', textinfo='percent+label')
        fig_risk.update_layout(height=500)
        
        mo.ui.plotly(fig_risk)
        
    return fig_risk, risk_df, risk_dist


@app.cell
def __(contracts_with_scores, mo, px):
    # Score distribution histogram
    if contracts_with_scores is not None:
        mo.md("## 📈 Histograma de Scores de Suspeição")
        
        fig_hist = px.histogram(
            contracts_with_scores,
            x='score_suspeita',
            nbins=20,
            title='Distribuição de Scores de Suspeição',
            labels={'score_suspeita': 'Score de Suspeição', 'count': 'Número de Contratos'},
            color_discrete_sequence=['#1f77b4']
        )
        fig_hist.update_layout(height=400)
        fig_hist.add_vline(x=50, line_dash="dash", line_color="red", 
                          annotation_text="Limiar de Suspeição", annotation_position="top")
        
        mo.ui.plotly(fig_hist)
    return fig_hist,


@app.cell
def __(contracts_with_scores, mo, px):
    # Value vs Score scatter plot  
    if contracts_with_scores is not None:
        mo.md("## 💰 Relação entre Valor do Contrato e Score de Suspeição")
        
        # Filter for better visualization
        scatter_data = contracts_with_scores[contracts_with_scores['valor_global_brl'] > 0].copy()
        scatter_data['valor_milhoes'] = scatter_data['valor_global_brl'] / 1_000_000
        
        fig_scatter = px.scatter(
            scatter_data.sample(n=min(5000, len(scatter_data))),  # Sample for performance
            x='valor_milhoes',
            y='score_suspeita', 
            color='uf_sigla',
            title='Score de Suspeição vs Valor do Contrato',
            labels={
                'valor_milhoes': 'Valor do Contrato (R$ Milhões)',
                'score_suspeita': 'Score de Suspeição',
                'uf_sigla': 'Estado'
            },
            hover_data=['fornecedor_nome', 'orgao_razao_social']
        )
        fig_scatter.update_layout(height=500)
        fig_scatter.add_hline(y=50, line_dash="dash", line_color="red", 
                             annotation_text="Limiar de Suspeição")
        
        mo.ui.plotly(fig_scatter)
    return fig_scatter, scatter_data


@app.cell
def __(fraud_report, mo):
    # Top suspicious contracts table
    if fraud_report is not None and fraud_report['top_suspicious']:
        mo.md("## 🔥 Top 10 Contratos Mais Suspeitos")
        
        # Convert to DataFrame for better display
        suspicious_df = pd.DataFrame(fraud_report['top_suspicious'])
        suspicious_df['valor_milhoes'] = suspicious_df['valor_global_brl'] / 1_000_000
        
        # Format for display
        display_df = suspicious_df[[
            'score_suspeita', 'contrato_id', 'orgao_razao_social', 
            'fornecedor_nome', 'valor_milhoes'
        ]].copy()
        
        display_df.columns = [
            'Score', 'ID do Contrato', 'Órgão', 'Fornecedor', 'Valor (R$ M)'
        ]
        
        # Round values
        display_df['Valor (R$ M)'] = display_df['Valor (R$ M)'].round(2)
        
        mo.ui.table(
            display_df,
            selection=None,
            pagination=True,
            page_size=10
        )
    return display_df, suspicious_df


@app.cell
def __(contracts_with_scores, mo, px):
    # Timeline analysis
    if contracts_with_scores is not None:
        mo.md("## 📅 Análise Temporal de Contratos Suspeitos")
        
        # Prepare temporal data
        temporal_df = contracts_with_scores.copy()
        temporal_df['data_assinatura'] = pd.to_datetime(temporal_df['data_assinatura'])
        temporal_df['mes_ano'] = temporal_df['data_assinatura'].dt.to_period('M').astype(str)
        temporal_df['suspeito'] = temporal_df['score_suspeita'] >= 50
        
        # Monthly aggregation
        monthly_stats = temporal_df.groupby('mes_ano').agg({
            'contrato_id': 'count',
            'suspeito': 'sum',
            'valor_global_brl': 'sum'
        }).reset_index()
        
        monthly_stats['taxa_suspeicao'] = (monthly_stats['suspeito'] / monthly_stats['contrato_id'] * 100).round(2)
        monthly_stats['valor_milhoes'] = monthly_stats['valor_global_brl'] / 1_000_000
        
        # Create dual-axis chart
        fig_timeline = make_subplots(
            specs=[[{"secondary_y": True}]],
            subplot_titles=("Evolução Temporal dos Contratos")
        )
        
        # Add volume bars
        fig_timeline.add_trace(
            go.Bar(
                x=monthly_stats['mes_ano'],
                y=monthly_stats['contrato_id'],
                name='Total de Contratos',
                marker_color='lightblue',
                opacity=0.7
            ),
            secondary_y=False
        )
        
        # Add suspicious rate line
        fig_timeline.add_trace(
            go.Scatter(
                x=monthly_stats['mes_ano'],
                y=monthly_stats['taxa_suspeicao'],
                name='Taxa de Suspeição (%)',
                line=dict(color='red', width=3),
                mode='lines+markers'
            ),
            secondary_y=True
        )
        
        # Update layout
        fig_timeline.update_xaxes(title_text="Mês/Ano")
        fig_timeline.update_yaxes(title_text="Número de Contratos", secondary_y=False)
        fig_timeline.update_yaxes(title_text="Taxa de Suspeição (%)", secondary_y=True)
        fig_timeline.update_layout(height=500, title="Evolução Temporal dos Contratos")
        
        mo.ui.plotly(fig_timeline)
    return fig_timeline, monthly_stats, temporal_df


@app.cell
def __(mo):
    mo.md("""
    ## 📋 Metodologia
    
    ### Algoritmo de Detecção de Fraudes (Score 0-100):
    
    1. **Contratos de Alto Valor** (20 pontos): Contratos acima do percentil 99
    2. **Duração Incomum** (15 pontos): Contratos muito longos (>3x mediana) ou curtos (<30 dias)
    3. **Frequência do Fornecedor** (15 pontos): Mesmo fornecedor com >10 ou >20 contratos
    4. **Valores Redondos** (10 pontos): Valores terminados em múltiplos exatos de R$ 10.000
    5. **Assinaturas em Fins de Semana** (5 pontos): Contratos assinados nos finais de semana
    6. **Palavras-chave de Emergência** (10 pontos): Termos como "emergência", "urgente" no objeto
    7. **Padrões de Monopólio** (15 pontos): Pares órgão-fornecedor com >5 contratos
    
    ### Detecção de Anomalias por Machine Learning:
    
    - **Isolation Forest**: Detecta outliers em padrões de valor e duração
    - **Taxa de Contaminação**: Assume ~10% de anomalias no dataset
    - **Features**: Valor global, duração do contrato (normalizados)
    
    ---
    
    *Relatório gerado por **Baliza Analytics** - Sistema de análise de transparência em contratações públicas*
    """)
    return


if __name__ == "__main__":
    app.run()